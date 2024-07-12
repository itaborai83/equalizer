package specs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type TableSpec struct {
	Name                string       `json:"name"`
	Columns             []ColumnSpec `json:"columns"`
	KeyColumns          []string     `json:"key_columns"`
	ChangeControlColumn string       `json:"change_control_column"`
}

func (t *TableSpec) Equalizable(other *TableSpec) (bool, error) {
	// does the key column count match
	if len(t.KeyColumns) != len(other.KeyColumns) {
		return false, fmt.Errorf("key column count does not match: %d != %d", len(t.KeyColumns), len(other.KeyColumns))
	}
	// does the key column types match
	for i := 0; i < len(t.KeyColumns); i++ {
		myColumnName := t.KeyColumns[i]
		myColumn := t.GetColumn(myColumnName)
		myType := myColumn.Type

		otherColumnName := other.KeyColumns[i]
		otherColumn := other.GetColumn(otherColumnName)
		otherType := otherColumn.Type

		if myType != otherType {
			return false, fmt.Errorf("key column type does not match on the %dth key column: %s != %s", i, myType, otherType)
		}
	}
	// does the change control column match
	bothHaveChangeControlColumn := t.ChangeControlColumn != "" && other.ChangeControlColumn != ""
	neitherHaveChangeControlColumn := t.ChangeControlColumn == "" && other.ChangeControlColumn == ""
	if !bothHaveChangeControlColumn && !neitherHaveChangeControlColumn {
		return false, fmt.Errorf("change control column does not match: %s != %s", t.ChangeControlColumn, other.ChangeControlColumn)
	}

	if neitherHaveChangeControlColumn {
		return true, nil
	}

	// does the change control column type match
	myChangeControlColumn := t.GetChangeControlColumn()
	if myChangeControlColumn == nil {
		return false, fmt.Errorf("source change control column not found: %s", t.ChangeControlColumn)
	}
	otherChangeControlColumn := other.GetChangeControlColumn()
	if otherChangeControlColumn == nil {
		return false, fmt.Errorf("target change control column not found: %s", other.ChangeControlColumn)
	}
	myChangeControlColumnType := myChangeControlColumn.Type
	otherChangeControlColumnType := otherChangeControlColumn.Type
	equalizable := myChangeControlColumnType == otherChangeControlColumnType
	if !equalizable {
		return false, fmt.Errorf("change control column type does not match: %s != %s", myChangeControlColumnType, otherChangeControlColumnType)
	}
	return true, nil
}

func (t *TableSpec) GetColumnValue(columnName string, rowIndex int, tableData map[string][]interface{}) (interface{}, error) {
	// get the column
	column := t.GetColumn(columnName)
	if column == nil {
		return nil, fmt.Errorf("column not found: %s", columnName)
	}

	// get the column values
	columnValues, ok := tableData[columnName]
	if !ok {
		return nil, fmt.Errorf("column values not found for: %s", columnName)
	}

	// get the value
	return column.GetValue(rowIndex, columnValues)
}

func (t *TableSpec) GetColumn(name string) *ColumnSpec {
	for _, col := range t.Columns {
		if col.Name == name {
			return &col
		}
	}
	return nil
}

func (t *TableSpec) GetChangeControlColumn() *ColumnSpec {
	return t.GetColumn(t.ChangeControlColumn)
}

func (t *TableSpec) ConformsToColumnar(data map[string][]interface{}) bool {
	// see if all the columns are present
	for _, col := range t.Columns {
		if _, ok := data[col.Name]; !ok {
			return false
		}
	}
	// see if all the columns conform to the column types
	for _, col := range t.Columns {
		if !col.ConformsTo(data) {
			return false
		}
	}
	return true
}

func (t *TableSpec) ConformsToRows(data []map[string]interface{}) bool {
	// index columns for fast lookup
	columns := make(map[string]ColumnSpec)
	for _, col := range t.Columns {
		columns[col.Name] = col
	}

	// for each row
	for _, row := range data {
		// see if all the columns conform to the column types
		for fieldName, fieldValue := range row {
			col, ok := columns[fieldName]
			if !ok {
				// ignore the column if it is not in the spec
				continue
			}
			if !col.IsValidValue(fieldValue) {
				return false
			}
		}
	}
	return true
}

func (t *TableSpec) NewColumnarTable() map[string][]interface{} {
	data := make(map[string][]interface{})
	for _, col := range t.Columns {
		data[col.Name] = make([]interface{}, 0)
	}
	return data
}

func (t *TableSpec) NewRow() map[string]interface{} {
	data := make(map[string]interface{})
	for _, col := range t.Columns {
		data[col.Name] = nil
	}
	return data
}

func SameKeys(sourceSpec, targetSpec *TableSpec, sourceData, targetData map[string][]interface{}, sourceIndex, targetIndex int) bool {
	// only works if the source and target spec are equalizable and source and target data conform to the specs
	keyColumnCount := len(sourceSpec.KeyColumns)
	for i := 0; i < keyColumnCount; i++ {
		sourceKeyColumnName := sourceSpec.KeyColumns[i]
		targetKeyColumnName := targetSpec.KeyColumns[i]
		sourceColumn := sourceSpec.GetColumn(sourceKeyColumnName)
		targetColumn := targetSpec.GetColumn(targetKeyColumnName)
		if sourceColumn.Type != targetColumn.Type {
			return false
		}

		sourceKeyColumnValue, err := sourceSpec.GetColumnValue(sourceKeyColumnName, sourceIndex, sourceData)
		if err != nil {
			return false
		}
		targetKeyColumnValue, err := targetSpec.GetColumnValue(targetKeyColumnName, targetIndex, targetData)
		if err != nil {
			return false
		}

		// same type?
		if fmt.Sprintf("%T", sourceKeyColumnValue) != fmt.Sprintf("%T", targetKeyColumnValue) {
			return false
		}
		// same value?
		if sourceKeyColumnValue != targetKeyColumnValue {
			return false
		}
	}
	return true
}

func NewerThan(sourceSpec, targetSpec *TableSpec, sourceData, targetData map[string][]interface{}, sourceIndex, targetIndex int) bool {
	// only works if the source and target spec are equalizable and source and target data conform to the specs
	sourceChangeControleColumnName := sourceSpec.ChangeControlColumn
	targetChangeControleColumnName := targetSpec.ChangeControlColumn

	// if neither have a change control column, then the source is always newer
	if sourceChangeControleColumnName == "" && targetChangeControleColumnName == "" {
		return true
	}

	// if one has a change control column and the other does not, then the source is always newer
	if sourceChangeControleColumnName == "" || targetChangeControleColumnName == "" {
		panic("one has a change control column and the other does not. This should not happen since specs were supposed to be equalizable")
	}

	sourceChangeControlColumnValue, err := sourceSpec.GetColumnValue(sourceChangeControleColumnName, sourceIndex, sourceData)
	if err != nil {
		panic(err)
	}
	targetChangeControlColumnValue, err := targetSpec.GetColumnValue(targetChangeControleColumnName, targetIndex, targetData)
	if err != nil {
		panic(err)
	}

	sourceTypeName := fmt.Sprintf("%T", sourceChangeControlColumnValue)
	targetTypeName := fmt.Sprintf("%T", targetChangeControlColumnValue)

	// same type?
	if sourceTypeName != targetTypeName {
		panic("source type: " + sourceTypeName + " != target type: " + targetTypeName)
	}

	// string?
	if sourceTypeName == "string" {
		return sourceChangeControlColumnValue.(string) > targetChangeControlColumnValue.(string)
	}
	// int?
	if sourceTypeName == "int" {
		return sourceChangeControlColumnValue.(int) > targetChangeControlColumnValue.(int)
	}
	// float64?
	if sourceTypeName == "float64" {
		return sourceChangeControlColumnValue.(float64) > targetChangeControlColumnValue.(float64)
	}
	// bool?
	if sourceTypeName == "bool" {
		panic("bool not supported")
	}
	// unsupport type
	panic("unknown type: " + sourceTypeName)
}

func ReadSpecFile(filePath string) (*TableSpec, error) {
	spec := &TableSpec{}
	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bytes, spec)
	if err != nil {
		return nil, err
	}
	return spec, nil
}
