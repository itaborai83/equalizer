package specs

import "fmt"

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
	myChangeControlColumn := t.GetColumn(t.ChangeControlColumn)
	myChangeControlColumnType := myChangeControlColumn.Type
	otherChangeControlColumn := other.GetColumn(other.ChangeControlColumn)
	otherChangeControlColumnType := otherChangeControlColumn.Type
	equalizable := myChangeControlColumnType == otherChangeControlColumnType
	if !equalizable {
		return false, fmt.Errorf("change control column type does not match: %s != %s", myChangeControlColumnType, otherChangeControlColumnType)
	}
	return true, nil
}

func (t *TableSpec) GetColumnValue(columnName string, rowIndex int, tableData map[string]interface{}) (interface{}, error) {
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

func (t *TableSpec) ConformsTo(data interface{}) bool {
	// see if table data is a map
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return false
	}
	// see if all the columns are present
	for _, col := range t.Columns {
		if _, ok := dataMap[col.Name]; !ok {
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
