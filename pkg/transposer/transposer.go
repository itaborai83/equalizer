package transposer

import (
	"fmt"

	"github.com/itaborai83/equalizer/pkg/specs"
)

func IsInRowFormat(data interface{}) bool {
	switch data.(type) {
	case []interface{}:
		return true
	default:
		return false
	}
}

func IsInColumnFormat(data interface{}) bool {
	switch data.(type) {
	case map[string]interface{}:
		return true
	default:
		return false
	}
}

func RowsToColumns(spec *specs.TableSpec, data interface{}) (map[string][]interface{}, error) {
	arrayOfMaps, ok := data.([]interface{})
	if !ok {
		return nil, fmt.Errorf("data is not in row format")
	}

	result := spec.NewColumnarTable()

	// get the spec column names for fast lookup
	specColumns := make(map[string]specs.ColumnSpec)
	for _, col := range spec.Columns {
		specColumns[col.Name] = col
	}

	// loop through the rows to collect the column data
	for i, row := range arrayOfMaps {
		row, ok := row.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("row %d is not a map", i)
		}
		// for each column in the spec, add the value to the column if it exists
		for fieldName, fieldValue := range row {
			// does the column exist in the spec?
			column, ok := specColumns[fieldName]
			if !ok {
				// ignore the column then
				continue
			}

			if fieldValue != nil {
				// does the value conform to the column type?
				if !column.IsValidValue(fieldValue) {
					msg := "value '%v' on the row index %d does not conform to column '%s' type of '%s'"
					return nil, fmt.Errorf(msg, fieldValue, i, fieldName, column.Type)
				}
			}

			// add the value to the column
			result[fieldName] = append(result[fieldName], fieldValue)
		}
	}
	return result, nil
}

func castJsonMapToColumnarTable(data interface{}) (map[string][]interface{}, error) {
	mapOfArraysOfInterfaces, ok := data.(map[string][]interface{})
	if ok {
		return mapOfArraysOfInterfaces, nil
	}

	mapOfInterfaces, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("data is not in column format")
	}
	columnNames := make([]string, 0)
	for key := range mapOfInterfaces {
		columnNames = append(columnNames, key)
	}
	result := make(map[string][]interface{})
	for _, columnName := range columnNames {
		value := mapOfInterfaces[columnName]
		array, ok := value.([]interface{})
		if !ok {
			return nil, fmt.Errorf("data is not in column format: column '%s' is not an array", columnName)
		}
		result[columnName] = array
	}
	return result, nil
}

func ColumnsToRows(spec *specs.TableSpec, data interface{}) ([]map[string]interface{}, error) {
	mapOfArrays, err := castJsonMapToColumnarTable(data)
	if err != nil {
		return nil, err
	}

	result := make([]map[string]interface{}, 0)

	// get the column for fast lookup
	columns := make(map[string]*specs.ColumnSpec, 0)
	for _, column := range spec.Columns {
		columns[column.Name] = &column
	}

	// assert that all columns have the same number of rows
	rowCount := len(mapOfArrays[spec.Columns[0].Name])
	for _, columnSpec := range spec.Columns {

		// does the map of arrays hold this column?
		_, ok := columns[columnSpec.Name]
		if !ok {
			return nil, fmt.Errorf("column '%s' not found", columnSpec.Name)
		}

		// do the columns have the same number of rows?
		if len(mapOfArrays[columnSpec.Name]) != rowCount {
			return nil, fmt.Errorf("column '%s' has a different number of rows", columnSpec.Name)
		}
	}

	// loop through the columns to collect the row data
	for i := 0; i < rowCount; i++ {

		newRow := spec.NewRow()
		// process each of the spec columns
		for columnIndex, column := range spec.Columns {
			// do we have a column
			value := mapOfArrays[column.Name][i]
			if value != nil {
				// if so, does the value conform to the column type?
				if !column.IsValidValue(value) {
					msg := "value '%v' on the row index %d does not conform to column '%s' type of '%s'"
					return nil, fmt.Errorf(msg, value, columnIndex, column.Name, column.Type)
				}
			}
			newRow[column.Name] = value
		}
		result = append(result, newRow)
	}
	return result, nil
}
