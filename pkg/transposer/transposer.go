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

	result := spec.NewEmptyData()

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
