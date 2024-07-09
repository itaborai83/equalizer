package transpose

import "fmt"

func IsInRowFormat(data interface{}) bool {
	_, ok := data.([]map[string]interface{})
	return ok
}

func IsInColumnFormat(data interface{}) bool {
	_, ok := data.(map[string][]interface{})
	return ok
}

func RowsToColumns(data interface{}) (interface{}, error) {
	arrayOfMaps, ok := data.([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("data is not in row format")
	}

	rowCount := len(arrayOfMaps)

	// loop trhough the rows to collect the column names
	columnNames := []string{}
	columnNamesSeen := map[string]bool{}
	for _, row := range arrayOfMaps {
		for key := range row {
			// have we seen this column name before
			_, ok := columnNamesSeen[key]
			if ok {
				continue
			}
			columnNamesSeen[key] = true
			columnNames = append(columnNames, key)
		}
	}

	// create a map of arrays to hold the column data
	mapOfArrays := make(map[string][]interface{})
	for _, columnName := range columnNames {
		mapOfArrays[columnName] = make([]interface{}, rowCount)
	}

	// loop through the rows to collect the column data
	for i, row := range arrayOfMaps {
		for _, columnName := range columnNames {
			mapOfArrays[columnName][i] = row[columnName]
		}
	}
	return mapOfArrays, nil
}

func ColumnsToRows(data interface{}) (interface{}, error) {
	// data is in column format if the data is a map of arrays
	mapOfArrays, ok := data.(map[string][]interface{})
	if !ok {
		return nil, fmt.Errorf("data is not in column format")
	}

	// collect the column names
	columnNames := []string{}
	for key := range mapOfArrays {
		columnNames = append(columnNames, key)
	}

	// allocate an array of maps to hold the row data
	rowCount := len(mapOfArrays[columnNames[0]])
	arrayOfMaps := make([]map[string]interface{}, rowCount)

	// assert that all columns have the same number of rows
	for _, columnName := range columnNames {
		if len(mapOfArrays[columnName]) != rowCount {
			return nil, fmt.Errorf("column %s has a different number of rows", columnName)
		}
	}

	// loop through the columns to collect the row data
	for i := 0; i < rowCount; i++ {
		row := make(map[string]interface{})
		for _, columnName := range columnNames {
			row[columnName] = mapOfArrays[columnName][i]
		}
		arrayOfMaps[i] = row
	}
	return arrayOfMaps, nil
}
