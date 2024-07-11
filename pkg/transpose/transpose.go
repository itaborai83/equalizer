package transpose

import (
	"fmt"

	"github.com/itaborai83/equalizer/internal/utils"
)

var (
	log = utils.NewLogger("transpose")
)

func ConvertToColumnarFormat(data interface{}) (map[string][]interface{}, error) {
	log.Println("converting data to columnar format")
	originalDataType := fmt.Sprintf("%T", data)
	log.Println("original data type: ", originalDataType)

	// is the top element an array or map?
	_, itIsaList := data.([]interface{})
	_, itIsaMap := data.(map[string]interface{})

	if !itIsaList && !itIsaMap {
		return nil, fmt.Errorf("data is not a list or a map")
	}

	if itIsaMap {
		log.Println("top element is a map of strings to interfaces")
		log.Println("attempting to cast data to a map of strings to a list of interfaces")
		data1, ok := data.(map[string]interface{})
		if !ok {
			panic("data is not a map of strings to interfaces") // should not have changed
		}

		log.Println("attempting to cast data to a map of strings to arrays of interfaces")
		data2 := make(map[string][]interface{})
		for key, value := range data1 {
			valueType := fmt.Sprintf("%T", value)
			log.Printf("attempting to cast value for key %s with type %s to an array of interfaces\n", key, valueType)
			data2[key], ok = value.([]interface{})
			if !ok {
				return nil, fmt.Errorf("value for key %s is not an array of interfaces", key)
			}
		}
		log.Println("data converted to columnar format")
		return data2, nil

	} else {
		log.Println("top element is an array of interfaces")
		log.Println("attempting to cast data to a map of strings to a list of interfaces")
		data3, err := RowsToColumns(data)
		if err != nil {
			return nil, fmt.Errorf("data is not in column format: %s", err.Error())
		}
		castedToMapOfArrays, ok := data3.(map[string][]interface{})
		if !ok {
			return nil, fmt.Errorf("data is not a map of arrays: %s", originalDataType)
		}
		return castedToMapOfArrays, nil
	}
}

func IsInRowFormat(data interface{}) bool {
	_, ok := data.([]map[string]interface{})
	if ok {
		return true
	}
	_, ok = data.([]interface{})
	return ok
}

func IsInColumnFormat(data interface{}) bool {
	dataTypeName := fmt.Sprintf("%T", data)
	fmt.Println(dataTypeName)
	_, ok := data.(map[string][]interface{})
	if ok {
		return true
	}
	_, ok = data.(map[string]interface{})
	return ok
}

func RowsToColumns(data interface{}) (interface{}, error) {
	log.Println("converting data from row format to columnar format")
	arrayOfMaps, ok := data.([]interface{})
	if !ok {
		return nil, fmt.Errorf("data is not in row format")
	}

	log.Println("counting rows")
	rowCount := len(arrayOfMaps)

	log.Println("collecting column names")
	// loop trhough the rows to collect the column names
	columnNames := []string{}
	columnNamesSeen := map[string]bool{}
	for idx, _ := range arrayOfMaps {
		// try to cast the row to a map
		row, ok := arrayOfMaps[idx].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("row %d is not a map", idx)
		}
		// loop through the columns to collect the column names
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
	if len(columnNames) == 0 {
		return nil, fmt.Errorf("no column names found")
	}
	// create a map of arrays to hold the column data
	mapOfArrays := make(map[string][]interface{})
	for _, columnName := range columnNames {
		mapOfArrays[columnName] = make([]interface{}, rowCount)
	}

	// loop through the rows to collect the column data
	for i, row := range arrayOfMaps {
		row, ok := row.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("row %d is not a map", i)
		}
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
