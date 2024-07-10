package specs

import "fmt"

type ColumnSpec struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (c *ColumnSpec) GetValue(rowIndex int, columnValues []interface{}) (interface{}, error) {
	// test if rowIndex is within bounds
	if rowIndex < 0 || rowIndex >= len(columnValues) {
		return nil, fmt.Errorf("row index out of bounds: %d", rowIndex)
	}
	// test if value conforms to column type

	switch c.Type {
	case ColumnTypeString, ColumnTypeDate, ColumnTypeDateTime:
		value, ok := columnValues[rowIndex].(string)
		if !ok {
			return nil, fmt.Errorf("value is not a string")
		}
		return value, nil
	case ColumnTypeInteger:
		value, ok := columnValues[rowIndex].(int)
		if !ok {
			return nil, fmt.Errorf("value is not an integer")
		}
		return value, nil
	case ColumnTypeFloat:
		value, ok := columnValues[rowIndex].(float64)
		if !ok {
			return nil, fmt.Errorf("value is not a float")
		}
		return value, nil
	case ColumnTypeBoolean:
		value, ok := columnValues[rowIndex].(bool)
		if !ok {
			return nil, fmt.Errorf("value is not a boolean")
		}
		return value, nil
	default:
		return nil, fmt.Errorf("invalid column type: %s", c.Type)
	}
}

func (c *ColumnSpec) IsValidValue(value interface{}) bool {
	switch c.Type {
	case ColumnTypeString:
		_, ok := value.(string)
		return ok
	case ColumnTypeInteger:
		_, ok := value.(int)
		return ok
	case ColumnTypeFloat:
		_, ok := value.(float64)
		return ok
	case ColumnTypeDate:
		_, ok := value.(string)
		return ok
	case ColumnTypeDateTime:
		_, ok := value.(string)
		return ok
	case ColumnTypeBoolean:
		_, ok := value.(bool)
		return ok
	default:
		return false
	}
}

func (c *ColumnSpec) ConformsTo(tableData map[string][]interface{}) bool {
	// see if table data is a map
	arrayOfValues, ok := tableData[c.Name]
	if !ok {
		return false
	}
	// see if value conforms to the column type
	for _, v := range arrayOfValues {
		if !c.IsValidValue(v) {
			return false
		}
	}
	return ok
}
