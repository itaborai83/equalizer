package specs

import (
	"testing"
)

func TestGetValue(t *testing.T) {
	tests := []struct {
		name          string
		columnSpec    ColumnSpec
		rowIndex      int
		columnValues  interface{}
		expectedValue interface{}
		expectedError bool
	}{
		{
			name:          "Valid String",
			columnSpec:    ColumnSpec{Name: "col1", Type: ColumnTypeString},
			rowIndex:      1,
			columnValues:  []interface{}{"test1", "test2"},
			expectedValue: "test2",
			expectedError: false,
		},
		{
			name:          "Invalid Row Index",
			columnSpec:    ColumnSpec{Name: "col1", Type: ColumnTypeString},
			rowIndex:      2,
			columnValues:  []interface{}{"test1", "test2"},
			expectedValue: nil,
			expectedError: true,
		},
		{
			name:          "Invalid Column Values",
			columnSpec:    ColumnSpec{Name: "col1", Type: ColumnTypeString},
			rowIndex:      1,
			columnValues:  "not an array",
			expectedValue: nil,
			expectedError: true,
		},
		{
			name:          "Invalid Type",
			columnSpec:    ColumnSpec{Name: "col1", Type: ColumnTypeInteger},
			rowIndex:      1,
			columnValues:  []interface{}{1, 2},
			expectedValue: 2,
			expectedError: false,
		},
		{
			name:          "Valid Float",
			columnSpec:    ColumnSpec{Name: "col1", Type: ColumnTypeFloat},
			rowIndex:      1,
			columnValues:  []interface{}{1.1, 2.2},
			expectedValue: 2.2,
			expectedError: false,
		},
		{
			name:          "Invalid Float",
			columnSpec:    ColumnSpec{Name: "col1", Type: ColumnTypeFloat},
			rowIndex:      1,
			columnValues:  []interface{}{1.1, "not a float"},
			expectedValue: nil,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := tt.columnSpec.GetValue(tt.rowIndex, tt.columnValues)
			if (err != nil) != tt.expectedError {
				t.Errorf("GetValue() error = %v, expectedError %v", err, tt.expectedError)
				return
			}
			if value != tt.expectedValue {
				t.Errorf("GetValue() value = %v, expectedValue %v", value, tt.expectedValue)
			}
		})
	}
}

func TestIsValidValue(t *testing.T) {
	tests := []struct {
		name         string
		columnSpec   ColumnSpec
		value        interface{}
		expectedBool bool
	}{
		{
			name:         "Valid String",
			columnSpec:   ColumnSpec{Name: "col1", Type: ColumnTypeString},
			value:        "test",
			expectedBool: true,
		},
		{
			name:         "Invalid String",
			columnSpec:   ColumnSpec{Name: "col1", Type: ColumnTypeString},
			value:        123,
			expectedBool: false,
		},
		{
			name:         "Valid Integer",
			columnSpec:   ColumnSpec{Name: "col1", Type: ColumnTypeInteger},
			value:        123,
			expectedBool: true,
		},
		{
			name:         "Invalid Integer",
			columnSpec:   ColumnSpec{Name: "col1", Type: ColumnTypeInteger},
			value:        "123",
			expectedBool: false,
		},
		{
			name:         "Valid Float",
			columnSpec:   ColumnSpec{Name: "col1", Type: ColumnTypeFloat},
			value:        123.45,
			expectedBool: true,
		},
		{
			name:         "Invalid Float",
			columnSpec:   ColumnSpec{Name: "col1", Type: ColumnTypeFloat},
			value:        "123.45",
			expectedBool: false,
		},
		{
			name:         "Valid Boolean",
			columnSpec:   ColumnSpec{Name: "col1", Type: ColumnTypeBoolean},
			value:        true,
			expectedBool: true,
		},
		{
			name:         "Invalid Boolean",
			columnSpec:   ColumnSpec{Name: "col1", Type: ColumnTypeBoolean},
			value:        "true",
			expectedBool: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.columnSpec.IsValidValue(tt.value); got != tt.expectedBool {
				t.Errorf("IsValidValue() = %v, expectedBool %v", got, tt.expectedBool)
			}
		})
	}
}

func TestColumnsConformsTo(t *testing.T) {
	tests := []struct {
		name         string
		columnSpec   ColumnSpec
		tableData    interface{}
		expectedBool bool
	}{
		{
			name:         "Valid Data",
			columnSpec:   ColumnSpec{Name: "col1", Type: ColumnTypeString},
			tableData:    map[string]interface{}{"col1": []interface{}{"test1", "test2"}},
			expectedBool: true,
		},
		{
			name:         "Invalid Data Type",
			columnSpec:   ColumnSpec{Name: "col1", Type: ColumnTypeString},
			tableData:    map[string]interface{}{"col1": "not an array"},
			expectedBool: false,
		},
		{
			name:         "Invalid Column Name",
			columnSpec:   ColumnSpec{Name: "col1", Type: ColumnTypeString},
			tableData:    map[string]interface{}{"col2": []interface{}{"test1", "test2"}},
			expectedBool: false,
		},
		{
			name:         "Invalid Value Type",
			columnSpec:   ColumnSpec{Name: "col1", Type: ColumnTypeInteger},
			tableData:    map[string]interface{}{"col1": []interface{}{"test1", "test2"}},
			expectedBool: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.columnSpec.ConformsTo(tt.tableData); got != tt.expectedBool {
				t.Errorf("ConformsTo() = %v, expectedBool %v", got, tt.expectedBool)
			}
		})
	}
}
