package transpose

import (
	"fmt"
	"reflect"
	"testing"
)

func TestTransposeRowsToColumns(t *testing.T) {
	tests := []struct {
		name           string
		input          interface{}
		expectedOutput interface{}
		expectedError  error
	}{
		{
			name: "Valid input",
			input: []map[string]interface{}{
				{"name": "John", "age": 30},
				{"name": "Jane", "age": 25},
			},
			expectedOutput: map[string][]interface{}{
				"name": {"John", "Jane"},
				"age":  {30, 25},
			},
			expectedError: nil,
		},
		{
			name:           "Invalid input type",
			input:          "not a slice of maps",
			expectedOutput: nil,
			expectedError:  fmt.Errorf("data is not in row format"),
		},
		{
			name: "Empty input",
			input: []map[string]interface{}{
				{},
			},
			expectedOutput: map[string][]interface{}{},
			expectedError:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := RowsToColumns(tt.input)
			if !reflect.DeepEqual(output, tt.expectedOutput) {
				t.Errorf("TransposeRowsToColumns() output = %v, expected %v", output, tt.expectedOutput)
			}
			if (err != nil && tt.expectedError == nil) || (err == nil && tt.expectedError != nil) || (err != nil && tt.expectedError != nil && err.Error() != tt.expectedError.Error()) {
				t.Errorf("TransposeRowsToColumns() error = %v, expected error %v", err, tt.expectedError)
			}
		})
	}
}

func TestTransposeColumnsToRows(t *testing.T) {
	tests := []struct {
		name           string
		input          interface{}
		expectedOutput interface{}
		expectedError  error
	}{
		{
			name: "Valid input",
			input: map[string][]interface{}{
				"name": {"John", "Jane"},
				"age":  {30, 25},
			},
			expectedOutput: []map[string]interface{}{
				{"name": "John", "age": 30},
				{"name": "Jane", "age": 25},
			},
			expectedError: nil,
		},
		{
			name:           "Invalid input type",
			input:          "not a map of arrays",
			expectedOutput: nil,
			expectedError:  fmt.Errorf("data is not in column format"),
		},
		{
			name: "Columns with different row counts",
			input: map[string][]interface{}{
				"name": {"John", "Jane"},
				"age":  {30},
			},
			expectedOutput: nil,
			expectedError:  fmt.Errorf("column age has a different number of rows"),
		},
		{
			name: "Empty input",
			input: map[string][]interface{}{
				"name": {},
				"age":  {},
			},
			expectedOutput: []map[string]interface{}{},
			expectedError:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := ColumnsToRows(tt.input)
			if !reflect.DeepEqual(output, tt.expectedOutput) {
				t.Errorf("TransposeColumnsToRows() output = %v, expected %v", output, tt.expectedOutput)
			}
			if (err != nil && tt.expectedError == nil) || (err == nil && tt.expectedError != nil) || (err != nil && tt.expectedError != nil && err.Error() != tt.expectedError.Error()) {
				t.Errorf("TransposeColumnsToRows() error = %v, expected error %v", err, tt.expectedError)
			}
		})
	}
}
