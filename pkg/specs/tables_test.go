package specs

import (
	"fmt"
	"strings"
	"testing"
)

func TestGetColumnValue(t *testing.T) {
	table := TableSpec{
		Name: "test_table",
		Columns: []ColumnSpec{
			{Name: "col1", Type: ColumnTypeString},
			{Name: "col2", Type: ColumnTypeInteger},
		},
	}

	tableData := map[string]interface{}{
		"col1": []interface{}{"value1", "value2"},
		"col2": []interface{}{1, 2},
	}

	tests := []struct {
		name          string
		columnName    string
		rowIndex      int
		expectedValue interface{}
		expectedError bool
	}{
		{
			name:          "Valid Column Value",
			columnName:    "col1",
			rowIndex:      1,
			expectedValue: "value2",
			expectedError: false,
		},
		{
			name:          "Invalid Column Name",
			columnName:    "col3",
			rowIndex:      0,
			expectedValue: nil,
			expectedError: true,
		},
		{
			name:          "Invalid Row Index",
			columnName:    "col2",
			rowIndex:      2,
			expectedValue: nil,
			expectedError: true,
		},
	}

	for idx, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := table.GetColumnValue(tt.columnName, tt.rowIndex, tableData)
			if (err != nil) != tt.expectedError {
				t.Errorf("Index %d, GetColumnValue() error = %v, expectedError %v", idx, err, tt.expectedError)
				return
			}
			if value != tt.expectedValue {
				t.Errorf("Index %d, GetColumnValue() value = %v, expectedValue %v", idx, value, tt.expectedValue)
			}
		})
	}
}

func TestGetColumn(t *testing.T) {
	table := TableSpec{
		Name: "test_table",
		Columns: []ColumnSpec{
			{Name: "col1", Type: ColumnTypeString},
			{Name: "col2", Type: ColumnTypeInteger},
		},
	}

	tests := []struct {
		name          string
		columnName    string
		expectedValue *ColumnSpec
	}{
		{
			name:          "Valid Column",
			columnName:    "col1",
			expectedValue: &ColumnSpec{Name: "col1", Type: ColumnTypeString},
		},
		{
			name:          "Invalid Column",
			columnName:    "col3",
			expectedValue: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := table.GetColumn(tt.columnName)
			if (col == nil && tt.expectedValue != nil) || (col != nil && *col != *tt.expectedValue) {
				t.Errorf("GetColumn() got = %v, expectedValue %v", col, tt.expectedValue)
			}
		})
	}
}

func TestTableConformsTo(t *testing.T) {
	table := TableSpec{
		Name: "test_table",
		Columns: []ColumnSpec{
			{Name: "col1", Type: ColumnTypeString},
			{Name: "col2", Type: ColumnTypeInteger},
		},
	}

	tests := []struct {
		name         string
		tableData    interface{}
		expectedBool bool
	}{
		{
			name: "Valid Data",
			tableData: map[string]interface{}{
				"col1": []interface{}{"value1", "value2"},
				"col2": []interface{}{1, 2},
			},
			expectedBool: true,
		},
		{
			name: "Missing Column",
			tableData: map[string]interface{}{
				"col1": []interface{}{"value1", "value2"},
			},
			expectedBool: false,
		},
		{
			name: "Invalid Column Type",
			tableData: map[string]interface{}{
				"col1": []interface{}{"value1", "value2"},
				"col2": []interface{}{"1", "2"},
			},
			expectedBool: false,
		},
		{
			name:         "Invalid Data Type",
			tableData:    "not a map",
			expectedBool: false,
		},
	}

	for idx, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := table.ConformsTo(tt.tableData); got != tt.expectedBool {
				t.Errorf("Index: %d, ConformsTo() = %v, expectedBool %v", idx, got, tt.expectedBool)
			}
		})
	}
}

func TestEqualizable(t *testing.T) {
	// Define some sample columns for testing
	col1 := ColumnSpec{Name: "col1", Type: ColumnTypeString}
	col2 := ColumnSpec{Name: "col2", Type: ColumnTypeInteger}
	col3 := ColumnSpec{Name: "col3", Type: ColumnTypeString}

	table1 := TableSpec{
		Name:                "table1",
		Columns:             []ColumnSpec{col1, col2},
		KeyColumns:          []string{"col1"},
		ChangeControlColumn: "col2",
	}

	table2 := TableSpec{
		Name:                "table2",
		Columns:             []ColumnSpec{col1, col2},
		KeyColumns:          []string{"col1"},
		ChangeControlColumn: "col2",
	}

	table3 := TableSpec{
		Name:                "table3",
		Columns:             []ColumnSpec{col1, col3},
		KeyColumns:          []string{"col1", "col3"},
		ChangeControlColumn: "col3",
	}

	table4 := TableSpec{
		Name:                "table5",
		Columns:             []ColumnSpec{col1, col2, col3},
		KeyColumns:          []string{"col2"},
		ChangeControlColumn: "",
	}

	tests := []struct {
		name           string
		table1         TableSpec
		table2         TableSpec
		expectedResult bool
		expectedError  error
	}{
		{
			name:           "Equalizable tables",
			table1:         table1,
			table2:         table2,
			expectedResult: true,
			expectedError:  nil,
		},
		{
			name:           "Different key column count",
			table1:         table1,
			table2:         table3,
			expectedResult: false,
			expectedError:  fmt.Errorf("key column count does not match: 1 != 2"),
		},
		{
			name:           "Different key column type",
			table1:         table1,
			table2:         table4,
			expectedResult: false,
			expectedError:  fmt.Errorf("key column type does not match on the 0th key column: STRING != INTEGER"),
		},
	}

	for idx, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.table1.Equalizable(&tt.table2)
			if result != tt.expectedResult {
				t.Errorf("Index %d, Equalizable() result = '%v', expected '%v'", idx, result, tt.expectedResult)
			}
			if err != nil {
				errMsg := err.Error()
				expectedErrMsg := tt.expectedError.Error()
				errorMatched := strings.Contains(errMsg, expectedErrMsg)
				if !errorMatched {
					t.Errorf("Index %d, Equalizable() error = '%v', expected error '%v'", idx, err, tt.expectedError)
				}
			} else {
				if tt.expectedError != nil {
					t.Errorf("Index %d, Equalizable() error = nil, expected error '%v'", idx, tt.expectedError)
				}
			}
		})
	}
}
