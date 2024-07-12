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

	tableData := map[string][]interface{}{
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
		tableData    map[string][]interface{}
		expectedBool bool
	}{
		{
			name: "Valid Data",
			tableData: map[string][]interface{}{
				"col1": []interface{}{"value1", "value2"},
				"col2": []interface{}{1, 2},
			},
			expectedBool: true,
		},
		{
			name: "Missing Column",
			tableData: map[string][]interface{}{
				"col1": []interface{}{"value1", "value2"},
			},
			expectedBool: false,
		},
		{
			name: "Invalid Column Type",
			tableData: map[string][]interface{}{
				"col1": []interface{}{"value1", "value2"},
				"col2": []interface{}{"1", "2"},
			},
			expectedBool: false,
		},
	}

	for idx, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := table.ConformsToColumnar(tt.tableData); got != tt.expectedBool {
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

func TestSameKey(t *testing.T) {
	// Define some sample columns for testing
	col1 := ColumnSpec{Name: "col1", Type: ColumnTypeInteger}
	col2 := ColumnSpec{Name: "col2", Type: ColumnTypeString}
	col3 := ColumnSpec{Name: "col3", Type: ColumnTypeInteger}

	table1 := TableSpec{
		Name:                "table1",
		Columns:             []ColumnSpec{col1, col2, col3},
		KeyColumns:          []string{"col1"},
		ChangeControlColumn: "col2",
	}

	table2 := TableSpec{
		Name:                "table2",
		Columns:             []ColumnSpec{col1, col2, col3},
		KeyColumns:          []string{"col2"},
		ChangeControlColumn: "col1",
	}

	table3 := TableSpec{
		Name:                "table3",
		Columns:             []ColumnSpec{col1, col2, col3},
		KeyColumns:          []string{"col3"},
		ChangeControlColumn: "col2",
	}

	tests := []struct {
		name        string
		sourceSpec  *TableSpec
		targetSpec  *TableSpec
		sourceData  map[string][]interface{}
		targetData  map[string][]interface{}
		sourceIndex int
		targetIndex int
		expected    bool
	}{
		{
			name:        "Same key types and values",
			sourceSpec:  &table1,
			targetSpec:  &table3,
			sourceData:  map[string][]interface{}{"col1": {1}},
			targetData:  map[string][]interface{}{"col3": {1}},
			sourceIndex: 0,
			targetIndex: 0,
			expected:    true,
		},
		{
			name:        "Different key types, same values",
			sourceSpec:  &table1,
			targetSpec:  &table2,
			sourceData:  map[string][]interface{}{"col1": {1}},
			targetData:  map[string][]interface{}{"col2": {1}},
			sourceIndex: 0,
			targetIndex: 0,
			expected:    false,
		},
		{
			name:        "Same key types, different values",
			sourceSpec:  &table1,
			targetSpec:  &table1,
			sourceData:  map[string][]interface{}{"col1": {1}},
			targetData:  map[string][]interface{}{"col1": {2}},
			sourceIndex: 0,
			targetIndex: 0,
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SameKeys(tt.sourceSpec, tt.targetSpec, tt.sourceData, tt.targetData, tt.sourceIndex, tt.targetIndex)
			if result != tt.expected {
				t.Errorf("SameKey() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNewerThan(t *testing.T) {
	stringSpec := &TableSpec{
		Columns: []ColumnSpec{
			{Name: "id", Type: ColumnTypeInteger},
			{Name: "updated_at", Type: ColumnTypeString},
		},
		KeyColumns:          []string{"id"},
		ChangeControlColumn: "updated_at",
	}

	intSpec := &TableSpec{
		Columns: []ColumnSpec{
			{Name: "id", Type: ColumnTypeInteger},
			{Name: "version", Type: ColumnTypeInteger},
		},
		KeyColumns:          []string{"id"},
		ChangeControlColumn: "version",
	}

	floatSpec := &TableSpec{
		Columns: []ColumnSpec{
			{Name: "id", Type: ColumnTypeInteger},
			{Name: "score", Type: ColumnTypeFloat},
		},
		KeyColumns:          []string{"id"},
		ChangeControlColumn: "score",
	}

	mistypedSpec := &TableSpec{
		Columns: []ColumnSpec{
			{Name: "id", Type: ColumnTypeInteger},
			{Name: "updated_at", Type: ColumnTypeInteger}, // wrong type
		},
		KeyColumns:          []string{"id"},
		ChangeControlColumn: "updated_at",
	}

	boolSpec := &TableSpec{
		Columns: []ColumnSpec{
			{Name: "id", Type: ColumnTypeInteger},
			{Name: "flag", Type: ColumnTypeBoolean},
		},
		KeyColumns:          []string{"id"},
		ChangeControlColumn: "flag",
	}

	tests := []struct {
		name        string
		sourceSpec  *TableSpec
		targetSpec  *TableSpec
		sourceData  map[string][]interface{}
		targetData  map[string][]interface{}
		sourceIndex int
		targetIndex int
		expected    bool
		shouldPanic bool
	}{
		{
			name:        "String type - source newer",
			sourceSpec:  stringSpec,
			targetSpec:  stringSpec,
			sourceData:  map[string][]interface{}{"updated_at": {"2023-07-10"}},
			targetData:  map[string][]interface{}{"updated_at": {"2023-07-09"}},
			sourceIndex: 0,
			targetIndex: 0,
			expected:    true,
			shouldPanic: false,
		},
		{
			name:        "Int type - source newer",
			sourceSpec:  intSpec,
			targetSpec:  intSpec,
			sourceData:  map[string][]interface{}{"version": {2}},
			targetData:  map[string][]interface{}{"version": {1}},
			sourceIndex: 0,
			targetIndex: 0,
			expected:    true,
			shouldPanic: false,
		},
		{
			name:        "Float64 type - source newer or equal",
			sourceSpec:  floatSpec,
			targetSpec:  floatSpec,
			sourceData:  map[string][]interface{}{"score": {90.5}, "id": {1}},
			targetData:  map[string][]interface{}{"score": {90.0}, "id": {1}},
			sourceIndex: 0,
			targetIndex: 0,
			expected:    true,
			shouldPanic: false,
		},
		{
			name:        "Different types",
			sourceSpec:  stringSpec,
			targetSpec:  mistypedSpec,
			sourceData:  map[string][]interface{}{"updated_at": {"2023-07-10"}},
			targetData:  map[string][]interface{}{"updated_at": {20230710}},
			sourceIndex: 0,
			targetIndex: 0,
			expected:    false,
			shouldPanic: true,
		},
		{
			name:        "Bool type - should panic",
			sourceSpec:  boolSpec,
			targetSpec:  boolSpec,
			sourceData:  map[string][]interface{}{"flag": {true}},
			targetData:  map[string][]interface{}{"flag": {false}},
			sourceIndex: 0,
			targetIndex: 0,
			expected:    false,
			shouldPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.shouldPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic but did not occur")
					}
				}()
			}

			result := NewerThan(tt.sourceSpec, tt.targetSpec, tt.sourceData, tt.targetData, tt.sourceIndex, tt.targetIndex)
			if result != tt.expected && !tt.shouldPanic {
				t.Errorf("NewerOrEqualsThan() = %v, want %v", result, tt.expected)
			}
		})
	}
}
