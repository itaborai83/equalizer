package transposer

import (
	"encoding/json"
	"testing"

	"github.com/itaborai83/equalizer/internal/utils"
	"github.com/itaborai83/equalizer/pkg/specs"
)

func TestIsInRowFormat(t *testing.T) {
	tests := []struct {
		Name          string
		Json          string
		ExpectedValue interface{}
	}{
		{
			Name:          "Valid Row Format",
			Json:          "[{\"col1\": 1, \"col2\": \"test\"}]",
			ExpectedValue: true,
		},
		{
			Name:          "Invalid Row Format",
			Json:          "{\"col1\": 1, \"col2\": \"test\"}",
			ExpectedValue: false,
		},
		{
			Name:          "Empty Row Format",
			Json:          "[]",
			ExpectedValue: true,
		},
		{
			Name:          "Scalar payload",
			Json:          "1",
			ExpectedValue: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			var data interface{}
			err := json.Unmarshal([]byte(tt.Json), &data)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			value := IsInRowFormat(data)
			if value != tt.ExpectedValue {
				t.Errorf("IsInRowFormat() value = %v, expectedValue %v", value, tt.ExpectedValue)
			}
		})
	}
}

func TestIsInColumnFormat(t *testing.T) {
	tests := []struct {
		Name          string
		Json          string
		ExpectedValue interface{}
	}{
		{
			Name:          "Valid Column Format",
			Json:          "{\"col1\": [1, 2], \"col2\": [\"test1\", \"test2\"]}",
			ExpectedValue: true,
		},
		{
			Name:          "Invalid Column Format",
			Json:          "[{\"col1\": 1, \"col2\": \"test\"}]",
			ExpectedValue: false,
		},
		{
			Name:          "Empty Column Format",
			Json:          "{}",
			ExpectedValue: true,
		},
		{
			Name:          "Scalar payload",
			Json:          "1",
			ExpectedValue: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			var data interface{}
			err := json.Unmarshal([]byte(tt.Json), &data)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			value := IsInColumnFormat(data)
			if value != tt.ExpectedValue {
				t.Errorf("IsInColumnFormat() value = %v, expectedValue %v", value, tt.ExpectedValue)
			}
		})
	}
}

func TestRowToColumns(t *testing.T) {
	spec := &specs.TableSpec{
		Columns: []specs.ColumnSpec{
			{Name: "col1", Type: specs.ColumnTypeInteger},
			{Name: "col2", Type: specs.ColumnTypeString},
		},
		KeyColumns:          []string{"col1"},
		ChangeControlColumn: "",
	}

	tests := []struct {
		Name          string
		Json          string
		ExpectedValue interface{}
		ExpectedError bool
	}{
		{
			Name:          "Valid Row Format",
			Json:          "[{\"col1\": 1, \"col2\": \"test\"}, {\"col1\": 2, \"col2\": \"test2\"}]",
			ExpectedValue: map[string][]interface{}{"col1": {1, 2}, "col2": {"test", "test2"}},
			ExpectedError: false,
		},
		{
			Name:          "Invalid Row Format",
			Json:          "{\"col1\": 1, \"col2\": \"test\"}",
			ExpectedValue: nil,
			ExpectedError: true,
		},
		{
			Name:          "Empty Row Format",
			Json:          "[]",
			ExpectedValue: map[string][]interface{}{"col1": {}, "col2": {}},
			ExpectedError: false,
		},
		{
			Name:          "Invalid Type",
			Json:          "[{\"col1\": 1, \"col2\": \"test\"}, {\"col1\": \"2\", \"col2\": \"test2\"}]",
			ExpectedValue: nil,
			ExpectedError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			var data interface{}
			err := json.Unmarshal([]byte(tt.Json), &data)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			value, err := RowsToColumns(spec, data)
			if (err != nil) != tt.ExpectedError {
				t.Errorf("RowsToColumns() error = %v, expectedError %v", err, tt.ExpectedError)
				return
			}
			equals := utils.RecursiveUntypedEquals(value, tt.ExpectedValue)
			if !equals {
				t.Errorf("RowsToColumns() value = %v, expectedValue %v", value, tt.ExpectedValue)
			}
		})
	}
}

func TestColumnsToRows(t *testing.T) {
	spec := &specs.TableSpec{
		Columns: []specs.ColumnSpec{
			{Name: "col1", Type: specs.ColumnTypeInteger},
			{Name: "col2", Type: specs.ColumnTypeString},
		},
		KeyColumns:          []string{"col1"},
		ChangeControlColumn: "",
	}

	tests := []struct {
		Name          string
		Json          string
		ExpectedValue interface{}
		ExpectedError bool
	}{
		{
			Name:          "Valid Column Format",
			Json:          "{\"col1\": [1, 2], \"col2\": [\"test\", \"test2\"]}",
			ExpectedValue: []interface{}{map[string]interface{}{"col1": 1, "col2": "test"}, map[string]interface{}{"col1": 2, "col2": "test2"}},
			ExpectedError: false,
		},
		{
			Name:          "Invalid Column Format",
			Json:          "[{\"col1\": 1, \"col2\": \"test\"}]",
			ExpectedValue: nil,
			ExpectedError: true,
		},
		{
			Name:          "Empty Column Format",
			Json:          "{}",
			ExpectedValue: []interface{}{},
			ExpectedError: false,
		},
		{
			Name:          "Invalid Type",
			Json:          "{\"col1\": [1, 2], \"col2\": [\"test\", 2]}",
			ExpectedValue: nil,
			ExpectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			var data interface{}
			err := json.Unmarshal([]byte(tt.Json), &data)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			value, err := ColumnsToRows(spec, data)
			if (err != nil) != tt.ExpectedError {
				t.Errorf("ColumnsToRows() error = '%v', expectedError '%v'", err, tt.ExpectedError)
				return
			}
			equals := utils.RecursiveUntypedEquals(value, tt.ExpectedValue)
			if !equals {
				t.Errorf("ColumnsToRows() value = '%v', expectedValue '%v'", value, tt.ExpectedValue)
			}
		})
	}
}
