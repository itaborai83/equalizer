package equalizer

import (
	"encoding/json"
	"testing"

	"github.com/itaborai83/equalizer/internal/utils"
	"github.com/itaborai83/equalizer/pkg/specs"
)

var (
	sourceSpec = &specs.TableSpec{
		Columns: []specs.ColumnSpec{
			{Name: "id", Type: specs.ColumnTypeInteger},
			{Name: "name", Type: specs.ColumnTypeString},
			{Name: "updated_at", Type: specs.ColumnTypeString},
		},
		KeyColumns:          []string{"id"},
		ChangeControlColumn: "updated_at",
	}
	targetSpec = &specs.TableSpec{
		Columns: []specs.ColumnSpec{
			{Name: "id", Type: specs.ColumnTypeInteger},
			{Name: "updated_at", Type: specs.ColumnTypeString},
		},
		KeyColumns:          []string{"id"},
		ChangeControlColumn: "updated_at",
	}
)

func TestComputePartitionMap(t *testing.T) {
	data := map[string][]interface{}{
		"id":         {1, 2, 3},
		"name":       {"Alice", "Bob", "Charlie"},
		"updated_at": {"2020-01-01", "2020-01-02", "2020-01-03"},
	}
	expectedHashes := []uint64{
		11168790016919534253,
		9760519482014861285,
		7660494990563649781,
	}
	// parition map has the hash as the key and a list of indices as the value
	partitionMap, err := ComputePartitionMap(sourceSpec, data)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// mark the hashes as seen
	seenHashes := make(map[uint64]bool)
	for hash, _ := range partitionMap {
		seenHashes[hash] = true
	}

	// check if the expected hashes are in the partition map
	for _, expectedHash := range expectedHashes {
		_, ok := seenHashes[expectedHash]
		if !ok {
			t.Errorf("expected hash %d not found in partitionMap", expectedHash)
		}
	}
}

func parseJsonOrPanic(jsonString string) interface{} {
	var data interface{}
	err := json.Unmarshal([]byte(jsonString), &data)
	if err != nil {
		panic(err)
	}
	return data
}

// A - Source: filled, Target: empty
// B - Source: columnar, Target: columnar
func TestEqualizeFilledColumnarSourceEmptyColumnarTarget(t *testing.T) {
	sourcePayload := `{
		"id": [1, 2, 3],
		"name": ["Alice", "Bob", "Charlie"],
		"updated_at": ["2020-01-01", "2020-01-02", "2020-01-03"]
	}`
	sourceData := parseJsonOrPanic(sourcePayload)
	targetData := parseJsonOrPanic(`{}`)
	expectedEqualizedData := sourceSpec.NewColumnarTable()
	expectedUpdateData := sourceSpec.NewColumnarTable()
	expectedDeletedData := targetSpec.NewColumnarTable()
	expectedInsertedData := map[string][]interface{}{
		"id":         {1, 2, 3},
		"name":       {"Alice", "Bob", "Charlie"},
		"updated_at": {"2020-01-01", "2020-01-02", "2020-01-03"},
	}

	result, err := Run(sourceSpec, targetSpec, sourceData, targetData)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	equalizedEquals := utils.RecursiveUntypedEquals(result.EqualizedData, expectedEqualizedData)
	if !equalizedEquals {
		t.Errorf("expected EqualizedData '%v', got '%v'", expectedEqualizedData, result.EqualizedData)
	}

	updateEquals := utils.RecursiveUntypedEquals(result.UpdateData, expectedUpdateData)
	if !updateEquals {
		t.Errorf("expected UpdateData '%v', got '%v'", expectedUpdateData, result.UpdateData)
	}

	deletedEquals := utils.RecursiveUntypedEquals(result.DeleteData, expectedDeletedData)
	if !deletedEquals {
		t.Errorf("expected DeletedData '%v', got '%v'", expectedDeletedData, result.DeleteData)
	}

	insertedEquals := utils.RecursiveUntypedEquals(result.InsertData, expectedInsertedData)
	if !insertedEquals {
		t.Errorf("expected InsertedData '%v', got '%v'", expectedInsertedData, result.InsertData)
	}
}

// A - Source: filled, Target: empty
// B - Source: row, Target: row
func TestEqualizeFilledSourceRowEmptyTargetRow(t *testing.T) {
	sourcePayload := `[
		{ "id": 1, "name": "Alice", "updated_at": "2020-01-01" },
		{ "id": 2, "name": "Bob", "updated_at": "2020-01-02" },
		{ "id": 3, "name": "Charlie", "updated_at": "2020-01-03" }
	]`
	sourceData := parseJsonOrPanic(sourcePayload)
	targetData := parseJsonOrPanic(`[]`)
	expectedEqualizedData := []interface{}{}
	expectedUpdateData := []interface{}{}
	expectedDeletedData := []interface{}{}
	expectedInsertedData := []map[string]interface{}{
		{"id": 1, "name": "Alice", "updated_at": "2020-01-01"},
		{"id": 2, "name": "Bob", "updated_at": "2020-01-02"},
		{"id": 3, "name": "Charlie", "updated_at": "2020-01-03"},
	}

	result, err := Run(sourceSpec, targetSpec, sourceData, targetData)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	equalizedEquals := utils.RecursiveUntypedEquals(result.EqualizedData, expectedEqualizedData)
	if !equalizedEquals {
		t.Errorf("expected EqualizedData '%v', got '%v'", expectedEqualizedData, result.EqualizedData)
	}

	updateEquals := utils.RecursiveUntypedEquals(result.UpdateData, expectedUpdateData)
	if !updateEquals {
		t.Errorf("expected UpdateData '%v', got '%v'", expectedUpdateData, result.UpdateData)
	}

	deletedEquals := utils.RecursiveUntypedEquals(result.DeleteData, expectedDeletedData)
	if !deletedEquals {
		t.Errorf("expected DeletedData '%v', got '%v'", expectedDeletedData, result.DeleteData)
	}

	insertedEquals := utils.RecursiveUntypedEquals(result.InsertData, expectedInsertedData)
	if !insertedEquals {
		t.Errorf("expected InsertedData '%v', got '%v'", expectedInsertedData, result.InsertData)
	}
}

// A - Source: filled, Target: empty
// B - Source: row, Target: columnar
func TestEqualizeFilledRowSourceEmptyColumnarTarget(t *testing.T) {
	sourcePayload := `[
		{ "id": 1, "name": "Alice", "updated_at": "2020-01-01" },
		{ "id": 2, "name": "Bob", "updated_at": "2020-01-02" },
		{ "id": 3, "name": "Charlie", "updated_at": "2020-01-03" }
	]`
	sourceData := parseJsonOrPanic(sourcePayload)
	targetData := parseJsonOrPanic(`{}`)
	expectedEqualizedData := sourceSpec.NewColumnarTable()
	expectedUpdateData := sourceSpec.NewColumnarTable()
	expectedDeletedData := targetSpec.NewColumnarTable()
	expectedInsertedData := map[string][]interface{}{
		"id":         {1, 2, 3},
		"name":       {"Alice", "Bob", "Charlie"},
		"updated_at": {"2020-01-01", "2020-01-02", "2020-01-03"},
	}

	result, err := Run(sourceSpec, targetSpec, sourceData, targetData)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	equalizedEquals := utils.RecursiveUntypedEquals(result.EqualizedData, expectedEqualizedData)
	if !equalizedEquals {
		t.Errorf("expected EqualizedData '%v', got '%v'", expectedEqualizedData, result.EqualizedData)
	}

	updateEquals := utils.RecursiveUntypedEquals(result.UpdateData, expectedUpdateData)
	if !updateEquals {
		t.Errorf("expected UpdateData '%v', got '%v'", expectedUpdateData, result.UpdateData)
	}

	deletedEquals := utils.RecursiveUntypedEquals(result.DeleteData, expectedDeletedData)
	if !deletedEquals {
		t.Errorf("expected DeletedData '%v', got '%v'", expectedDeletedData, result.DeleteData)
	}

	insertedEquals := utils.RecursiveUntypedEquals(result.InsertData, expectedInsertedData)
	if !insertedEquals {
		t.Errorf("expected InsertedData '%v', got '%v'", expectedInsertedData, result.InsertData)
	}
}

// A - Source: filled, Target: empty
// B - Source: columnar, Target: row
func TestEqualizeFilledColumnarSourceEmptyRowTarget(t *testing.T) {
	sourcePayload := `{
		"id": [1, 2, 3],
		"name": ["Alice", "Bob", "Charlie"],
		"updated_at": ["2020-01-01", "2020-01-02", "2020-01-03"]
	}`
	sourceData := parseJsonOrPanic(sourcePayload)
	targetData := parseJsonOrPanic(`[]`)
	expectedEqualizedData := []interface{}{}
	expectedUpdateData := []interface{}{}
	expectedDeletedData := []interface{}{}
	expectedInsertedData := []map[string]interface{}{
		{"id": 1, "name": "Alice", "updated_at": "2020-01-01"},
		{"id": 2, "name": "Bob", "updated_at": "2020-01-02"},
		{"id": 3, "name": "Charlie", "updated_at": "2020-01-03"},
	}

	result, err := Run(sourceSpec, targetSpec, sourceData, targetData)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	equalizedEquals := utils.RecursiveUntypedEquals(result.EqualizedData, expectedEqualizedData)
	if !equalizedEquals {
		t.Errorf("expected EqualizedData '%v', got '%v'", expectedEqualizedData, result.EqualizedData)
	}

	updateEquals := utils.RecursiveUntypedEquals(result.UpdateData, expectedUpdateData)
	if !updateEquals {
		t.Errorf("expected UpdateData '%v', got '%v'", expectedUpdateData, result.UpdateData)
	}

	deletedEquals := utils.RecursiveUntypedEquals(result.DeleteData, expectedDeletedData)
	if !deletedEquals {
		t.Errorf("expected DeletedData '%v', got '%v'", expectedDeletedData, result.DeleteData)
	}

	insertedEquals := utils.RecursiveUntypedEquals(result.InsertData, expectedInsertedData)
	if !insertedEquals {
		t.Errorf("expected InsertedData '%v', got '%v'", expectedInsertedData, result.InsertData)
	}
}

// A - Source: empty, Target: filled
// B - Source: columnar, Target: columnar
func TestEqualizeEmptyColumnarSourceFilledColumnarTarget(t *testing.T) {
	sourceData := parseJsonOrPanic(`{}`)
	targetPayload := `{
		"id": [1, 2, 3],
		"updated_at": ["2020-01-01", "2020-01-02", "2020-01-03"]
	}`
	targetData := parseJsonOrPanic(targetPayload)
	expectedEqualizedData := sourceSpec.NewColumnarTable()
	expectedUpdateData := sourceSpec.NewColumnarTable()
	expectedInsertedData := sourceSpec.NewColumnarTable()
	expectedDeletedData := map[string][]interface{}{
		"id":         {1, 2, 3},
		"updated_at": {"2020-01-01", "2020-01-02", "2020-01-03"},
	}

	result, err := Run(sourceSpec, targetSpec, sourceData, targetData)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	equalizedEquals := utils.RecursiveUntypedEquals(result.EqualizedData, expectedEqualizedData)
	if !equalizedEquals {
		t.Errorf("expected EqualizedData '%v', got '%v'", expectedEqualizedData, result.EqualizedData)
	}

	updateEquals := utils.RecursiveUntypedEquals(result.UpdateData, expectedUpdateData)
	if !updateEquals {
		t.Errorf("expected UpdateData '%v', got '%v'", expectedUpdateData, result.UpdateData)
	}

	deletedEquals := utils.RecursiveUntypedEquals(result.DeleteData, expectedDeletedData)
	if !deletedEquals {
		t.Errorf("expected DeletedData '%v', got '%v'", expectedDeletedData, result.DeleteData)
	}

	insertedEquals := utils.RecursiveUntypedEquals(result.InsertData, expectedInsertedData)
	if !insertedEquals {
		t.Errorf("expected InsertedData '%v', got '%v'", expectedInsertedData, result.InsertData)
	}
}

// A - Source: empty, Target: filled
// B - Source: columnar, Target: row
func TestEqualizedEmptyColumnarSourceFilledRowTarget(t *testing.T) {
	sourceData := parseJsonOrPanic(`{}`)
	targetPayload := `[
		{ "id": 1, "updated_at": "2020-01-01" },
		{ "id": 2, "updated_at": "2020-01-02" },
		{ "id": 3, "updated_at": "2020-01-03" }
	]`
	targetData := parseJsonOrPanic(targetPayload)
	expectedEqualizedData := []interface{}{}
	expectedUpdateData := []interface{}{}
	expectedInsertedData := []interface{}{}
	expectedDeletedData := []map[string]interface{}{
		{"id": 1, "updated_at": "2020-01-01"},
		{"id": 2, "updated_at": "2020-01-02"},
		{"id": 3, "updated_at": "2020-01-03"},
	}

	result, err := Run(sourceSpec, targetSpec, sourceData, targetData)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	equalizedEquals := utils.RecursiveUntypedEquals(result.EqualizedData, expectedEqualizedData)
	if !equalizedEquals {
		t.Errorf("expected EqualizedData '%v', got '%v'", expectedEqualizedData, result.EqualizedData)
	}

	updateEquals := utils.RecursiveUntypedEquals(result.UpdateData, expectedUpdateData)
	if !updateEquals {
		t.Errorf("expected UpdateData '%v', got '%v'", expectedUpdateData, result.UpdateData)
	}

	deletedEquals := utils.RecursiveUntypedEquals(result.DeleteData, expectedDeletedData)
	if !deletedEquals {
		t.Errorf("expected DeletedData '%v', got '%v'", expectedDeletedData, result.DeleteData)
	}

	insertedEquals := utils.RecursiveUntypedEquals(result.InsertData, expectedInsertedData)
	if !insertedEquals {
		t.Errorf("expected InsertedData '%v', got '%v'", expectedInsertedData, result.InsertData)
	}
}

// A - Source: empty, Target: filled
// B - Source: row, Target: row
func TestEqualizeEmptyRowSourceFilledRowTarget(t *testing.T) {
	sourceData := parseJsonOrPanic(`[]`)
	targetPayload := `[
		{ "id": 1, "name": "Alice", "updated_at": "2020-01-01" },
		{ "id": 2, "name": "Bob", "updated_at": "2020-01-02" },
		{ "id": 3, "name": "Charlie", "updated_at": "2020-01-03" }
	]`
	targetData := parseJsonOrPanic(targetPayload)
	expectedEqualizedData := []interface{}{}
	expectedUpdateData := []interface{}{}
	expectedInsertedData := []interface{}{}
	expectedDeletedData := []map[string]interface{}{
		{"id": 1, "name": "Alice", "updated_at": "2020-01-01"},
		{"id": 2, "name": "Bob", "updated_at": "2020-01-02"},
		{"id": 3, "name": "Charlie", "updated_at": "2020-01-03"},
	}

	result, err := Run(sourceSpec, targetSpec, sourceData, targetData)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	equalizedEquals := utils.RecursiveUntypedEquals(result.EqualizedData, expectedEqualizedData)
	if !equalizedEquals {
		t.Errorf("expected EqualizedData '%v', got '%v'", expectedEqualizedData, result.EqualizedData)
	}

	updateEquals := utils.RecursiveUntypedEquals(result.UpdateData, expectedUpdateData)
	if !updateEquals {
		t.Errorf("expected UpdateData '%v', got '%v'", expectedUpdateData, result.UpdateData)
	}

	deletedEquals := utils.RecursiveUntypedEquals(result.DeleteData, expectedDeletedData)
	if !deletedEquals {
		t.Errorf("expected DeletedData '%v', got '%v'", expectedDeletedData, result.DeleteData)
	}

	insertedEquals := utils.RecursiveUntypedEquals(result.InsertData, expectedInsertedData)
	if !insertedEquals {
		t.Errorf("expected InsertedData '%v', got '%v'", expectedInsertedData, result.InsertData)
	}
}

// A - Source: empty, Target: filled
// B - Source: row, Target: columnar
func TestEqualizeEmptyRowSourceFilledColumnarTarget(t *testing.T) {
	sourceData := parseJsonOrPanic(`[]`)
	targetPayload := `{
		"id": [1, 2, 3],
		"name": ["Alice", "Bob", "Charlie"],
		"updated_at": ["2020-01-01", "2020-01-02", "2020-01-03"]
	}`
	targetData := parseJsonOrPanic(targetPayload)
	expectedEqualizedData := sourceSpec.NewColumnarTable()
	expectedUpdateData := sourceSpec.NewColumnarTable()
	expectedInsertedData := sourceSpec.NewColumnarTable()
	expectedDeletedData := map[string][]interface{}{
		"id":         {1, 2, 3},
		"name":       {"Alice", "Bob", "Charlie"},
		"updated_at": {"2020-01-01", "2020-01-02", "2020-01-03"},
	}

	result, err := Run(sourceSpec, targetSpec, sourceData, targetData)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	equalizedEquals := utils.RecursiveUntypedEquals(result.EqualizedData, expectedEqualizedData)
	if !equalizedEquals {
		t.Errorf("expected EqualizedData '%v', got '%v'", expectedEqualizedData, result.EqualizedData)
	}

	updateEquals := utils.RecursiveUntypedEquals(result.UpdateData, expectedUpdateData)
	if !updateEquals {
		t.Errorf("expected UpdateData '%v', got '%v'", expectedUpdateData, result.UpdateData)
	}

	deletedEquals := utils.RecursiveUntypedEquals(result.DeleteData, expectedDeletedData)
	if !deletedEquals {
		t.Errorf("expected DeletedData '%v', got '%v'", expectedDeletedData, result.DeleteData)
	}

	insertedEquals := utils.RecursiveUntypedEquals(result.InsertData, expectedInsertedData)
	if !insertedEquals {
		t.Errorf("expected InsertedData '%v', got '%v'", expectedInsertedData, result.InsertData)
	}
}

// A - Source: filled, Target: filled
// B - Source: columnar, Target: columnar
func TestEqualizeFilledColumnarSourceFilledColumnarTarget(t *testing.T) {
	sourcePayload := `{
		"id": [1, 2, 3],
		"name": ["Alice", "Bob", "Charlie"],
		"updated_at": ["2020-01-01", "2020-01-03", "2020-01-03"]
	}`
	sourceData := parseJsonOrPanic(sourcePayload)
	targetPayload := `{
		"id": [1, 2, 4],
		"updated_at": ["2020-01-01", "2020-01-02", "2020-01-03"]
	}`
	targetData := parseJsonOrPanic(targetPayload)
	expectedEqualizedData := parseJsonOrPanic(`{ "id": [1], "name": ["Alice"], "updated_at": ["2020-01-01"] }`)
	expectedUpdateData := parseJsonOrPanic(`{ "id": [2], "name": ["Bob"], "updated_at": ["2020-01-03"] }`)
	expectedInsertedData := parseJsonOrPanic(`{ "id": [3], "name": ["Charlie"], "updated_at": ["2020-01-03"] }`)
	expectedDeletedData := parseJsonOrPanic(`{ "id": [4], "updated_at": ["2020-01-03"] }`)

	result, err := Run(sourceSpec, targetSpec, sourceData, targetData)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	equalizedEquals := utils.RecursiveUntypedEquals(result.EqualizedData, expectedEqualizedData)
	if !equalizedEquals {
		t.Errorf("expected EqualizedData '%v', got '%v'", expectedEqualizedData, result.EqualizedData)
	}

	updateEquals := utils.RecursiveUntypedEquals(result.UpdateData, expectedUpdateData)
	if !updateEquals {
		t.Errorf("expected UpdateData '%v', got '%v'", expectedUpdateData, result.UpdateData)
	}

	deletedEquals := utils.RecursiveUntypedEquals(result.DeleteData, expectedDeletedData)
	if !deletedEquals {
		t.Errorf("expected DeletedData '%v', got '%v'", expectedDeletedData, result.DeleteData)
	}

	insertedEquals := utils.RecursiveUntypedEquals(result.InsertData, expectedInsertedData)
	if !insertedEquals {
		t.Errorf("expected InsertedData '%v', got '%v'", expectedInsertedData, result.InsertData)
	}
}

/*
func TestEqualize(t *testing.T) {
	sourceSpec := &specs.TableSpec{
		Columns: []specs.ColumnSpec{
			{Name: "id", Type: specs.ColumnTypeInteger},
			{Name: "name", Type: specs.ColumnTypeString},
			{Name: "surname", Type: specs.ColumnTypeString},
			{Name: "age", Type: specs.ColumnTypeInteger},
			{Name: "updated_at", Type: specs.ColumnTypeString},
		},
		KeyColumns:          []string{"id"},
		ChangeControlColumn: "updated_at",
	}
	targetSpec := &specs.TableSpec{
		Columns: []specs.ColumnSpec{
			{Name: "id", Type: specs.ColumnTypeInteger},
			{Name: "updated_at", Type: specs.ColumnTypeString},
		},
		KeyColumns:          []string{"id"},
		ChangeControlColumn: "updated_at",
	}
	// first element is equalized
	// second element is to be updated
	// third element is to be inserted
	sourceMapOfArrays := map[string][]interface{}{
		"id":         {1, 2, 3},
		"name":       {"Alice", "Bob", "Charlie"},
		"surname":    {"Smith", "Jones", "Brown"},
		"age":        {30, 40, 50},
		"updated_at": {"2020-01-01", "2020-01-03", "2020-01-04"},
	}
	// first element is equalized
	// second element is to be updated
	// third element is to be deleted
	targetMapOfArrays := map[string][]interface{}{
		"id":         {1, 2, 4},
		"updated_at": {"2020-01-01", "2020-01-02", "2020-01-04"},
	}

	result, err := Run(sourceSpec, targetSpec, sourceMapOfArrays, targetMapOfArrays)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	fmt.Println("Equalize Run Result")
	fmt.Println("EqualizedData: " + fmt.Sprint(result.EqualizedData))
	fmt.Println("UpdatedData: " + fmt.Sprint(result.UpdateData))
	fmt.Println("InsertedData: " + fmt.Sprint(result.InsertData))
	fmt.Println("DeletedData: " + fmt.Sprint(result.DeleteData))

	expectedEqualizedData := map[string][]interface{}{
		"id":         {1},
		"name":       {"Alice"},
		"surname":    {"Smith"},
		"age":        {30},
		"updated_at": {"2020-01-01"},
	}

	expectedUpdatedData := map[string][]interface{}{
		"id":         {2},
		"name":       {"Bob"},
		"surname":    {"Jones"},
		"age":        {40},
		"updated_at": {"2020-01-03"},
	}

	expectedInsertedData := map[string][]interface{}{
		"id":         {3},
		"name":       {"Charlie"},
		"surname":    {"Brown"},
		"age":        {50},
		"updated_at": {"2020-01-04"},
	}

	expectedDeletedData := map[string][]interface{}{
		"id":         {4},
		"updated_at": {"2020-01-04"},
	}

	for columnName, expectedColumnValues := range expectedEqualizedData {
		data, ok := result.EqualizedData.(map[string][]interface{})
		if !ok {
			t.Errorf("EqualizedData is not a map")
			return
		}
		columnValues, ok := data[columnName]
		if !ok {
			t.Errorf("column %s not found in EqualizedData", columnName)
			continue
		}
		if len(columnValues) != len(expectedColumnValues) {
			t.Errorf("column %s has %d values, expected %d", columnName, len(columnValues), len(expectedColumnValues))
			return
		}
		for i, expectedColumnValue := range expectedColumnValues {
			if columnValues[i] != expectedColumnValue {
				t.Errorf("columnValues[%d] = %v, expected %v", i, columnValues[i], expectedColumnValue)
			}
		}
	}

	for columnName, expectedColumnValues := range expectedUpdatedData {
		data, ok := result.UpdateData.(map[string][]interface{})
		if !ok {
			t.Errorf("UpdateData is not a map")
			return
		}
		columnValues, ok := data[columnName]
		if !ok {
			t.Errorf("column %s not found in UpdateData", columnName)
			continue
		}
		if len(columnValues) != len(expectedColumnValues) {
			t.Errorf("column %s has %d values, expected %d", columnName, len(columnValues), len(expectedColumnValues))
			return
		}
		for i, expectedColumnValue := range expectedColumnValues {
			if columnValues[i] != expectedColumnValue {
				t.Errorf("columnValues[%d] = %v, expected %v", i, columnValues[i], expectedColumnValue)
			}
		}
	}

	for columnName, expectedColumnValues := range expectedInsertedData {
		data, ok := result.InsertData.(map[string][]interface{})
		if !ok {
			t.Errorf("InsertData is not a map")
			return
		}
		columnValues, ok := data[columnName]
		if !ok {
			t.Errorf("column %s not found in InsertData", columnName)
			continue
		}
		if len(columnValues) != len(expectedColumnValues) {
			t.Errorf("column %s has %d values, expected %d", columnName, len(columnValues), len(expectedColumnValues))
			return
		}
		for i, expectedColumnValue := range expectedColumnValues {
			if columnValues[i] != expectedColumnValue {
				t.Errorf("columnValues[%d] = %v, expected %v", i, columnValues[i], expectedColumnValue)
			}
		}
	}

	for columnName, expectedColumnValues := range expectedDeletedData {
		data, ok := result.DeleteData.(map[string][]interface{})
		if !ok {
			t.Errorf("DeleteData is not a map")
			return
		}
		columnValues, ok := data[columnName]
		if !ok {
			t.Errorf("column %s not found in DeleteData", columnName)
			continue
		}
		if len(columnValues) != len(expectedColumnValues) {
			t.Errorf("column %s has %d values, expected %d", columnName, len(columnValues), len(expectedColumnValues))
			return
		}
		for i, expectedColumnValue := range expectedColumnValues {
			if columnValues[i] != expectedColumnValue {
				t.Errorf("columnValues[%d] = %v, expected %v", i, columnValues[i], expectedColumnValue)
			}
		}
	}
}
*/
