package equalizer

import (
	"fmt"

	"github.com/itaborai83/equalizer/pkg/hasher"
	"github.com/itaborai83/equalizer/pkg/specs"
	"github.com/itaborai83/equalizer/pkg/transpose"
)

type EqualizeResult struct {
	SourceSpec    *specs.TableSpec
	TargetSpec    *specs.TableSpec
	InsertData    interface{}
	UpdateData    interface{}
	DeleteData    interface{}
	EqualizedData interface{}
	Error         error
}

type PartitionAnalysisRequest struct {
	SourceSpec    *specs.TableSpec
	TargetSpec    *specs.TableSpec
	SourceData    map[string][]interface{}
	TargetData    map[string][]interface{}
	RowKeyHash    uint64
	SourceIndices []int
	TargetIndices []int
}

type PartitionAnalysisResult struct {
	RowKeyHash       uint64
	InsertIndices    []int
	UpdateIndices    []int
	DeleteIndices    []int
	EqualizedIndices []int
}

func GetColumnNames(data map[string][]interface{}) ([]string, error) {

	if len(data) == 0 {
		return nil, fmt.Errorf("no data to get column names")
	}
	columnNames := make([]string, len(data))
	i := 0
	for columnName := range data {
		columnNames[i] = columnName
		i++
	}
	return columnNames, nil
}

func ComputePartitionCount(sourceRowCount, targetRowCount int) int {
	return sourceRowCount + targetRowCount
}
func ComputeRowKeyHash(h *hasher.Hasher, spec *specs.TableSpec, data map[string][]interface{}, rowIndex int) (uint64, error) {
	h.Reset()
	for _, keyColumn := range spec.KeyColumns {
		columnValue, err := spec.GetColumnValue(keyColumn, rowIndex, data)
		if err != nil {
			return 0, err
		}
		h.Update(columnValue)
	}
	return h.GetHash()
}

func ComputeRowKeyHashes(spec *specs.TableSpec, columnNames []string, data map[string][]interface{}) (map[uint64][]int, error) {
	hashes := make(map[uint64][]int)
	hasher := hasher.NewHasher()
	rowCount := len(data[columnNames[0]])
	for i := 0; i < rowCount; i++ {
		hash, err := ComputeRowKeyHash(hasher, spec, data, i)
		if err != nil {
			return nil, err
		}
		hashes[hash] = append(hashes[hash], i)
	}
	return hashes, nil
}

func MergeRowKeyHashes(sourceRowKeyHashes, targetRowKeyHashes map[uint64][]int) []uint64 {
	seen := make(map[uint64]bool)
	merged := make([]uint64, 0)
	for hash, _ := range sourceRowKeyHashes {
		merged = append(merged, hash)
		seen[hash] = true
	}
	for hash, _ := range targetRowKeyHashes {
		_, ok := seen[hash]
		if !ok {
			merged = append(merged, hash)
		}
	}
	return merged
}

func ProcessPartition(request *PartitionAnalysisRequest, response *PartitionAnalysisResult) {
	result := &PartitionAnalysisResult{
		RowKeyHash:       request.RowKeyHash,
		InsertIndices:    make([]int, 0),
		UpdateIndices:    make([]int, 0),
		DeleteIndices:    make([]int, 0),
		EqualizedIndices: make([]int, 0),
	}

	matchedSourceIndices := make(map[int]bool)
	matchedTargetIndices := make(map[int]bool)

	// for each row in the source table
	for _, srcIndex := range request.SourceIndices {
		// for each row in the target table
		for _, tgtIndex := range request.TargetIndices {
			matchedRows := specs.SameKeys(request.SourceSpec, request.TargetSpec, request.SourceData, request.TargetData, srcIndex, tgtIndex)
			if matchedRows {
				// mark both rows as matched
				matchedSourceIndices[srcIndex] = true
				matchedTargetIndices[tgtIndex] = true

				// check if the source row is newer than the target row to determine whether to update or not
				newer := specs.NewerThan(request.SourceSpec, request.TargetSpec, request.SourceData, request.TargetData, srcIndex, tgtIndex)
				if newer {
					result.UpdateIndices = append(result.UpdateIndices, srcIndex)
				} else {
					result.EqualizedIndices = append(result.EqualizedIndices, srcIndex)
				}
			}
		}
		// for each row in the source table that has not been matched, mark it for insertion
		for _, srcIndex := range request.SourceIndices {
			_, ok := matchedSourceIndices[srcIndex]
			if !ok {
				result.InsertIndices = append(result.InsertIndices, srcIndex)
			}
		}
		// for each row in the target table that has not been matched, mark it for deletion
		for _, tgtIndex := range request.TargetIndices {
			_, ok := matchedTargetIndices[tgtIndex]
			if !ok {
				result.DeleteIndices = append(result.DeleteIndices, tgtIndex)
			}
		}
	}
}

func CopyData(sourceData map[string][]interface{}, indices []int) map[string][]interface{} {
	data := make(map[string][]interface{})
	for columnName, columnValues := range sourceData {
		data[columnName] = make([]interface{}, len(indices))
		for i, index := range indices {
			data[columnName][i] = columnValues[index]
		}
	}
	return data
}

func Equalize(sourceSpec, targetSpec *specs.TableSpec, sourceData, targetData interface{}) *EqualizeResult {
	result := &EqualizeResult{
		SourceSpec: sourceSpec,
		TargetSpec: targetSpec,
	}

	// check if the source and target tables are equalizable
	equalizable, err := sourceSpec.Equalizable(targetSpec)
	if err != nil {
		result.Error = err
		return result
	}

	if !equalizable {
		result.Error = fmt.Errorf("source and target tables are not equalizable according to their specs")
		return result
	}

	// does source data needs to be transposed to column format
	sourceInRowFormat := transpose.IsInRowFormat(sourceData)
	if sourceInRowFormat {
		sourceData, err = transpose.RowsToColumns(sourceData)
		if err != nil {
			result.Error = err
			return result
		}
	}
	sourceMapOfArrays, ok := sourceData.(map[string][]interface{})
	if !ok {
		result.Error = fmt.Errorf("source data is not in column format")
		return result
	}
	sourceColumnNames, err := GetColumnNames(sourceMapOfArrays)
	if err != nil {
		result.Error = err
		return result
	}
	sourceRowKeyHashes, err := ComputeRowKeyHashes(sourceSpec, sourceColumnNames, sourceMapOfArrays)
	if err != nil {
		result.Error = err
		return result
	}

	// does target data needs to be transposed to column format
	targetInRowFormat := transpose.IsInRowFormat(targetData)
	if targetInRowFormat {
		targetData, err = transpose.RowsToColumns(targetData)
		if err != nil {
			result.Error = err
			return result
		}
	}
	targetMapOfArrays, ok := targetData.(map[string][]interface{})
	if !ok {
		result.Error = fmt.Errorf("target data is not in column format")
		return result
	}
	targetColumnNames, err := GetColumnNames(targetMapOfArrays)
	if err != nil {
		result.Error = err
		return result
	}
	targetRowKeyHashes, err := ComputeRowKeyHashes(targetSpec, targetColumnNames, targetMapOfArrays)

	// merge the row key hashes
	mergedRowKeyHashes := MergeRowKeyHashes(sourceRowKeyHashes, targetRowKeyHashes)

	insertIndices := make([]int, 0)
	updateIndices := make([]int, 0)
	deleteIndices := make([]int, 0)
	equalizedIndices := make([]int, 0)

	request := PartitionAnalysisRequest{
		SourceSpec: sourceSpec,
		TargetSpec: targetSpec,
		SourceData: sourceMapOfArrays,
		TargetData: targetMapOfArrays,
	}
	var response PartitionAnalysisResult
	for _, currentHash := range mergedRowKeyHashes {
		request.RowKeyHash = currentHash
		request.SourceIndices = sourceRowKeyHashes[currentHash]
		request.TargetIndices = targetRowKeyHashes[currentHash]

		ProcessPartition(&request, &response)

		// copy the results
		insertIndices = append(insertIndices, response.InsertIndices...)
		updateIndices = append(updateIndices, response.UpdateIndices...)
		deleteIndices = append(deleteIndices, response.DeleteIndices...)
		equalizedIndices = append(equalizedIndices, response.EqualizedIndices...)

		// clear the response. Make sure no aliasing is happening to the indices array
		response.InsertIndices = response.InsertIndices[:0]
		response.UpdateIndices = response.UpdateIndices[:0]
		response.DeleteIndices = response.DeleteIndices[:0]
		response.EqualizedIndices = response.EqualizedIndices[:0]
	}

	// append the results to the response
	result.InsertData = CopyData(sourceMapOfArrays, insertIndices)
	result.UpdateData = CopyData(sourceMapOfArrays, updateIndices)
	result.InsertData = CopyData(targetMapOfArrays, deleteIndices)
	result.EqualizedData = CopyData(sourceMapOfArrays, equalizedIndices)

	return result
}
