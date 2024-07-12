package equalizer

import (
	"fmt"

	"github.com/itaborai83/equalizer/internal/utils"
	"github.com/itaborai83/equalizer/pkg/hasher"
	"github.com/itaborai83/equalizer/pkg/specs"
	"github.com/itaborai83/equalizer/pkg/transposer"
)

type EqualizeResult struct {
	InsertData    interface{}
	UpdateData    interface{}
	DeleteData    interface{}
	EqualizedData interface{}
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
	InsertIndices    []int
	UpdateIndices    []int
	DeleteIndices    []int
	EqualizedIndices []int
}

var (
	log = utils.NewLogger("equalizer")
)

func IsEmpty(data interface{}) bool {
	aList, ok := data.([]interface{})
	if ok {
		return len(aList) == 0
	}
	anotherList, ok := data.([]map[string]interface{})
	if ok {
		return len(anotherList) == 0
	}

	aMap, ok := data.(map[string]interface{})
	if ok {
		return len(aMap) == 0
	}
	anotherMap, ok := data.(map[string][]interface{})
	if ok {
		return len(anotherMap) == 0
	}
	panic("data is not a list or a map")
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

func ComputePartitionMap(spec *specs.TableSpec, data map[string][]interface{}) (map[uint64][]int, error) {
	columnNames := make([]string, 0)
	for _, column := range spec.Columns {
		columnNames = append(columnNames, column.Name)
	}
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
	for hash := range sourceRowKeyHashes {
		merged = append(merged, hash)
		seen[hash] = true
	}
	for hash := range targetRowKeyHashes {
		_, ok := seen[hash]
		if !ok {
			merged = append(merged, hash)
		}
	}
	return merged
}

func ProcessPartition(request *PartitionAnalysisRequest, response *PartitionAnalysisResult) {
	// reset the response
	response.InsertIndices = make([]int, 0)
	response.UpdateIndices = make([]int, 0)
	response.DeleteIndices = make([]int, 0)
	response.EqualizedIndices = make([]int, 0)

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
					response.UpdateIndices = append(response.UpdateIndices, srcIndex)
				} else {
					response.EqualizedIndices = append(response.EqualizedIndices, srcIndex)
				}
			}
		}
	}
	// for each row in the source table that has not been matched, mark it for insertion
	for _, srcIndex := range request.SourceIndices {
		_, ok := matchedSourceIndices[srcIndex]
		if !ok {
			response.InsertIndices = append(response.InsertIndices, srcIndex)
		}
	}
	// for each row in the target table that has not been matched, mark it for deletion
	for _, tgtIndex := range request.TargetIndices {
		_, ok := matchedTargetIndices[tgtIndex]
		if !ok {
			response.DeleteIndices = append(response.DeleteIndices, tgtIndex)
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

func Run(sourceSpec, targetSpec *specs.TableSpec, sourceData, targetData interface{}) (*EqualizeResult, error) {
	log.Println("equalizing data")
	var msg string
	result := &EqualizeResult{}

	log.Println("checking if source and target tables are equalizable")
	equalizable, err := sourceSpec.Equalizable(targetSpec)
	if err != nil {
		return nil, err
	}

	if !equalizable {
		msg = "source and target tables are not equalizable according to their specs"
		log.Print(msg)
		err = fmt.Errorf(msg)
		return nil, err
	}

	isSourceEmpty := IsEmpty(sourceData)
	isTargetEmpty := IsEmpty(targetData)
	if isSourceEmpty && isTargetEmpty {
		msg = "source and target data are empty"
		log.Print(msg)
		err = fmt.Errorf(msg)
		return nil, err
	}

	isSourceColumnar := transposer.IsInColumnFormat(sourceData)
	isSourceRow := transposer.IsInRowFormat(sourceData)
	isTargetColumnar := transposer.IsInColumnFormat(targetData)
	isTargetRow := transposer.IsInRowFormat(targetData)

	if !isSourceColumnar && !isSourceRow {
		msg = "source data is not in columnar or row format"
		log.Print(msg)
		err = fmt.Errorf(msg)
		return nil, err
	}

	if !isTargetColumnar && !isTargetRow {
		msg = "target data is not in columnar or row format"
		log.Print(msg)
		err = fmt.Errorf(msg)
		return nil, err
	}

	var sourceMapOfArrays map[string][]interface{}
	var targetMapOfArrays map[string][]interface{}

	if !isSourceEmpty {
		log.Println("converting source data to a map of string to array of interfaces")
		sourceMapOfArrays, err = transposer.ConvertToColumnarFormat(sourceSpec, sourceData)
		if err != nil {
			return nil, fmt.Errorf("source data error: " + err.Error())
		}
	}

	if !isTargetEmpty {
		log.Println("converting target data to a map of string to array of interfaces")
		targetMapOfArrays, err = transposer.ConvertToColumnarFormat(targetSpec, targetData)
		if err != nil {
			return nil, fmt.Errorf("target data error: " + err.Error())
		}
	}

	if isTargetEmpty {
		log.Println("target data is empty. returning source data as insert data")

		if isTargetColumnar {
			result.EqualizedData = sourceSpec.NewColumnarTable()
			result.UpdateData = sourceSpec.NewColumnarTable()
			result.DeleteData = targetSpec.NewColumnarTable()
		}

		if isTargetRow {
			result.EqualizedData = []interface{}{}
			result.UpdateData = []interface{}{}
			result.DeleteData = []interface{}{}
		}

		// A - Source: filled, Target: empty
		// B - Source: columnar, Target: columnar
		if isTargetColumnar && isSourceColumnar {
			result.InsertData = sourceData
			return result, nil
		}

		// A - Source: filled, Target: empty
		// B - Source: columnar, Target: row
		if isTargetColumnar && isSourceRow {
			result.InsertData = sourceMapOfArrays
			return result, nil
		}

		// A - Source: filled, Target: empty
		// B - Source: row, Target: row
		if isTargetRow && isSourceRow {
			result.InsertData = sourceData
			return result, nil
		}

		// A - Source: filled, Target: empty
		// B - Source: row, Target: columnar
		if isTargetRow && isSourceColumnar {
			result.InsertData, err = transposer.ConvertToRowFormat(sourceSpec, sourceMapOfArrays)
			if err != nil {
				return nil, fmt.Errorf("source data error: " + err.Error())
			}
			return result, nil
		}

		panic("should not have reached here")
	}

	if isSourceEmpty {
		log.Println("source data is empty. returning target data as delete data")

		if isTargetColumnar {
			result.EqualizedData = sourceSpec.NewColumnarTable()
			result.UpdateData = sourceSpec.NewColumnarTable()
			result.InsertData = sourceSpec.NewColumnarTable()
		}

		if isTargetRow {
			result.EqualizedData = []interface{}{}
			result.UpdateData = []interface{}{}
			result.InsertData = []interface{}{}
		}

		// A - Source: empty, Target: filled
		// B - Source: columnar, Target: columnar
		if isTargetColumnar && isSourceColumnar {
			result.DeleteData = targetData
			return result, nil
		}

		// A - Source: empty, Target: filled
		// B - Source: columnar, Target: row
		if isTargetColumnar && isSourceRow {
			result.DeleteData = targetMapOfArrays
			return result, nil
		}

		// A - Source: empty, Target: filled
		// B - Source: row, Target: row
		if isTargetRow && isSourceRow {
			result.DeleteData = targetData
			return result, nil
		}

		// A - Source: empty, Target: filled
		// B - Source: row, Target: columnar
		if isTargetRow && isSourceColumnar {
			result.DeleteData, err = transposer.ConvertToRowFormat(targetSpec, targetMapOfArrays)
			if err != nil {
				return nil, fmt.Errorf("target data error: " + err.Error())
			}
			return result, nil
		}

		panic("should not have reached here")
	}

	log.Println("computing partition map for source data")
	sourceRowKeyHashes, err := ComputePartitionMap(sourceSpec, sourceMapOfArrays)
	if err != nil {
		return nil, err
	}

	log.Println("computing partition map for target data")
	targetRowKeyHashes, err := ComputePartitionMap(targetSpec, targetMapOfArrays)
	if err != nil {
		return nil, err
	}

	log.Println("merging partion maps key hashes")
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
	log.Println("processing partitions")
	for _, currentHash := range mergedRowKeyHashes {
		request.RowKeyHash = currentHash
		request.SourceIndices = sourceRowKeyHashes[currentHash]
		request.TargetIndices = targetRowKeyHashes[currentHash]

		log.Println("Processing partition " + fmt.Sprint(request.RowKeyHash))
		log.Println("Source indices: " + fmt.Sprint(request.SourceIndices))
		log.Println("Target indices: " + fmt.Sprint(request.TargetIndices))
		ProcessPartition(&request, &response)
		log.Println("Insert indices: " + fmt.Sprint(response.InsertIndices))
		log.Println("Update indices: " + fmt.Sprint(response.UpdateIndices))
		log.Println("Delete indices: " + fmt.Sprint(response.DeleteIndices))
		log.Println("Equalized indices: " + fmt.Sprint(response.EqualizedIndices))

		// copy the results
		insertIndices = append(insertIndices, response.InsertIndices...)
		updateIndices = append(updateIndices, response.UpdateIndices...)
		deleteIndices = append(deleteIndices, response.DeleteIndices...)
		equalizedIndices = append(equalizedIndices, response.EqualizedIndices...)
	}

	// append the results to the response
	result.InsertData = CopyData(sourceMapOfArrays, insertIndices)
	result.UpdateData = CopyData(sourceMapOfArrays, updateIndices)
	result.DeleteData = CopyData(targetMapOfArrays, deleteIndices)
	result.EqualizedData = CopyData(sourceMapOfArrays, equalizedIndices)

	return result, nil
}
