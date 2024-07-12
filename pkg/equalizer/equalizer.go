package equalizer

import (
	"fmt"

	"github.com/itaborai83/equalizer/internal/utils"
	"github.com/itaborai83/equalizer/pkg/hasher"
	"github.com/itaborai83/equalizer/pkg/specs"
	"github.com/itaborai83/equalizer/pkg/transpose"
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
	aMap, ok := data.(map[string]interface{})
	if ok {
		return len(aMap) == 0
	}
	return true
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
	columnNames, err := GetColumnNames(data)
	if err != nil {
		return nil, err
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

	var sourceMapOfArrays map[string][]interface{}
	var targetMapOfArrays map[string][]interface{}

	if !isSourceEmpty {
		log.Println("converting source data to a map of string to array of interfaces")
		sourceMapOfArrays, err = transpose.ConvertToColumnarFormat(sourceData)
		if err != nil {
			return nil, fmt.Errorf("source data is not in column format: " + err.Error())
		}
	}

	if !isTargetEmpty {
		log.Println("converting target data to a map of string to array of interfaces")
		targetMapOfArrays, err = transpose.ConvertToColumnarFormat(targetData)
		if err != nil {
			return nil, fmt.Errorf("target data is not in column format: " + err.Error())
		}
	}

	/*
		if !sourceSpec.ConformsTo(sourceMapOfArrays) {
			msg = "source data does not conform to the source spec"
			log.Print(msg)
			err = fmt.Errorf(msg)
			return nil, err
		}
		if !targetSpec.ConformsTo(targetMapOfArrays) {
			msg = "target data does not conform to the target spec"
			log.Print(msg)
			err = fmt.Errorf(msg)
			return nil, err
		}
	*/

	if isSourceEmpty {
		log.Println("source data is empty. returning target data as delete data")
		result.EqualizedData = sourceSpec.NewEmptyData()
		result.UpdateData = sourceSpec.NewEmptyData()
		result.InsertData = sourceSpec.NewEmptyData()
		result.DeleteData = targetMapOfArrays
		return result, nil
	}

	if isTargetEmpty {
		log.Println("target data is empty. returning source data as insert data")
		result.EqualizedData = sourceSpec.NewEmptyData()
		result.UpdateData = sourceSpec.NewEmptyData()
		result.InsertData = sourceMapOfArrays
		result.DeleteData = targetSpec.NewEmptyData()
		return result, nil
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
