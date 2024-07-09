package equalizer

import (
	"fmt"

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

	// does target data needs to be transposed to column format
	targetInRowFormat := transpose.IsInRowFormat(targetData)
	if targetInRowFormat {
		targetData, err = transpose.RowsToColumns(targetData)
		if err != nil {
			result.Error = err
			return result
		}
	}

	// compute the source key hashes

	// compute the target key hashes

	return nil
}
