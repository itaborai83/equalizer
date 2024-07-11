package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/itaborai83/equalizer/internal/utils"
	"github.com/itaborai83/equalizer/pkg/equalizer"
	"github.com/itaborai83/equalizer/pkg/specs"
)

const (
	ProcessedDataDir  = "processed_data"
	ErrorDataDir      = "error_data"
	InsertDataFile    = "insert_data.json"
	UpdateDataFile    = "update_data.json"
	DeleteDataFile    = "delete_data.json"
	EqualizedDataFile = "equalized_data.json"
)

type Params struct {
	WorkDir        string
	SourceSpecFile string
	TargetSpecFile string
	SourceDataFile string
	TargetDataFile string
}

func ParseParams() (Params, error) {
	params := Params{}
	flag.StringVar(&params.WorkDir, "work-dir", "", "working directory")
	flag.StringVar(&params.SourceSpecFile, "source-spec", "", "source spec file")
	flag.StringVar(&params.TargetSpecFile, "target-spec", "", "target spec file")
	flag.StringVar(&params.SourceDataFile, "source-data", "", "source data file")
	flag.StringVar(&params.TargetDataFile, "target-data", "", "target data file")
	flag.Parse()

	if params.WorkDir == "" {
		return params, fmt.Errorf("work-dir is required")
	}
	if !utils.DoesDirectoryExist(params.WorkDir) {
		return params, fmt.Errorf("work-dir does not exist: %s", params.WorkDir)
	}

	if params.SourceSpecFile == "" {
		return params, fmt.Errorf("source-spec-file is required")
	}
	if !utils.DoesFileExist(params.WorkDir, params.SourceSpecFile) {
		return params, fmt.Errorf("source-spec-file does not exist: %s", params.SourceSpecFile)
	}

	if params.TargetSpecFile == "" {
		return params, fmt.Errorf("target-spec-file is required")
	}
	if !utils.DoesFileExist(params.WorkDir, params.TargetSpecFile) {
		return params, fmt.Errorf("target-spec-file does not exist: %s", params.TargetSpecFile)
	}

	if params.SourceDataFile == "" {
		return params, fmt.Errorf("source-data-file is required")
	}
	if !utils.DoesFileExist(params.WorkDir, params.SourceDataFile) {
		return params, fmt.Errorf("source-data-file does not exist: %s", params.SourceDataFile)
	}

	if params.TargetDataFile == "" {
		return params, fmt.Errorf("target-data-file is required")
	}
	if !utils.DoesFileExist(params.WorkDir, params.TargetDataFile) {
		return params, fmt.Errorf("target-data-file does not exist: %s", params.TargetDataFile)
	}

	return params, nil
}

func createDirs(p Params) {
	processedDir := filepath.Join(p.WorkDir, ProcessedDataDir)
	errorDir := filepath.Join(p.WorkDir, ErrorDataDir)
	utils.AssertCreateDirectory(processedDir)
	utils.AssertCreateDirectory(errorDir)
}

func readSpecFile(filePath string) (*specs.TableSpec, error) {
	spec, err := specs.ReadSpecFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading spec file: %v", err)
	}
	return spec, nil
}

func moveAllFilesToDir(sourceDir, targetDir string) error {
	// does the source directory exist?
	if !utils.DoesDirectoryExist(sourceDir) {
		return fmt.Errorf("source directory does not exist: %s", sourceDir)
	}

	// does the target directory exist?
	if !utils.DoesDirectoryExist(targetDir) {
		return fmt.Errorf("target directory does not exist: %s", targetDir)
	}

	// get all files in the source directory
	files, err := ioutil.ReadDir(sourceDir)
	if err != nil {
		return fmt.Errorf("error reading source directory: %v", err)
	}

	// move all files to the target directory
	for _, file := range files {
		sourcePath := filepath.Join(sourceDir, file.Name())
		targetPath := filepath.Join(targetDir, file.Name())
		// is it a file?
		if file.IsDir() {
			continue
		}
		// move the file
		err = os.Rename(sourcePath, targetPath)
		if err != nil {
			return fmt.Errorf("error moving file: %v", err)
		}
	}
	return nil
}

func main() {
	// Parse command line arguments
	params, err := ParseParams()
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	createDirs(params)
	processedDataDir := filepath.Join(params.WorkDir, ProcessedDataDir)
	errorDataDir := filepath.Join(params.WorkDir, ErrorDataDir)
	sourceSpecPath := filepath.Join(params.WorkDir, params.SourceSpecFile)
	targetSpecPath := filepath.Join(params.WorkDir, params.TargetSpecFile)
	sourceDataPath := filepath.Join(params.WorkDir, params.SourceDataFile)
	targetDataPath := filepath.Join(params.WorkDir, params.TargetDataFile)

	equalizedDataFile := filepath.Join(params.WorkDir, ProcessedDataDir, EqualizedDataFile)
	updateDataFile := filepath.Join(params.WorkDir, ProcessedDataDir, UpdateDataFile)
	insertDataFile := filepath.Join(params.WorkDir, ProcessedDataDir, InsertDataFile)
	deleteDataFile := filepath.Join(params.WorkDir, ProcessedDataDir, DeleteDataFile)

	// defer moving all files to error_data if there is an error
	cleanUp := func() {
		err := moveAllFilesToDir(params.WorkDir, errorDataDir)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			os.Exit(1)
		}
	}

	sourceSpec, err := specs.ReadSpecFile(sourceSpecPath)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		cleanUp()
		os.Exit(1)
	}
	targetSpec, err := specs.ReadSpecFile(targetSpecPath)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		cleanUp()
		os.Exit(1)
	}

	sourceData, err := utils.ReadUntypedJsonFile(sourceDataPath)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		cleanUp()
		os.Exit(1)
	}

	targetData, err := utils.ReadUntypedJsonFile(targetDataPath)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		cleanUp()
		os.Exit(1)
	}

	result, err := equalizer.Run(sourceSpec, targetSpec, sourceData, targetData)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		cleanUp()
		os.Exit(1)
	}

	err = utils.WriteUntypedJsonFile(insertDataFile, result.InsertData)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		cleanUp()
		os.Exit(1)
	}

	err = utils.WriteUntypedJsonFile(updateDataFile, result.UpdateData)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		cleanUp()
		os.Exit(1)
	}

	err = utils.WriteUntypedJsonFile(deleteDataFile, result.DeleteData)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		cleanUp()
		os.Exit(1)
	}

	err = utils.WriteUntypedJsonFile(equalizedDataFile, result.EqualizedData)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		cleanUp()
		os.Exit(1)
	}

	// move all files to processed_data
	err = moveAllFilesToDir(params.WorkDir, processedDataDir)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		cleanUp()
		os.Exit(1)
	}

	fmt.Println("done")
}
