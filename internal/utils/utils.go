package utils

import (
	"encoding/json"
	"os"
	"path/filepath"
)

func DoesDirectoryExist(dir string) bool {
	_, err := os.Stat(dir)
	return !os.IsNotExist(err)
}

func DoesFileExist(dir, fileName string) bool {
	path := filepath.Join(dir, fileName)
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func AssertCreateDirectory(dir string) error {
	if DoesDirectoryExist(dir) {
		return nil
	}
	return os.Mkdir(dir, os.ModePerm)
}

func ReadUntypedJsonFile(filePath string) (interface{}, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data interface{}
	err = json.NewDecoder(file).Decode(&data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func WriteUntypedJsonFile(filePath string, data interface{}) error {
	// write to a temporary file first
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	// write to the temporary file
	err = json.NewEncoder(file).Encode(data)
	if err != nil {
		return err
	}
	return nil
}
