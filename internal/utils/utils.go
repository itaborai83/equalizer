package utils

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
)

const (
	BOM = "\xef\xbb\xbf"
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
	// read all body
	body, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	// trim the BOM mark
	body = bytes.TrimPrefix(body, []byte(BOM))

	var data interface{}
	err = json.Unmarshal(body, &data)
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

func NewLogger(name string) *log.Logger {
	// log to stdout
	return log.New(os.Stdout, name+": ", log.LstdFlags)
}

func RecursiveUntypedEquals(a, b interface{}) bool {
	// this is a hack ... the values are equal if their json representation are equal
	// this is not a perfect solution but it works for now
	aAsJson, err := json.Marshal(a)
	if err != nil {
		return false
	}
	bAsJson, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return string(aAsJson) == string(bAsJson)
}
