package rendezvous

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Repository interface {
	write(rendezvous *Rendezvous) error
	read(name string) (*Rendezvous, error)
	Exists(name string) (bool, error)
	Create(rendezvous *Rendezvous) error
	Update(rendezvous *Rendezvous) error
	Get(name string) (*Rendezvous, error)
	Delete(name string) error
	List() ([]*Rendezvous, error)
	PostData(name, dataname string, data []byte) error
	GetData(name, dataname string) ([]byte, error)
	DeleteData(name, dataname string) error
}

type FileRepository struct {
	directory string
}

type InMemoryRepository struct {
	rendezvous map[string][]byte
	data       map[string]map[string][]byte
}

// constructor
func NewFileRepository(directory string) (*FileRepository, error) {
	// does the directory exist?
	stat, err := os.Stat(directory)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("cannot create a directory based rendezvous repository because the directory '%s' does not exist", directory)
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("cannot create a directory based rendezvous repository because the path '%s' is not a directory", directory)
	}
	result := &FileRepository{directory: directory}
	return result, nil
}

func NewInMemoryRepository() (*InMemoryRepository, error) {
	result := &InMemoryRepository{
		rendezvous: make(map[string][]byte),
		data:       make(map[string]map[string][]byte),
	}
	return result, nil
}

// write(rendezvous *Rendezvous) error
func (r *FileRepository) write(rendezvous *Rendezvous) error {
	err := ValidateRendezvousName(rendezvous.Name)
	if err != nil {
		return fmt.Errorf("cannot write rendezvous: %w", err)
	}
	rendezvousDir := filepath.Join(r.directory, rendezvous.Name)
	err = os.Mkdir(rendezvousDir, 0755)
	if err != nil {
		return fmt.Errorf("cannot write rendezvous: %w", err)
	}
	rendezvousPath := filepath.Join(rendezvousDir, "rendezvous.json")
	jsonBytes, err := json.Marshal(rendezvous)
	if err != nil {
		return fmt.Errorf("cannot marshal rendezvous to json: %w", err)
	}
	err = os.WriteFile(rendezvousPath, jsonBytes, 0644)
	if err != nil {
		return fmt.Errorf("cannot write rendezvous to file: %w", err)
	}
	return nil
}

func (r *InMemoryRepository) write(rendezvous *Rendezvous) error {
	err := ValidateRendezvousName(rendezvous.Name)
	if err != nil {
		return fmt.Errorf("cannot write rendezvous: %w", err)
	}
	jsonBytes, err := json.Marshal(rendezvous)
	if err != nil {
		return fmt.Errorf("cannot marshal rendezvous to json: %w", err)
	}
	r.rendezvous[rendezvous.Name] = jsonBytes
	return nil
}

// read(name string) (*Rendezvous, error)
func (r *FileRepository) read(name string) (*Rendezvous, error) {
	err := ValidateRendezvousName(name)
	if err != nil {
		return nil, fmt.Errorf("cannot read rendezvous: %w", err)
	}
	rendezvousPath := filepath.Join(r.directory, name, "rendezvous.json")
	jsonBytes, err := os.ReadFile(rendezvousPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read rendezvous from file: %w", err)
	}
	var rendezvous Rendezvous
	err = json.Unmarshal(jsonBytes, &rendezvous)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal rendezvous from json: %w", err)
	}
	return &rendezvous, nil
}

func (r *InMemoryRepository) read(name string) (*Rendezvous, error) {
	err := ValidateRendezvousName(name)
	if err != nil {
		return nil, fmt.Errorf("cannot read rendezvous: %w", err)
	}
	jsonBytes, ok := r.rendezvous[name]
	if !ok {
		return nil, fmt.Errorf("rendezvous named '%s' does not exist", name)
	}
	var rendezvous Rendezvous
	err = json.Unmarshal(jsonBytes, &rendezvous)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal rendezvous from json: %w", err)
	}
	return &rendezvous, nil
}

// Exists(name string) (bool, error)

func (r *FileRepository) Exists(name string) (bool, error) {
	err := ValidateRendezvousName(name)
	if err != nil {
		return false, fmt.Errorf("cannot check if rendezvous exists: %w", err)
	}
	stat, err := os.Stat(filepath.Join(r.directory, name))
	if os.IsNotExist(err) {
		return false, nil
	}
	if stat.IsDir() {
		return true, nil
	}
	return false, fmt.Errorf("internal error: rendezvous named '%s' exists but is not a directory", name)
}

func (r *InMemoryRepository) Exists(name string) (bool, error) {
	err := ValidateRendezvousName(name)
	if err != nil {
		return false, fmt.Errorf("cannot check if rendezvous exists: %w", err)
	}
	_, ok := r.rendezvous[name]
	return ok, nil
}

// Create(rendezvous *Rendezvous) error
func (r *FileRepository) Create(rendezvous *Rendezvous) error {
	// does the rendezvous already exist?
	exists, err := r.Exists(rendezvous.Name)
	if err != nil {
		return fmt.Errorf("cannot create rendezvous: %w", err)
	}
	if exists {
		return fmt.Errorf("rendezvous named '%s' already exists", rendezvous.Name)
	}
	err = r.write(rendezvous)
	if err != nil {
		return fmt.Errorf("cannot create rendezvous: %w", err)
	}
	return nil
}

func (r *InMemoryRepository) Create(rendezvous *Rendezvous) error {
	// does the rendezvous already exist?
	exists, err := r.Exists(rendezvous.Name)
	if err != nil {
		return fmt.Errorf("cannot create rendezvous: %w", err)
	}
	if exists {
		return fmt.Errorf("cannot create rendezvous: rendezvous named '%s' already exists", rendezvous.Name)
	}
	err = r.write(rendezvous)
	if err != nil {
		return fmt.Errorf("cannot create rendezvous: %w", err)
	}
	return nil
}

// Update(rendezvous *Rendezvous) error

func (r *FileRepository) Update(rendezvous *Rendezvous) error {
	// does the rendezvous exist?
	exists, err := r.Exists(rendezvous.Name)
	if err != nil {
		return fmt.Errorf("cannot update rendezvous: %w", err)
	}
	if !exists {
		return fmt.Errorf("cannot update rendezvous: rendezvous named '%s' does not exist", rendezvous.Name)
	}
	err = r.write(rendezvous)
	if err != nil {
		return fmt.Errorf("cannot update rendezvous: %w", err)
	}
	return nil
}

func (r *InMemoryRepository) Update(rendezvous *Rendezvous) error {
	// does the rendezvous exist?
	exists, err := r.Exists(rendezvous.Name)
	if err != nil {
		return fmt.Errorf("cannot update rendezvous: %w", err)
	}
	if !exists {
		return fmt.Errorf("cannot update rendezvous: rendezvous named '%s' does not exist", rendezvous.Name)
	}
	err = r.write(rendezvous)
	if err != nil {
		return fmt.Errorf("cannot update rendezvous: %w", err)
	}
	return nil
}

// Get(name string) (*Rendezvous, error)

func (r *FileRepository) Get(name string) (*Rendezvous, error) {
	// does the rendezvous exist?
	exists, err := r.Exists(name)
	if err != nil {
		return nil, fmt.Errorf("cannot get rendezvous: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("cannot get rendezvous: rendezvous named '%s' does not exist", name)
	}
	rendezvous, err := r.read(name)
	if err != nil {
		return nil, fmt.Errorf("cannot get rendezvous: %w", err)
	}
	return rendezvous, nil
}

func (r *InMemoryRepository) Get(name string) (*Rendezvous, error) {
	// does the rendezvous exist?
	exists, err := r.Exists(name)
	if err != nil {
		return nil, fmt.Errorf("cannot get rendezvous: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("cannot get rendezvous: rendezvous named '%s' does not exist", name)
	}
	rendezvous, err := r.read(name)
	if err != nil {
		return nil, fmt.Errorf("cannot get rendezvous: %w", err)
	}
	return rendezvous, nil
}

// Delete(name string) error

func (r *FileRepository) Delete(name string) error {
	// does the rendezvous exist?
	exists, err := r.Exists(name)
	if err != nil {
		return fmt.Errorf("cannot delete rendezvous: %w", err)
	}
	if !exists {
		return fmt.Errorf("cannot delete rendezvous: rendezvous named '%s' does not exist", name)
	}
	err = os.Remove(filepath.Join(r.directory, name))
	if err != nil {
		return fmt.Errorf("cannot delete rendezvous: %w", err)
	}
	return nil
}

func (r *InMemoryRepository) Delete(name string) error {
	// does the rendezvous exist?
	exists, err := r.Exists(name)
	if err != nil {
		return fmt.Errorf("cannot delete rendezvous: %w", err)
	}
	if !exists {
		return fmt.Errorf("cannot delete rendezvous: rendezvous named '%s' does not exist", name)
	}
	delete(r.rendezvous, name)
	delete(r.data, name)
	return nil
}

// List() ([]*Rendezvous, error)

func (r *FileRepository) List() ([]*Rendezvous, error) {
	entries, err := os.ReadDir(r.directory)
	if err != nil {
		return nil, fmt.Errorf("cannot list rendezvous: %w", err)
	}
	var result []*Rendezvous
	for _, file := range entries {
		rendezvous, err := r.read(file.Name())
		if err != nil {
			return nil, fmt.Errorf("cannot list rendezvous: %w", err)
		}
		result = append(result, rendezvous)
	}
	return result, nil
}

func (r *InMemoryRepository) List() ([]*Rendezvous, error) {
	var result []*Rendezvous
	for _, jsonBytes := range r.rendezvous {
		var rendezvous Rendezvous
		err := json.Unmarshal(jsonBytes, &rendezvous)
		if err != nil {
			return nil, fmt.Errorf("cannot list rendezvous: %w", err)
		}
		result = append(result, &rendezvous)
	}
	return result, nil
}

// PostData(name, dataname string, data []byte) error
func (r *FileRepository) PostData(name, dataname string, data []byte) error {
	// does the rendezvous exist?
	exists, err := r.Exists(name)
	if err != nil {
		return fmt.Errorf("cannot post data: %w", err)
	}
	if !exists {
		return fmt.Errorf("cannot post data: rendezvous named '%s' does not exist", name)
	}
	dataPath := filepath.Join(r.directory, name, dataname)
	err = os.WriteFile(dataPath, data, 0644)
	if err != nil {
		return fmt.Errorf("cannot post data: %w", err)
	}
	return nil
}

func (r *InMemoryRepository) PostData(name, dataname string, data []byte) error {
	// does the rendezvous exist?
	exists, err := r.Exists(name)
	if err != nil {
		return fmt.Errorf("cannot post data: %w", err)
	}
	if !exists {
		return fmt.Errorf("cannot post data: rendezvous named '%s' does not exist", name)
	}
	if _, ok := r.data[name]; !ok {
		r.data[name] = make(map[string][]byte)
	}
	r.data[name][dataname] = data
	return nil
}

// GetData(name, dataname string) ([]byte, error)
func (r *FileRepository) GetData(name, dataname string) ([]byte, error) {
	// does the rendezvous exist?
	exists, err := r.Exists(name)
	if err != nil {
		return nil, fmt.Errorf("cannot get data: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("cannot get data: rendezvous named '%s' does not exist", name)
	}
	dataPath := filepath.Join(r.directory, name, dataname)
	// does the data exist?
	_, err = os.Stat(dataPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	data, err := os.ReadFile(dataPath)
	if err != nil {
		return nil, fmt.Errorf("cannot get data: %w", err)
	}
	return data, nil
}

func (r *InMemoryRepository) GetData(name, dataname string) ([]byte, error) {
	// does the rendezvous exist?
	exists, err := r.Exists(name)
	if err != nil {
		return nil, fmt.Errorf("cannot get data: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("cannot get data: rendezvous named '%s' does not exist", name)
	}
	data, ok := r.data[name][dataname]
	if !ok {
		return nil, nil
	}
	return data, nil
}

// DeleteData(name, dataname string) error
func (r *FileRepository) DeleteData(name, dataname string) error {
	// does the rendezvous exist?
	exists, err := r.Exists(name)
	if err != nil {
		return fmt.Errorf("cannot delete data: %w", err)
	}
	if !exists {
		return fmt.Errorf("cannot delete data: rendezvous named '%s' does not exist", name)
	}
	dataPath := filepath.Join(r.directory, name, dataname)
	// does the data exist?
	_, err = os.Stat(dataPath)
	if os.IsNotExist(err) {
		return nil
	}
	err = os.Remove(dataPath)
	if err != nil {
		return fmt.Errorf("cannot delete data: %w", err)
	}
	return nil
}

func (r *InMemoryRepository) DeleteData(name, dataname string) error {
	// does the rendezvous exist?
	exists, err := r.Exists(name)
	if err != nil {
		return fmt.Errorf("cannot delete data: %w", err)
	}
	if !exists {
		return fmt.Errorf("cannot delete data: rendezvous named '%s' does not exist", name)
	}
	_, ok := r.data[name][dataname]
	if !ok {
		return nil
	}
	delete(r.data[name], dataname)
	return nil
}
