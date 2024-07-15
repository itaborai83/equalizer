package rendezvous

import (
	"encoding/json"
	"fmt"

	"github.com/itaborai83/equalizer/internal/utils"
	"github.com/itaborai83/equalizer/pkg/equalizer"
)

const (
	pidfile = "equalizer.pid"
)

var (
	serviceLog = utils.NewLogger("rendezvous")
)

type Service interface {
	GetAuthToken(rendezvouzName string) (string, error)
	CheckAuthToken(token, rendezvouzName string) (bool, error)
	GetStatus(rendezvouzName string) string
	Create(name string, rendezvouz *RendezvousRequest) *ApiResponse
	Get(name string) *ApiResponse
	Update(name string, rendezvouz *RendezvousRequest) *ApiResponse
	Delete(name string) *ApiResponse
	List() *ApiResponse
	PostSourceData(name string, data []byte) *ApiResponse
	PostTargetData(name string, data []byte) *ApiResponse
	GetSourceData(name string) *ApiResponse
	GetTargetData(name string) *ApiResponse
	DeleteSourceData(name string) *ApiResponse
	DeleteTargetData(name string) *ApiResponse
	Equalize(name string) *ApiResponse
	GetInsertData(name string) *ApiResponse
	GetDeleteData(name string) *ApiResponse
	GetUpdateData(name string) *ApiResponse
	GetEqualizedData(name string) *ApiResponse
}

type service struct {
	repository Repository
}

func NewService(repository Repository) (Service, error) {
	return &service{repository: repository}, nil
}

func (s *service) GetAuthToken(rendezvouzName string) (string, error) {
	err := ValidateRendezvousName(rendezvouzName)
	if err != nil {
		msg := fmt.Sprintf("cannot retrieve auth token: %s", err)
		serviceLog.Println(msg)
		return "", fmt.Errorf(msg)
	}
	rendezvouz, err := s.repository.Get(rendezvouzName)
	if err != nil {
		msg := fmt.Sprintf("cannot retrieve auth token: %s", err)
		serviceLog.Println(msg)
		return "", fmt.Errorf(msg)
	}
	return rendezvouz.AuthToken, nil
}

func (s *service) CheckAuthToken(token, rendezvouzName string) (bool, error) {
	err := ValidateRendezvousName(rendezvouzName)
	if err != nil {
		msg := fmt.Sprintf("cannot check auth token: %s", err)
		serviceLog.Println(msg)
		return false, fmt.Errorf(msg)
	}
	err = ValidateAuthToken(token)
	if err != nil {
		msg := fmt.Sprintf("cannot check auth token: %s", err)
		serviceLog.Println(msg)
		return false, fmt.Errorf(msg)
	}
	actualToken, err := s.GetAuthToken(rendezvouzName)
	if err != nil {
		msg := fmt.Sprintf("cannot check auth token: %s", err)
		serviceLog.Println(msg)
		return false, fmt.Errorf(msg)
	}
	result := token == actualToken
	return result, nil
}

func (s *service) GetStatus(rendezvouzName string) string {
	err := ValidateRendezvousName(rendezvouzName)
	if err != nil {
		return StatusError
	}
	return StatusUnknown
}

func (s *service) Create(name string, rendezvous *RendezvousRequest) *ApiResponse {
	var msg string
	err := ValidateRendezvousCreation(name, rendezvous)
	if err != nil {
		msg = fmt.Sprintf("cannot create rendezvous: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	r := &Rendezvous{
		Name:       name,
		SourceSpec: rendezvous.SourceSpec,
		TargetSpec: rendezvous.TargetSpec,
		AuthToken:  rendezvous.AuthToken,
	}
	err = s.repository.Create(r)
	if err != nil {
		msg = fmt.Sprintf("cannot create rendezvous: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	response := &RendezvousResponse{
		Name:       r.Name,
		Status:     s.GetStatus(name),
		SourceSpec: r.SourceSpec,
		TargetSpec: r.TargetSpec,
	}
	msg = fmt.Sprintf("rendezvous '%s' created", name)
	return NewApiResponse(201, msg, response)
}

func (s *service) Get(name string) *ApiResponse {
	var msg string
	err := ValidateRendezvousName(name)
	if err != nil {
		msg = fmt.Sprintf("cannot retrieve rendezvous: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	// does rendezvous exist?
	exists, err := s.repository.Exists(name)
	if err != nil {
		msg = fmt.Sprintf("cannot retrieve rendezvous: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	if !exists {
		msg = fmt.Sprintf("rendezvous '%s' not found", name)
		return NewApiResponse(404, msg, nil)
	}
	rendezvous, err := s.repository.Get(name)
	if err != nil {
		msg = fmt.Sprintf("cannot retrieve rendezvous: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	response := &RendezvousResponse{
		Name:       rendezvous.Name,
		Status:     s.GetStatus(name),
		SourceSpec: rendezvous.SourceSpec,
		TargetSpec: rendezvous.TargetSpec,
	}
	msg = fmt.Sprintf("rendezvous '%s' retrieved", name)
	return NewApiResponse(200, msg, response)
}

func (s *service) Update(name string, rendezvous *RendezvousRequest) *ApiResponse {
	var msg string
	err := ValidateRendezvousUpdate(name, rendezvous)
	if err != nil {
		msg = fmt.Sprintf("cannot update rendezvous: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	r := &Rendezvous{
		Name:       name,
		SourceSpec: rendezvous.SourceSpec,
		TargetSpec: rendezvous.TargetSpec,
		AuthToken:  rendezvous.AuthToken,
	}
	err = s.repository.Update(r)
	if err != nil {
		msg = fmt.Sprintf("cannot update rendezvous: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	response := &RendezvousResponse{
		Name:       r.Name,
		Status:     s.GetStatus(name),
		SourceSpec: r.SourceSpec,
		TargetSpec: r.TargetSpec,
	}
	msg = fmt.Sprintf("rendezvous '%s' updated", name)
	return NewApiResponse(200, msg, response)
}

func (s *service) Delete(name string) *ApiResponse {
	var msg string
	err := ValidateRendezvousDeletion(name)
	if err != nil {
		msg = fmt.Sprintf("cannot delete rendezvous: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	err = s.repository.Delete(name)
	if err != nil {
		msg = fmt.Sprintf("cannot delete rendezvous: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	msg = fmt.Sprintf("rendezvous '%s' deleted", name)
	return NewApiResponse(200, msg, nil)
}

func (s *service) List() *ApiResponse {
	rendezvousList, err := s.repository.List()
	if err != nil {
		msg := fmt.Sprintf("cannot list rendezvous: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	result := make([]*RendezvousResponse, len(rendezvousList))
	for i, r := range rendezvousList {
		result[i] = &RendezvousResponse{
			Name:       r.Name,
			Status:     s.GetStatus(r.Name),
			SourceSpec: r.SourceSpec,
			TargetSpec: r.TargetSpec,
		}
	}
	msg := "rendezvous list retrieved"
	return NewApiResponse(200, msg, rendezvousList)
}

func (s *service) PostSourceData(name string, data []byte) *ApiResponse {
	err := ValidatePostSourceData(name, data)
	if err != nil {
		msg := fmt.Sprintf("cannot post source data: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	err = s.repository.PostData(name, sourceDataFile, data)
	if err != nil {
		msg := fmt.Sprintf("cannot post source data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	msg := fmt.Sprintf("source data posted to rendezvous '%s'", name)
	return NewApiResponse(201, msg, nil)
}

func (s *service) PostTargetData(name string, data []byte) *ApiResponse {
	err := ValidatePostTargetData(name, data)
	if err != nil {
		msg := fmt.Sprintf("cannot post target data: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	err = s.repository.PostData(name, targetDataFile, data)
	if err != nil {
		msg := fmt.Sprintf("cannot post target data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	msg := fmt.Sprintf("target data posted to rendezvous '%s'", name)
	return NewApiResponse(201, msg, nil)
}

func (s *service) GetSourceData(name string) *ApiResponse {
	err := ValidateGetSourceData(name)
	if err != nil {
		msg := fmt.Sprintf("cannot get source data: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	data, err := s.repository.GetData(name, sourceDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot get source data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	// convert data to string
	text := string(data)
	msg := fmt.Sprintf("source data retrieved from rendezvous '%s'", name)
	return NewApiResponse(200, msg, text)
}

func (s *service) GetTargetData(name string) *ApiResponse {
	err := ValidateGetTargetData(name)
	if err != nil {
		msg := fmt.Sprintf("cannot get target data: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	data, err := s.repository.GetData(name, targetDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot get target data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	// convert data to string
	text := string(data)
	msg := fmt.Sprintf("target data retrieved from rendezvous '%s'", name)
	return NewApiResponse(200, msg, text)
}

func (s *service) DeleteSourceData(name string) *ApiResponse {
	err := ValidateDeleteSourceData(name)
	if err != nil {
		msg := fmt.Sprintf("cannot delete source data: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	err = s.repository.DeleteData(name, sourceDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot delete source data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	msg := fmt.Sprintf("source data deleted from rendezvous '%s'", name)
	return NewApiResponse(200, msg, nil)
}

func (s *service) DeleteTargetData(name string) *ApiResponse {
	err := ValidateDeleteTargetData(name)
	if err != nil {
		msg := fmt.Sprintf("cannot delete target data: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	err = s.repository.DeleteData(name, targetDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot delete target data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	msg := fmt.Sprintf("target data deleted from rendezvous '%s'", name)
	return NewApiResponse(200, msg, nil)
}

func (s *service) Equalize(name string) *ApiResponse {
	serviceLog.Println("Starting equalize...")
	err := ValidateEqualize(name)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(400, msg)
	}
	exists, err := s.repository.Exists(name)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}
	if !exists {
		msg := fmt.Sprintf("cannot equalize data: rendezvous '%s' not found", name)
		serviceLog.Printf("Error: %s", msg)
		return NewApiResponse(404, msg, nil)
	}

	// get the rendezvous
	rendezvous, err := s.repository.Get(name)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}

	// delete previous results if any exist
	err = s.repository.DeleteData(name, insertDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to delete previous insert data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}
	err = s.repository.DeleteData(name, deleteDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to delete previous delete data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}
	err = s.repository.DeleteData(name, updateDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to delete previous update data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}
	err = s.repository.DeleteData(name, equalizedDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to delete previous equalized data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}

	sourceSpec := rendezvous.SourceSpec
	targetSpec := rendezvous.TargetSpec
	sourceDataBytes, err := s.repository.GetData(name, sourceDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to retrieve source data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}
	var sourceData interface{}
	err = json.Unmarshal(sourceDataBytes, &sourceData)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to unmarshal source data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}

	targetDataBytes, err := s.repository.GetData(name, targetDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to retrieve target data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}
	var targetData interface{}
	err = json.Unmarshal(targetDataBytes, &targetData)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to unmarshal target data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}

	// equalize data
	result, err := equalizer.Run(sourceSpec, targetSpec, sourceData, targetData)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}

	// save insert data
	insertData, err := json.Marshal(result.InsertData)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to marshal insert data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}
	err = s.repository.PostData(name, insertDataFile, insertData)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to save insert data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}

	// save delete data
	deleteData, err := json.Marshal(result.DeleteData)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to marshal delete data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}
	err = s.repository.PostData(name, deleteDataFile, deleteData)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to save delete data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}

	// save update data
	updateData, err := json.Marshal(result.UpdateData)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to marshal update data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}
	err = s.repository.PostData(name, updateDataFile, updateData)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to save update data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}

	// save equalized data
	equalizedData, err := json.Marshal(result.EqualizedData)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to marshal equalized data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}
	err = s.repository.PostData(name, equalizedDataFile, equalizedData)
	if err != nil {
		msg := fmt.Sprintf("cannot equalize data: unable to save equalized data: %s", err)
		serviceLog.Printf("Error: %s", msg)
		return NewErrorApiResponse(500, msg)
	}

	msg := fmt.Sprintf("data equalized for rendezvous '%s'", name)
	serviceLog.Printf("Success: %s", msg)
	return NewApiResponse(200, msg, nil)
}

func (s *service) GetInsertData(name string) *ApiResponse {
	err := ValidateGetInsertData(name)
	if err != nil {
		msg := fmt.Sprintf("cannot get insert data: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	exists, err := s.repository.Exists(name)
	if err != nil {
		msg := fmt.Sprintf("cannot get insert data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	if !exists {
		msg := fmt.Sprintf("rendezvous '%s' not found", name)
		return NewApiResponse(404, msg, nil)
	}
	data, err := s.repository.GetData(name, insertDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot get insert data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	// convert data to string
	text := string(data)
	msg := fmt.Sprintf("insert data retrieved from rendezvous '%s'", name)
	return NewApiResponse(200, msg, text)
}

func (s *service) GetDeleteData(name string) *ApiResponse {
	err := ValidateGetDeleteData(name)
	if err != nil {
		msg := fmt.Sprintf("cannot get delete data: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	exists, err := s.repository.Exists(name)
	if err != nil {
		msg := fmt.Sprintf("cannot get delete data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	if !exists {
		msg := fmt.Sprintf("rendezvous '%s' not found", name)
		return NewApiResponse(404, msg, nil)
	}
	data, err := s.repository.GetData(name, deleteDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot get delete data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	// convert data to string
	text := string(data)
	msg := fmt.Sprintf("delete data retrieved from rendezvous '%s'", name)
	return NewApiResponse(200, msg, text)
}

func (s *service) GetUpdateData(name string) *ApiResponse {
	err := ValidateGetUpdateData(name)
	if err != nil {
		msg := fmt.Sprintf("cannot get update data: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	exists, err := s.repository.Exists(name)
	if err != nil {
		msg := fmt.Sprintf("cannot get update data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	if !exists {
		msg := fmt.Sprintf("rendezvous '%s' not found", name)
		return NewApiResponse(404, msg, nil)
	}
	data, err := s.repository.GetData(name, updateDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot get update data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	// convert data to string
	text := string(data)
	msg := fmt.Sprintf("update data retrieved from rendezvous '%s'", name)
	return NewApiResponse(200, msg, text)
}

func (s *service) GetEqualizedData(name string) *ApiResponse {
	err := ValidateGetEqualizedData(name)
	if err != nil {
		msg := fmt.Sprintf("cannot get equalized data: %s", err)
		return NewErrorApiResponse(400, msg)
	}
	exists, err := s.repository.Exists(name)
	if err != nil {
		msg := fmt.Sprintf("cannot get equalized data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	if !exists {
		msg := fmt.Sprintf("rendezvous '%s' not found", name)
		return NewApiResponse(404, msg, nil)
	}
	data, err := s.repository.GetData(name, equalizedDataFile)
	if err != nil {
		msg := fmt.Sprintf("cannot get equalized data: %s", err)
		return NewErrorApiResponse(500, msg)
	}
	// convert data to string
	text := string(data)
	msg := fmt.Sprintf("equalized data retrieved from rendezvous '%s'", name)
	return NewApiResponse(200, msg, text)
}
