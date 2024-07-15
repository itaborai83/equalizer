package rendezvous

import "fmt"

const (
	pidfile = "equalizer.pid"
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
		return "", fmt.Errorf("cannot retrieve auth token: %w", err)
	}
	rendezvouz, err := s.repository.Get(rendezvouzName)
	if err != nil {
		return "", fmt.Errorf("cannot retrieve auth token: %w", err)
	}
	return rendezvouz.AuthToken, nil
}

func (s *service) CheckAuthToken(token, rendezvouzName string) (bool, error) {
	err := ValidateRendezvousName(rendezvouzName)
	if err != nil {
		return false, fmt.Errorf("cannot check auth token: %w", err)
	}
	err = ValidateAuthToken(token)
	if err != nil {
		return false, fmt.Errorf("cannot check auth token: %w", err)
	}
	actualToken, err := s.GetAuthToken(rendezvouzName)
	if err != nil {
		return false, fmt.Errorf("cannot check auth token: %w", err)
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
