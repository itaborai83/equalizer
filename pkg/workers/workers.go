package process

/*
import (
	"github.com/itaborai83/equalizer/internal/utils"
	"github.com/itaborai83/equalizer/pkg/dirlock"
)

// this packages provides functions to manage go routines running as background processes that can be started, stopped and polled

type Service interface {
	Start(name string, function func())
	Stop(name string) error
	Poll(name string) (bool, error)
}

type service struct {
	directory string
	locks map[string]dirlock.DirLock
}


func NewService(directory string) (Service, error) {
	// does the directory exist?
	err := utils.DoesDirectoryExist(directory)
	if err != nil {
		return nil, "cannot create worker service: %w", err
	result := &service{directory: directory, locks: make(map[string]dirlock.DirLock)}
	return result, nil
}

func (s *service) Start(name string, function func()) {
	// acquire the dirlock

}

func (s *service) Stop(name string) error {
	// stop the go routine
	return nil
}

func (s *service) Poll(name string) (bool, error) {
	// check if the go routine is running
	return false, nil
}
*/
