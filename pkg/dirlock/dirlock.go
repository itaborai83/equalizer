package dirlock

import (
	"fmt"
	"os"
	"time"
)

type DirLock struct {
	basePath string
	lockName string // lock is a directory ... it will always append the suffix ".lock" to it
}

const (
	lockSuffix = ".lock"
)

func New(basePath, lockName string) (*DirLock, error) {
	// does the base path exist?
	_, err := os.Stat(basePath)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("base path does not exist: %s", basePath)
	}
	if err != nil {
		return nil, err
	}
	// strip trailing slashes
	for len(basePath) > 0 && (basePath[len(basePath)-1] == '/' || basePath[len(basePath)-1] == '\\') {
		basePath = basePath[:len(basePath)-1]
	}
	result := &DirLock{basePath: basePath, lockName: lockName}
	return result, nil
}

func (d *DirLock) lockPath() string {
	return fmt.Sprintf("%s/%s%s", d.basePath, d.lockName, lockSuffix)
}

func (d *DirLock) TryLock() (bool, error) {
	lockPath := d.lockPath()
	err := os.Mkdir(lockPath, 0755)
	if os.IsExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("could not create lock entry: %s", err.Error())
	}
	return true, nil
}

func (d *DirLock) IsLocked() (bool, error) {
	lockPath := d.lockPath()
	_, err := os.Stat(lockPath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("could not stat lock entry: %s", err.Error())
	}
	return true, nil
}

func (d *DirLock) WaitLock(timeout int) error {
	if timeout <= 0 {
		return fmt.Errorf("max wait time must be greater than 0: %d", timeout)
	}
	startTime := time.Now()
	sleepTime := 1 * time.Second
	maxDuration := time.Duration(timeout) * time.Second
	for {
		locked, err := d.TryLock()
		if err != nil {
			return err
		}
		if locked {
			return nil
		}
		// have we waited too long?
		elapsedTime := time.Since(startTime)
		if elapsedTime > maxDuration {
			return fmt.Errorf("waited too long for lock: %s", d.lockPath())
		}
		// go to sleep
		time.Sleep(sleepTime)
		// exponential backoff
		sleepTime = sleepTime * 2
		// try again
	}
}

func (d *DirLock) Unlock() (bool, error) {
	lockPath := d.lockPath()
	err := os.Remove(lockPath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("could not remove lock entry: %s", err.Error())
	}
	return true, nil
}
