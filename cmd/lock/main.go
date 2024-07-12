package main

import (
	"flag"
	"fmt"

	"github.com/itaborai83/equalizer/internal/utils"
	"github.com/itaborai83/equalizer/pkg/dirlock"
)

const (
	DEFAULT_WAIT_VAR      = false
	DEFAULT_MAX_WAIT_TIME = 60
)

type Params struct {
	Dir         string
	Lock        string
	Wait        bool
	MaxWaitTime int
	Unlock      bool
}

var (
	log    = utils.NewLogger("lock")
	params *Params
)

func parseArgs() (*Params, error) {
	params := &Params{}
	flag.StringVar(&params.Dir, "dir", "", "directory to lock")
	flag.StringVar(&params.Lock, "lock", "", "lock file name")
	flag.BoolVar(&params.Wait, "wait", false, "wait for lock")
	flag.IntVar(&params.MaxWaitTime, "timeout", DEFAULT_MAX_WAIT_TIME, "max locking wait time in seconds")
	flag.BoolVar(&params.Unlock, "unlock", false, "unlock the flag")
	flag.Parse()

	if params.Dir == "" {
		return nil, fmt.Errorf("dir is required")
	}

	if params.Lock == "" {
		return nil, fmt.Errorf("lock is required")
	}

	if params.Wait && params.Unlock {
		return nil, fmt.Errorf("wait and unlock are mutually exclusive")
	}

	return params, nil
}

func main() {
	var err error
	params, err = parseArgs()

	if err != nil {
		log.Fatalf("error parsing args: %s", err.Error())
	}
	dl, err := dirlock.New(params.Dir, params.Lock)
	if err != nil {
		log.Fatalf("error creating dirlock: %s", err.Error())
	}
	if params.Unlock {
		bool, err := dl.Unlock()
		if err != nil {
			log.Fatalf("error unlocking: %s", err.Error())
		}
		if !bool {
			log.Fatalf("unable to unlock: lock not found")
		} else {
			log.Println("lock released")
		}
		return

	} else {
		if params.Wait {
			err := dl.WaitLock(params.MaxWaitTime)
			if err != nil {
				log.Fatalf("error waiting for lock: %s", err.Error())
			}
		} else {
			locked, err := dl.TryLock()
			if err != nil {
				log.Fatalf("error trying to lock: %s", err.Error())
			}
			if !locked {
				log.Fatalf("unable to unlock: lock already exists")
			}
		}
		log.Println("lock acquired")
	}
}
