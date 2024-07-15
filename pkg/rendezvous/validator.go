package rendezvous

import (
	"fmt"
	"strings"

	"github.com/itaborai83/equalizer/pkg/specs"
)

func ValidateRendezvousName(name string) error {
	if name == "" {
		return fmt.Errorf("rendezvous name cannot be empty")
	}
	if strings.Contains(name, " ") {
		return fmt.Errorf("rendezvous name cannot contain spaces")
	}
	if strings.Contains(name, "/") || strings.Contains(name, "\\") {
		return fmt.Errorf("rendezvous name cannot contain slashes or backslashes")
	}
	if strings.Contains(name, ".") {
		return fmt.Errorf("rendezvous name cannot contain dots")
	}
	return nil
}

func ValidateAuthToken(token string) error {
	if token == "" {
		return fmt.Errorf("auth token cannot be empty")
	}
	if strings.Contains(token, " ") {
		return fmt.Errorf("auth token cannot contain spaces")
	}
	if strings.Contains(token, "/") || strings.Contains(token, "\\") {
		return fmt.Errorf("auth token cannot contain slashes or backslashes")
	}
	if strings.Contains(token, ".") {
		return fmt.Errorf("auth token cannot contain dots")
	}
	if len(token) < 8 {
		return fmt.Errorf("auth token must be at least 8 characters long")
	}
	return nil
}

func ValidateSpecs(sourceSpec, targetSpec *specs.TableSpec) error {
	if sourceSpec == nil {
		return fmt.Errorf("source spec name cannot be nil")
	}
	if targetSpec == nil {
		return fmt.Errorf("target spec name cannot be nil")
	}
	if sourceSpec.Name == "" {
		return fmt.Errorf("source spec name cannot be empty")
	}
	if targetSpec.Name == "" {
		return fmt.Errorf("target spec name cannot be empty")
	}
	if len(sourceSpec.Columns) == 0 {
		return fmt.Errorf("source spec must have at least one column")
	}
	if len(targetSpec.Columns) == 0 {
		return fmt.Errorf("target spec must have at least one column")
	}
	if len(sourceSpec.KeyColumns) == 0 {
		return fmt.Errorf("source spec must have at least one key column")
	}
	if len(targetSpec.KeyColumns) == 0 {
		return fmt.Errorf("target spec must have at least one key column")
	}
	equalizable, err := sourceSpec.Equalizable(targetSpec)
	if err != nil {
		return fmt.Errorf("error checking if source and target specs are equalizable: %s", err)
	}
	if !equalizable {
		return fmt.Errorf("source and target specs are not equalizable: %s", err)
	}
	return nil
}

func ValidateRendezvousCreation(name string, request *RendezvousRequest) error {
	err := ValidateRendezvousName(name)
	if err != nil {
		return fmt.Errorf("cannot create rendezvous: %w", err)
	}

	err = ValidateAuthToken(request.AuthToken)
	if err != nil {
		return fmt.Errorf("cannot create rendezvous: %w", err)
	}

	err = ValidateSpecs(request.SourceSpec, request.TargetSpec)
	if err != nil {
		return fmt.Errorf("cannot create rendezvous: %w", err)
	}

	return nil
}

func ValidateGetRendezvous(name string) error {
	err := ValidateRendezvousName(name)
	if err != nil {
		return fmt.Errorf("cannot get rendezvous: %w", err)
	}
	return nil
}

func ValidateRendezvousUpdate(name string, request *RendezvousRequest) error {
	err := ValidateRendezvousName(name)
	if err != nil {
		return fmt.Errorf("cannot update rendezvous: %w", err)
	}

	err = ValidateAuthToken(request.AuthToken)
	if err != nil {
		return fmt.Errorf("cannot update rendezvous: %w", err)
	}

	err = ValidateSpecs(request.SourceSpec, request.TargetSpec)
	if err != nil {
		return fmt.Errorf("cannot update rendezvous: %w", err)
	}

	return nil
}

func ValidateRendezvousDeletion(name string) error {
	err := ValidateRendezvousName(name)
	if err != nil {
		return fmt.Errorf("cannot delete rendezvous: %w", err)
	}
	return nil
}

func ValidatePostSourceData(name string, data []byte) error {
	err := ValidateRendezvousName(name)
	if err != nil {
		return fmt.Errorf("cannot post source data: %w", err)
	}
	if len(data) == 0 {
		return fmt.Errorf("cannot post source data: data cannot be empty")
	}
	return nil
}

func ValidatePostTargetData(name string, data []byte) error {
	err := ValidateRendezvousName(name)
	if err != nil {
		return fmt.Errorf("cannot post target data: %w", err)
	}
	if len(data) == 0 {
		return fmt.Errorf("cannot post target data: data cannot be empty")
	}
	return nil
}

func ValidateGetSourceData(name string) error {
	err := ValidateRendezvousName(name)
	if err != nil {
		return fmt.Errorf("cannot get source data: %w", err)
	}
	return nil
}

func ValidateGetTargetData(name string) error {
	err := ValidateRendezvousName(name)
	if err != nil {
		return fmt.Errorf("cannot get target data: %w", err)
	}
	return nil
}

func ValidateDeleteSourceData(name string) error {
	err := ValidateRendezvousName(name)
	if err != nil {
		return fmt.Errorf("cannot delete source data: %w", err)
	}
	return nil
}

func ValidateDeleteTargetData(name string) error {
	err := ValidateRendezvousName(name)
	if err != nil {
		return fmt.Errorf("cannot delete target data: %w", err)
	}
	return nil
}
