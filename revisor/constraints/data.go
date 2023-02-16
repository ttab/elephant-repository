package constraints

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"

	"github.com/ttab/elephant/revisor"
)

// TODO: The functionality in this file should be cut. Leaving fatals in for now.

//go:embed *.json
var BuiltInConstraints embed.FS

func Core() revisor.ConstraintSet {
	data, err := fs.ReadFile(BuiltInConstraints, "core.json")
	if err != nil {
		log.Fatalf("failed to read core constraints: %v", err)
	}

	dec := json.NewDecoder(bytes.NewReader(data))

	dec.DisallowUnknownFields()

	var spec revisor.ConstraintSet

	err = dec.Decode(&spec)
	if err != nil {
		log.Fatalf("failed to unmarshal core constraints: %v", err)
	}

	return spec
}

func DefaultValidator() (*revisor.Validator, error) {
	return createValidator(
		"tt.json",
	)
}

func createValidator(paths ...string) (*revisor.Validator, error) {
	constraints := []revisor.ConstraintSet{
		Core(),
	}

	for _, name := range paths {
		data, err := BuiltInConstraints.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to read constraints file: %w", err)
		}

		var c revisor.ConstraintSet

		err = json.Unmarshal(data, &c)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to unmarshal %q constraints: %w",
				name, err)
		}

		constraints = append(constraints, c)
	}

	v, err := revisor.NewValidator(constraints...)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create validator with constraints: %w", err)
	}

	return v, nil
}
