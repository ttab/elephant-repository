package constraints

import (
	"embed"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"

	"github.com/ttab/elephant/revisor"
)

// TODO: The functionality in this file should be cut. Leaving fatals in for now.

//go:embed *.json
var BuiltInConstraints embed.FS

func Core() revisor.ConstraintSet {
	var spec revisor.ConstraintSet

	f, err := BuiltInConstraints.Open("core.json")
	if err != nil {
		log.Fatalf("failed to open core contstraint file: %v", err)
	}

	defer f.Close()

	dec := json.NewDecoder(f)

	dec.DisallowUnknownFields()

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
