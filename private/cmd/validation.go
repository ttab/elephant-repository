package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/navigacontentlab/revisor"
	"github.com/ttab/docformat"
)

func DefaultValidator() (*revisor.Validator, error) {
	return createValidator(
		"constraints/core.json",
		"constraints/tt.json",
	)
}

func createValidator(paths ...string) (*revisor.Validator, error) {
	var constraints []revisor.ConstraintSet

	for _, name := range paths {
		data, err := docformat.BuiltInConstraints.ReadFile(name)
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
