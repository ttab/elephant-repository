package revisor_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ttab/elephant/doc"
	"github.com/ttab/elephant/revisor"
	"github.com/ttab/elephant/revisor/internal"
)

func FuzzValidationWide(f *testing.F) {
	baseConstraints, err := os.ReadFile("constraints/naviga.json")
	if err != nil {
		f.Fatalf("failed to read base constraints: %v", err)
	}

	exampleConstraints, err := os.ReadFile("constraints/example.json")
	if err != nil {
		f.Fatalf("failed to read example constraints: %v", err)
	}

	paths, err := filepath.Glob(filepath.Join("testdata", "*.json"))
	if err != nil {
		f.Fatalf("failed to glob for result files: %v", err)
	}

	for i := range paths {
		docData, err := os.ReadFile(paths[i])
		if err != nil {
			f.Fatalf("failed to read document data from %q: %v",
				paths[i], err)
		}

		f.Add(baseConstraints, exampleConstraints, docData)
	}

	f.Fuzz(func(t *testing.T, constraintsA []byte, constraintsB []byte, documentData []byte) {
		var (
			document         doc.Document
			constraints      revisor.ConstraintSet
			extraConstraints revisor.ConstraintSet
		)

		if !(decodeBytes(t, constraintsA, &constraints) &&
			decodeBytes(t, constraintsB, &extraConstraints) &&
			decodeBytes(t, documentData, &document)) {
			return
		}

		validator, err := revisor.NewValidator(constraints, extraConstraints)
		if err != nil {
			return
		}

		_ = validator.ValidateDocument(&document)
	})
}

func FuzzValidationConstraints(f *testing.F) {
	constraintPaths, err := filepath.Glob(filepath.Join("constraints", "*.json"))
	if err != nil {
		f.Fatalf("failed to glob for constraint files: %v", err)
	}

	for i := range constraintPaths {
		data, err := os.ReadFile(constraintPaths[i])
		if err != nil {
			f.Fatalf("failed to read constraints from %q: %v", constraintPaths[i], err)
		}

		f.Add(data)
	}

	paths, err := filepath.Glob(filepath.Join("testdata", "*.json"))
	if err != nil {
		f.Fatalf("failed to glob for result files: %v", err)
	}

	var documents []*doc.Document

	for i := range paths {
		var d doc.Document

		err := internal.UnmarshalFile(paths[i], &d)
		if err != nil {
			f.Fatalf("failed to decode document %s: %v",
				paths[i], err)
		}

		documents = append(documents, &d)
	}

	f.Fuzz(func(t *testing.T, constraintData []byte) {
		var (
			constraints revisor.ConstraintSet
		)

		if !(decodeBytes(t, constraintData, &constraints)) {
			return
		}

		validator, err := revisor.NewValidator(constraints)
		if err != nil {
			return
		}

		for _, document := range documents {
			_ = validator.ValidateDocument(document)
		}
	})
}

func decodeBytes(t *testing.T, data []byte, o interface{}) bool {
	t.Helper()

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	err := dec.Decode(o)

	return err == nil
}

type validatorTest struct {
	Name      string
	Prefix    string
	Validator *revisor.Validator
}

func TestValidateDocument(t *testing.T) {
	var (
		constraints      revisor.ConstraintSet
		extraConstraints revisor.ConstraintSet
	)

	err := internal.UnmarshalFile("constraints/naviga.json", &constraints)
	if err != nil {
		t.Fatalf("failed to load base constraints: %v", err)
	}

	err = internal.UnmarshalFile("constraints/example.json", &extraConstraints)
	if err != nil {
		t.Fatalf("failed to load org constraints: %v", err)
	}

	baseValidator, err := revisor.NewValidator(constraints)
	if err != nil {
		t.Fatalf("failed to create base validator: %v", err)
	}

	orgValidator, err := revisor.NewValidator(constraints, extraConstraints)
	if err != nil {
		t.Fatalf("failed to create org validator: %v", err)
	}

	tests := []validatorTest{
		{
			Name:      "Base",
			Prefix:    "base-",
			Validator: baseValidator,
		},
		{
			Name:      "OrgConf",
			Prefix:    "example-",
			Validator: orgValidator,
		},
	}

	paths, err := filepath.Glob(filepath.Join("testdata", "results", "*.json"))
	if err != nil {
		t.Fatalf("failed to glob for result files: %v", err)
	}

	for j := range tests {
		testCase := tests[j]

		t.Run(testCase.Name, func(t *testing.T) {
			for i := range paths {
				goldenPath := paths[i]

				if !strings.HasPrefix(filepath.Base(goldenPath), testCase.Prefix) {
					continue
				}

				testAgainstGolden(t, goldenPath, testCase)
			}
		})
	}
}

func testAgainstGolden(t *testing.T, goldenPath string, testCase validatorTest) {
	t.Helper()

	sourceDocPath := filepath.Join(
		"testdata",
		strings.TrimPrefix(filepath.Base(goldenPath), testCase.Prefix),
	)

	t.Run(sourceDocPath, func(t *testing.T) {
		var (
			document doc.Document
			want     []revisor.ValidationResult
		)

		err := internal.UnmarshalFile(sourceDocPath, &document)
		if err != nil {
			t.Fatalf("failed to load document: %v", err)
		}

		err = internal.UnmarshalFile(goldenPath, &want)
		if err != nil {
			t.Fatalf("failed to load expected result: %v", err)
		}

		got := testCase.Validator.ValidateDocument(&document)

		for i := range got {
			if !resultHas(want, got[i]) {
				t.Errorf("unexpected validation error: %v", got[i])
			}
		}

		for i := range want {
			if !resultHas(got, want[i]) {
				t.Errorf("missing validation error: %v", want[i])
			}
		}

		if len(got) != len(want) {
			t.Errorf("wanted %d errors, got %d",
				len(want), len(got))
		}
	})
}

func resultHas(list []revisor.ValidationResult, item revisor.ValidationResult) bool {
	for i := range list {
		if equalResult(list[i], item) {
			return true
		}
	}

	return false
}

func equalResult(a, b revisor.ValidationResult) bool {
	if a.Error != b.Error {
		return false
	}

	if len(a.Entity) != len(b.Entity) {
		return false
	}

	for i := range a.Entity {
		if a.Entity[i] != b.Entity[i] {
			return false
		}
	}

	return true
}
