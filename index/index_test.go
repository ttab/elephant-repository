package index_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/ttab/elephant/index"
	"github.com/ttab/elephant/internal"
	"github.com/ttab/elephant/revisor"
)

func TestAnnotationCollection(t *testing.T) {
	var (
		state            index.DocumentState
		golden           map[string]index.Field
		constraints      revisor.ConstraintSet
		extraConstraints revisor.ConstraintSet
	)

	err := internal.UnmarshalFile(
		"testdata/raw_1.input.json", &state)
	if err != nil {
		t.Fatalf("failed to load state data: %v", err)
	}

	err = internal.UnmarshalFile(
		"testdata/raw_1.fields.json", &golden)
	if err != nil {
		t.Fatalf("failed to load golden state: %v", err)
	}

	err = internal.UnmarshalFile(
		"../revisor/constraints/core.json", &constraints)
	if err != nil {
		t.Fatalf("failed to load base constraints: %v", err)
	}

	err = internal.UnmarshalFile(
		"../revisor/constraints/tt.json", &extraConstraints)
	if err != nil {
		t.Fatalf("failed to load org constraints: %v", err)
	}

	validator, err := revisor.NewValidator(constraints, extraConstraints)
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	doc := index.DiscoverFields(validator, state)

	if diff := cmp.Diff(golden, doc.Fields); diff != "" {
		t.Errorf("DiscoverFields() mismatch (-want +got):\n%s", diff)
	}
}
