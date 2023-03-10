package constraints_test

import (
	"testing"

	"github.com/ttab/elephant/revisor/constraints"
)

func TestCore(t *testing.T) {
	spec, err := constraints.CoreSchema()
	if err != nil {
		t.Fatalf("failed to load core schema: %v", err)
	}

	if len(spec.Documents) == 0 {
		t.Fatalf("expected core constraint set to contain documents")
	}
}
