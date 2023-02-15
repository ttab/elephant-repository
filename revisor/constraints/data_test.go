package constraints_test

import (
	"testing"

	"github.com/navigacontentlab/revisor/constraints"
)

func TestNaviga(t *testing.T) {
	spec := constraints.Naviga()

	if len(spec.Documents) == 0 {
		t.Fatalf("expected Naviga constraint set to contain documents")
	}
}
