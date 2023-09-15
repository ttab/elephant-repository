package planning_test

import (
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/ttab/elephant-repository/planning"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"github.com/ttab/newsdoc"
)

func TestItemToRows(t *testing.T) {
	regenerate := os.Getenv("REGENERATE") == "true"

	var doc newsdoc.Document

	err := elephantine.UnmarshalFile(
		"../testdata/planning_newsdoc.json", &doc)
	test.Must(t, err, "unmarshal NewsDoc")

	item, err := planning.NewItemFromDocument(doc)
	test.Must(t, err, "create news item")

	rows, err := item.ToRows(1)
	test.Must(t, err, "convert item to rows")

	goldenPath := "../testdata/planning_rows.json"

	if regenerate {
		err := elephantine.MarshalFile(goldenPath, rows)
		test.Must(t, err, "update %q", goldenPath)
	}

	var golden planning.Rows

	err = elephantine.UnmarshalFile(goldenPath, &golden)
	test.Must(t, err, "unmarshal expected rows")

	if diff := cmp.Diff(&golden, rows); diff != "" {
		t.Errorf("ToRows() mismatch (-want +got):\n%s", diff)
	}
}
