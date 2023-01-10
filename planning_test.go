package docformat_test

import (
	"encoding/json"
	"encoding/xml"
	"os"
	"testing"

	"github.com/ttab/docformat"
)

func TestUnmarshalPlanning(t *testing.T) {
	data, _ := os.ReadFile("planning.xml")

	var nml docformat.Planning

	err := xml.Unmarshal(data, &nml)
	if err != nil {
		t.Fatal(err.Error())
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	enc.Encode(nml)
}
