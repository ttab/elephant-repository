package ingest

import (
	"fmt"

	navigadoc "github.com/navigacontentlab/navigadoc/doc"
	"github.com/ttab/elephant/doc"
)

func postprocessNewscoverage(nDoc navigadoc.Document, d *doc.Document) error {
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "role":
		case "copyrightholder":
			d.Links = append(d.Links, doc.Block{
				Rel:   "copyrightholder",
				Title: prop.Value,
			})
		case "headline":
			d.Title = prop.Value
		case "nrp:sector":
			d.Links = append(d.Links, doc.Block{
				Rel:   "sector",
				Value: prop.Value,
				Title: prop.Parameters["literal"],
			})
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	return nil
}
