package docformat

import (
	"fmt"

	navigadoc "github.com/navigacontentlab/navigadoc/doc"
)

func postprocessNewscoverage(nDoc navigadoc.Document, doc *Document) error {
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "role":
		case "copyrightholder":
			doc.Links = append(doc.Links, Block{
				Rel:   "copyrightholder",
				Title: prop.Value,
			})
		case "headline":
			doc.Title = prop.Value
		case "nrp:sector":
			doc.Links = append(doc.Links, Block{
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
