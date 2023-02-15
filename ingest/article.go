package ingest

import (
	"fmt"

	navigadoc "github.com/navigacontentlab/navigadoc/doc"
	"github.com/ttab/elephant/doc"
)

func postprocessArticle(nDoc navigadoc.Document, d *doc.Document) error {
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "imext:haspublishedversion":
		case "ttext:typ":
		case "by", "creator", "headline", "infoSource", "profil",
			"urgency", "ttext:orglink":
			// Leaked data from wires most probably
		case "ttext:week", "ttext:featsub":
			// TODO: check if this really just was some testing data.
		case "ttext:usage":
			// All non-articles are already filtered out
		case "slugline":
			d.Meta = append(d.Meta, doc.Block{
				Type:  "tt/slugline",
				Value: prop.Value,
			})
		case "description": // Internal?
			if prop.Value == "" {
				break
			}

			d.Meta = append(d.Meta, doc.Block{
				Type: "core/description",
				Role: "internal",
				Data: doc.DataMap{
					"text": prop.Value,
				},
			})
		case "altId":
			d.Links = append(d.Links, doc.Block{
				Rel:  "alternate",
				Type: "tt/alt-id",
				URI:  prop.Value,
			})
		case "language":
			d.Language = prop.Value
		case "sector":
			d.Meta = append(d.Meta, doc.Block{
				Type:  "tt/sector",
				Value: prop.Value,
			})
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	return nil
}
