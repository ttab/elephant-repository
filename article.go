package docformat

import (
	"fmt"

	navigadoc "github.com/navigacontentlab/navigadoc/doc"
)

func postprocessArticle(nDoc navigadoc.Document, doc *Document) error {
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
			doc.Meta = append(doc.Meta, Block{
				Type:  "tt/slugline",
				Value: prop.Value,
			})
		case "description": // Internal?
			if prop.Value == "" {
				break
			}

			doc.Meta = append(doc.Meta, Block{
				Type: "core/description",
				Role: "internal",
				Data: BlockData{
					"text": prop.Value,
				},
			})
		case "altId":
			doc.Links = append(doc.Links, Block{
				Rel:  "alternate",
				Type: "tt/alt-id",
				URI:  prop.Value,
			})
		case "language":
			doc.Language = prop.Value
		case "sector":
			doc.Meta = append(doc.Meta, Block{
				Type:  "tt/sector",
				Value: prop.Value,
			})
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	return nil
}
