package ingest

import (
	"fmt"

	navigadoc "github.com/navigacontentlab/navigadoc/doc"
	"github.com/ttab/elephant/doc"
)

func postprocessArticle(nDoc navigadoc.Document, d *doc.Document) error {
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case propHasPublished:
		case propTTType:
		case propBy, propCreator, propHeadline, propInfoSource,
			propProfile, propUrgency, propTTOrgLink:
			// Leaked data from wires most probably
		case propTTWeek, propTTFeatSub:
			// TODO: check if this really just was some testing data.
		case propTTUsage:
			// All non-articles are already filtered out
		case propSlugline:
			d.Meta = append(d.Meta, doc.Block{
				Type:  "tt/slugline",
				Value: prop.Value,
			})
		case propDescription: // Internal?
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
		case propAlternateID:
			d.Links = append(d.Links, doc.Block{
				Rel:  "alternate",
				Type: "tt/alt-id",
				URI:  prop.Value,
			})
		case propLanguage:
			d.Language = prop.Value
		case propTTSector:
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
