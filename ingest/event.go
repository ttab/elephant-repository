package ingest

import (
	"fmt"

	navigadoc "github.com/navigacontentlab/navigadoc/doc"
	"github.com/ttab/elephant/doc"
)

func postprocessEvent(nDoc navigadoc.Document, d *doc.Document) error {
	// Navigadoc conversion takes care of most of these.
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case propNRPStart:
		case propNRPEnd:
		case propNRPCreated:
		case propNRPModified:
		case propUrgency:
		case propConceptID:
		case propCopyright:
			d.Links = append(d.Links, doc.Block{
				Rel:   "copyrightholder",
				Title: prop.Value,
			})
		case propDefinition:
			processDefinitionProp(prop, d)
		case propHeadline:
			d.Meta = withBlockOfType("core/event", d.Meta,
				func(block doc.Block) doc.Block {
					if block.Data == nil {
						block.Data = make(doc.DataMap)

						block.Data["headline"] = prop.Value
					}

					return block
				})
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	if d.URI == "core://event/" {
		d.URI = fmt.Sprintf("core://event/%s", d.UUID)
	}

	for i := range d.Links {
		if d.Links[i].Rel == relTopic {
			d.Links[i].Rel = "subject"
		}
	}

	var eventMeta *doc.Block

	for i := range d.Meta {
		if d.Meta[i].Type == "core/event" {
			eventMeta = &d.Meta[i]

			break
		}
	}

	if eventMeta == nil {
		return fmt.Errorf("missing event metadata block")
	}

	publicDesc, ok := eventMeta.Data["publicDescription"]
	if ok {
		d.Meta = append(d.Meta, doc.Block{
			Type: "core/description",
			Role: "public",
			Data: doc.DataMap{
				"text": publicDesc,
			},
		})

		delete(eventMeta.Data, "publicDescription")
	}

	internalDesc, ok := eventMeta.Data["internalDescription"]
	if ok {
		d.Meta = append(d.Meta, doc.Block{
			Type: "core/description",
			Role: "internal",
			Data: doc.DataMap{
				"text": internalDesc,
			},
		})

		delete(eventMeta.Data, "internalDescription")
	}

	return nil
}
