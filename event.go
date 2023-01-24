package docformat

import (
	"fmt"

	navigadoc "github.com/navigacontentlab/navigadoc/doc"
)

func postprocessEvent(nDoc navigadoc.Document, doc *Document) error {
	// Navigadoc conversion takes care of most of these.
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "nrpdate:start":
		case "nrpdate:end":
		case "nrpdate:created":
		case "nrpdate:modified":
		case "urgency":
		case "conceptid":
		case "copyrightHolder":
			doc.Links = append(doc.Links, Block{
				Rel:   "copyrightholder",
				Title: prop.Value,
			})
		case "definition":
			processDefinitionProp(prop, doc)
		case "headline":
			doc.Meta = withBlockOfType("core/event", doc.Meta,
				func(block Block) Block {
					if block.Data == nil {
						block.Data = make(DataMap)

						block.Data["headline"] = prop.Value
					}

					return block
				})
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	if doc.URI == "core://event/" {
		doc.URI = fmt.Sprintf("core://event/%s", doc.UUID)
	}

	for i := range doc.Links {
		switch doc.Links[i].Rel {
		case "topic":
			doc.Links[i].Rel = "subject"
		}
	}

	var eventMeta *Block

	for i := range doc.Meta {
		if doc.Meta[i].Type == "core/event" {
			eventMeta = &doc.Meta[i]
			break
		}
	}

	if eventMeta == nil {
		return fmt.Errorf("missing event metadata block")
	}

	publicDesc, ok := eventMeta.Data["publicDescription"]
	if ok {
		doc.Meta = append(doc.Meta, Block{
			Type: "core/description",
			Role: "public",
			Data: DataMap{
				"text": publicDesc,
			},
		})

		delete(eventMeta.Data, "publicDescription")
	}

	internalDesc, ok := eventMeta.Data["internalDescription"]
	if ok {
		doc.Meta = append(doc.Meta, Block{
			Type: "core/description",
			Role: "internal",
			Data: DataMap{
				"text": internalDesc,
			},
		})

		delete(eventMeta.Data, "internalDescription")
	}

	return nil
}
