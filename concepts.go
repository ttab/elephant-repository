package docformat

import (
	"fmt"
	"strings"

	navigadoc "github.com/navigacontentlab/navigadoc/doc"
)

func postprocessContact(nDoc navigadoc.Document, doc *Document) error {
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "headline", "conceptid", "copyrightHolder",
			"nrpdate:created", "nrpdate:modified", "ttext:typ":
		case "tt:kontid":
			doc.Meta = withBlockOfType("core/contact", doc.Meta,
				func(block Block) Block {
					if block.Data == nil {
						block.Data = make(BlockData)
					}

					block.Data["kontid"] = prop.Value

					return block
				})
		case "definition":
			processDefinitionProp(prop, doc)
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	return nil
}

func postprocessPerson(nDoc navigadoc.Document, doc *Document) error {
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "headline", "conceptid":
		case "definition":
			processDefinitionProp(prop, doc)
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	return nil
}

func postprocessOrganisation(nDoc navigadoc.Document, doc *Document) error {
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "headline", "conceptid", "ttext:typ":
		case "definition":
			processDefinitionProp(prop, doc)
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	return nil
}

func postprocessGroup(nDoc navigadoc.Document, doc *Document) error {
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "headline", "nrpdate:created", "copyrightHolder",
			"nrpdate:modified", "conceptid":
		case "tt:kontid":
			doc.Meta = append(doc.Meta, Block{
				Type: "core/group",
				Data: BlockData{
					"kontid": prop.Value,
				},
			})
		case "definition":
			processDefinitionProp(prop, doc)
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	return nil
}

func postprocessTopic(nDoc navigadoc.Document, doc *Document) error {
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "conceptid":
		case "definition":
			processDefinitionProp(prop, doc)
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	return nil
}

func postprocessStory(nDoc navigadoc.Document, doc *Document) error {
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "conceptid":
		case "definition":
			processDefinitionProp(prop, doc)
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	return nil
}

func postprocessAuthor(nDoc navigadoc.Document, doc *Document) error {
	var meta *Block

	for i := range doc.Meta {
		if doc.Meta[i].Type != "core/contact-info" {
			continue
		}

		meta = &doc.Meta[i]

		if meta.Data == nil {
			meta.Data = make(BlockData)
		}

		break
	}

	if meta == nil {
		m := Block{
			Type: "core/contact-info",
			Data: make(BlockData),
		}

		doc.Meta = append(doc.Meta, m)

		meta = &doc.Meta[len(doc.Meta)-1]
	}

	meta.ID = ""

	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "ttext:typ", "conceptid", "imext:haspublishedversion":
		case "definition":
			processDefinitionProp(prop, doc)
		case "headline":
			meta.Data["name"] = prop.Value
		case "imext:firstName", "imext:lastName",
			"imext:initials", "imext:signature":
			attr := strings.TrimPrefix(prop.Name, "imext:")

			meta.Data[attr] = prop.Value
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	dropEmptyData(meta.Data)

	// Clean up bad creator links
	for i, l := range doc.Links {
		if l.Rel != "creator" || l.Type != "core/author" {
			continue
		}

		copy(doc.Links[i:], doc.Links[i+1:])
		doc.Links = doc.Links[0 : len(doc.Links)-1]
	}

	return nil
}

func withBlockOfType(
	blockType string, blocks []Block,
	fn func(block Block) Block,
) []Block {
	newKids := make([]Block, len(blocks))

	copy(newKids, blocks)

	idx := -1
	for i := range newKids {
		if newKids[i].Type == blockType {
			idx = i
			break
		}
	}

	if idx == -1 {
		return append(newKids, fn(Block{
			Type: blockType,
		}))
	}

	newKids[idx] = fn(newKids[idx])

	return newKids
}

func processDefinitionProp(prop navigadoc.Property, doc *Document) {
	if prop.Value == "" {
		return
	}

	ns, role, ok := strings.Cut(paramOrZero(prop.Parameters, "role"), ":")
	if !ok {
		role = ns
	}

	block := Block{
		Type: "core/definition",
		Role: role,
		Data: BlockData{
			"text": prop.Value,
		},
	}

	doc.Meta = append(doc.Meta, block)
}

func paramOrZero(params map[string]string, name string) string {
	if params == nil {
		return ""
	}

	return params[name]
}
