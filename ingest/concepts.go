package ingest

import (
	"fmt"
	"strings"

	navigadoc "github.com/navigacontentlab/navigadoc/doc"
	"github.com/ttab/elephant/doc"
)

func postprocessContact(nDoc navigadoc.Document, d *doc.Document) error {
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "headline", "conceptid", "copyrightHolder",
			"nrpdate:created", "nrpdate:modified", "ttext:typ":
		case "tt:kontid":
			d.Meta = withBlockOfType("core/contact", d.Meta,
				func(block doc.Block) doc.Block {
					if block.Data == nil {
						block.Data = make(doc.DataMap)
					}

					block.Data["kontid"] = prop.Value

					return block
				})
		case "definition":
			processDefinitionProp(prop, d)
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	return nil
}

func postprocessPerson(nDoc navigadoc.Document, doc *doc.Document) error {
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

func postprocessOrganisation(nDoc navigadoc.Document, doc *doc.Document) error {
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

func postprocessGroup(nDoc navigadoc.Document, d *doc.Document) error {
	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "headline", "nrpdate:created", "copyrightHolder",
			"nrpdate:modified", "conceptid":
		case "tt:kontid":
			d.Meta = append(d.Meta, doc.Block{
				Type: "core/group",
				Data: doc.DataMap{
					"kontid": prop.Value,
				},
			})
		case "definition":
			processDefinitionProp(prop, d)
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	return nil
}

func postprocessTopic(nDoc navigadoc.Document, doc *doc.Document) error {
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

func postprocessStory(nDoc navigadoc.Document, doc *doc.Document) error {
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

func postprocessAuthor(nDoc navigadoc.Document, d *doc.Document) error {
	var meta *doc.Block

	for i := range d.Meta {
		if d.Meta[i].Type != "core/contact-info" {
			continue
		}

		meta = &d.Meta[i]

		if meta.Data == nil {
			meta.Data = make(doc.DataMap)
		}

		break
	}

	if meta == nil {
		m := doc.Block{
			Type: "core/contact-info",
			Data: make(doc.DataMap),
		}

		d.Meta = append(d.Meta, m)

		meta = &d.Meta[len(d.Meta)-1]
	}

	meta.ID = ""

	for _, prop := range nDoc.Properties {
		switch prop.Name {
		case "ttext:typ", "conceptid", "imext:haspublishedversion":
		case "definition":
			processDefinitionProp(prop, d)
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
	for i, l := range d.Links {
		if l.Rel != "creator" || l.Type != "core/author" {
			continue
		}

		copy(d.Links[i:], d.Links[i+1:])
		d.Links = d.Links[0 : len(d.Links)-1]
	}

	return nil
}

func withBlockOfType(
	blockType string, blocks []doc.Block,
	fn func(block doc.Block) doc.Block,
) []doc.Block {
	newKids := make([]doc.Block, len(blocks))

	copy(newKids, blocks)

	idx := -1
	for i := range newKids {
		if newKids[i].Type == blockType {
			idx = i
			break
		}
	}

	if idx == -1 {
		return append(newKids, fn(doc.Block{
			Type: blockType,
		}))
	}

	newKids[idx] = fn(newKids[idx])

	return newKids
}

func processDefinitionProp(prop navigadoc.Property, d *doc.Document) {
	if prop.Value == "" {
		return
	}

	ns, role, ok := strings.Cut(paramOrZero(prop.Parameters, "role"), ":")
	if !ok {
		role = ns
	}

	block := doc.Block{
		Type: "core/definition",
		Role: role,
		Data: doc.DataMap{
			"text": prop.Value,
		},
	}

	d.Meta = append(d.Meta, block)
}

func paramOrZero(params map[string]string, name string) string {
	if params == nil {
		return ""
	}

	return params[name]
}
