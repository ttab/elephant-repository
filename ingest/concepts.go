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
		case propHeadline, propConceptID, propCopyright,
			propNRPCreated, propNRPModified, propTTType:
		case propTTAccountID:
			break
		case propDefinition:
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
		case propHeadline, propConceptID:
		case propDefinition:
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
		case propHeadline, propConceptID, propTTType:
		case propDefinition:
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
		case propHeadline, propNRPCreated, propCopyright,
			propNRPModified, propConceptID:
		case propTTAccountID:
			break
		case propDefinition:
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
		case propConceptID:
		case propDefinition:
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
		case propConceptID:
		case propDefinition:
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
		case propTTType, propConceptID, propHasPublished:
		case propDefinition:
			processDefinitionProp(prop, d)
		case propHeadline:
			meta.Data["name"] = prop.Value
		case propFirstName, propLastName,
			propTTInitials, propTTSignature:
			attr := strings.TrimPrefix(prop.Name, "imext:")

			meta.Data[attr] = prop.Value
		default:
			return fmt.Errorf("unknown property %q", prop.Name)
		}
	}

	if meta.Data["initials"] == "" {
		meta.Data["initials"] = meta.Data["signature"]
	}

	if meta.Data["initials"] == meta.Data["signature"] {
		delete(meta.Data, "signature")
	}

	dropEmptyData(meta.Data)

	// Clean up bad creator links
	for i, l := range d.Links {
		if l.Rel != relCreator || l.Type != "core/author" {
			continue
		}

		copy(d.Links[i:], d.Links[i+1:])
		d.Links = d.Links[0 : len(d.Links)-1]
	}

	return nil
}

// withBlockOfType is a helper function that finds a block of the given type and
// lets the callback function modify it. If no block with a matching type is
// found the callback is called with a new block and the result is appended to
// the block slice.
//
// The function returns a copy of the input blocks slice.
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
		// This is safe as we've populated the full newKids slice using
		// copy().
		newKids = append(newKids, fn(doc.Block{ //nolint:makezero

			Type: blockType,
		}))

		return newKids
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
