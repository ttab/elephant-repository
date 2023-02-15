package ingest

import (
	"context"
	"encoding/xml"
	"fmt"
	"strings"

	"github.com/ttab/elephant/doc"
)

type NMLConcept struct {
	XMLName  xml.Name       `xml:"conceptItem"`
	Concept  NMLConceptInfo `xml:"concept"`
	ItemMeta NMLItemMeta    `xml:"itemMeta"`
}

type NMLItemMeta struct {
	Properties []ExtProperty `xml:"itemMetaExtProperty"`
}

type NMLConceptInfo struct {
	Employer    string           `xml:"personDetails>affiliation>name"`
	ContactInfo []NMLContactInfo `xml:"personDetails>contactInfo"`
	Definition  []NMLDescription `xml:"definition"`
	Name        []NMLConceptName `xml:"name"`
	Note        string           `xml:"note"`
	Properties  []ExtProperty    `xml:"conceptExtProperty"`
}

type NMLConceptName struct {
	Part string `xml:"part,attr"`
	Role string `xml:"role,attr"`
	Text string `xml:",chardata"`
}

type NMLContactInfo struct {
	Role     string `xml:"role,attr"`
	Address  string `xml:"address>line"`
	Locality string `xml:"locality>name"`
	Country  string `xml:"address>country>name"`
	Phone    string `xml:"phone"`
	Email    string `xml:"email"`
	Web      string `xml:"web"`
}

func (nci NMLContactInfo) ToBlock() doc.Block {
	block := doc.Block{
		Type: "core/contact-info",
		Role: strings.TrimPrefix(nci.Role, "ciprol:"),
		Data: doc.DataMap{
			"address":  nci.Address,
			"locality": nci.Locality,
			"country":  nci.Country,
			"phone":    nci.Phone,
		},
	}

	block.Data["email"] = strings.TrimPrefix(nci.Email, "mailto:")

	dropEmptyData(block.Data)

	if nci.Web != "" {
		url := nci.Web
		if !(strings.HasPrefix(url, "http://") ||
			strings.HasPrefix(url, "https://")) {
			url = "https://" + url
		}

		block.Links = append(block.Links, doc.Block{
			Type: "text/html",
			Rel:  "see-also",
			URL:  url,
		})
	}

	return block
}

func sideloadContactInformation(
	ctx context.Context, oc ObjectGetter,
	evt OCLogEvent, d *doc.Document,
) error {
	var con NMLConcept

	_, err := oc.GetObject(
		ctx, evt.UUID, evt.Content.Version, &con)
	if err != nil {
		return fmt.Errorf("failed to fetch original document: %w", err)
	}

	properties := make(map[string]string)

	for _, p := range con.Concept.Properties {
		properties[p.Type] = p.Literal
	}

	for _, p := range con.ItemMeta.Properties {
		properties[p.Type] = p.Literal
	}

	names := make(map[string]string)

	for _, n := range con.Concept.Name {
		names[n.Part] = n.Text
		names[n.Role] = n.Text
	}

	d.Title = names["nrol:full"]

	d.Meta = withBlockOfType("core/contact", d.Meta,
		func(block doc.Block) doc.Block {
			if block.Data == nil {
				block.Data = make(doc.DataMap)
			}

			block.Data["employer"] = con.Concept.Employer
			block.Data["firstName"] = names["namepart:given"]
			block.Data["lastName"] = names["namepart:family"]
			block.Data["title"] = properties["tt:title"]

			dropEmptyData(block.Data)

			return block
		})

	if properties["ttext:typ"] != "" {
		d.Meta = withBlockOfType("tt/type", d.Meta,
			func(block doc.Block) doc.Block {
				block.Value = properties["ttext:typ"]

				return block
			})
	}

	var work doc.Block

	for _, c := range con.Concept.ContactInfo {
		switch c.Role {
		case "ciprol:office":
			work = c.ToBlock()
		case "ciprol:home":
			d.Meta = append(d.Meta, c.ToBlock())
		}
	}

	for _, c := range con.Concept.ContactInfo {
		if c.Role != "ciprol:mobile" || c.Phone == "" {
			continue
		}

		work.Data["mobile"] = c.Phone
	}

	d.Meta = append(d.Meta, work)

	if con.Concept.Note != "" {
		d.Meta = append(d.Meta, doc.Block{
			Type: "core/note",
			Role: "internal",
			Data: doc.DataMap{
				"text": con.Concept.Note,
			},
		})
	}

	return nil
}
