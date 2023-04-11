package sinks

import (
	"regexp"

	"github.com/ttab/elephant/doc"
	"github.com/ttab/elephant/repository"
	"golang.org/x/exp/slices"
)

type EventDetail struct {
	Event    repository.Event `json:"event"`
	Document *DocumentDetail  `json:"document"`
}

type DocumentDetail struct {
	UUID         string                    `json:"uuid"`
	URI          string                    `json:"uri"`
	Type         string                    `json:"type"`
	Title        string                    `json:"title"`
	LinkTypes    []string                  `json:"link_types,omitempty"`
	Links        map[string][]DocumentLink `json:"rels"`
	MetaTypes    []string                  `json:"meta_types,omitempty"`
	Meta         map[string][]DocumentMeta `json:"meta"`
	ContentTypes []string                  `json:"content_types,omitempty"`
	ContentUUIDs []string                  `json:"content_uuids,omitempty"`
	ContentURIs  []string                  `json:"content_uris,omitempty"`
}

type DocumentMeta struct {
	Role  string      `json:"role,omitempty"`
	Value string      `json:"value,omitempty"`
	Data  doc.DataMap `json:"data,omitempty"`
}

type DocumentLink struct {
	UUID  string `json:"uuid,omitempty"`
	URI   string `json:"uri,omitempty"`
	Type  string `json:"type,omitempty"`
	Role  string `json:"role,omitempty"`
	Value string `json:"value,omitempty"`
}

var nonAlphaNum = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)

func DetailFromDocument(d *doc.Document) *DocumentDetail {
	if d == nil {
		return nil
	}

	e := DocumentDetail{
		UUID:  d.UUID,
		URI:   d.URI,
		Type:  d.Type,
		Title: d.Title,
		Links: make(map[string][]DocumentLink),
		Meta:  make(map[string][]DocumentMeta),
	}

	for _, l := range d.Links {
		dl := DocumentLink{
			UUID:  l.UUID,
			URI:   l.URI,
			Type:  l.Type,
			Role:  l.Role,
			Value: l.Value,
		}

		if !slices.Contains(e.LinkTypes, l.Type) {
			e.LinkTypes = append(e.LinkTypes, l.Type)
		}

		relKey := nonAlphaNum.ReplaceAllString(l.Rel, "_")

		e.Links[relKey] = append(e.Links[relKey], dl)
	}

	for _, m := range d.Meta {
		dm := DocumentMeta{
			Role:  m.Role,
			Value: m.Value,
		}

		if !slices.Contains(e.MetaTypes, m.Type) {
			e.MetaTypes = append(e.MetaTypes, m.Type)
		}

		for k, v := range m.Data {
			if k == "text" {
				continue
			}

			if dm.Data == nil {
				dm.Data = make(doc.DataMap)
			}

			dm.Data[k] = v
		}

		typeKey := nonAlphaNum.ReplaceAllString(m.Type, "_")

		e.Meta[typeKey] = append(e.Meta[typeKey], dm)
	}

	for _, c := range d.Content {
		if !slices.Contains(e.ContentTypes, c.Type) {
			e.ContentTypes = append(e.ContentTypes, c.Type)
		}

		if c.URI != "" && !slices.Contains(e.ContentURIs, c.URI) {
			e.ContentURIs = append(e.ContentURIs, c.URI)
		}

		if c.UUID != "" && !slices.Contains(e.ContentUUIDs, c.UUID) {
			e.ContentUUIDs = append(e.ContentUUIDs, c.UUID)
		}
	}

	return &e
}
