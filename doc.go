package docformat

import (
	"bytes"
	"encoding/json"
	"sort"
)

type Document struct {
	// UUID is a unique ID for the document, this can be a random v4
	// UUID, or a URI-derived v5 UUID.
	UUID string `json:"uuid,omitempty"`
	// Type is the content type of the document.
	Type string `json:"type,omitempty"`
	// URI identifies the document (in a more human-readable way than
	// the UUID)
	URI string `json:"uri,omitempty"`
	// URL is the browseable location of the document (if any)
	URL string `json:"url,omitempty"`
	// Title is the title of the document, often used as the headline
	// when the document is displayed.
	Title string `json:"title,omitempty"`
	// Content is the content of the documen, this is essentially what
	// gets rendered on the page when you view a document.
	Content []Block `json:"content,omitempty"`
	// Meta is the metadata for a document, this could be stuff like
	// open graph tags and content profile information.
	Meta []Block `json:"meta,omitempty"`
	// Links are links to other resources and entities. This could be
	// links to categories and subject for the document, or authors.
	Links []Block `json:"links,omitempty"`
	// Language is the language used in the document as an IETF language
	// tag. F.ex. "en", "en-UK", "es", or "sv-SE".
	Language string `json:"language,omitempty"`
}

type Block struct {
	// ID is the block ID
	ID string `json:"id,omitempty"`
	// UUID is used to reference another Document in a block.
	UUID string `json:"uuid,omitempty"`
	// URI is used to reference another entity in a document.
	URI string `json:"uri,omitempty"`
	// URL is a browseable URL for the the block.
	URL string `json:"url,omitempty"`
	// Type is the type of the block
	Type string `json:"type,omitempty"`
	// Title is the title/headline of the block, typically used in the
	// presentation of the block.
	Title string `json:"title,omitempty"`
	// Data contains block data
	Data DataMap `json:"data,omitempty"`
	// Relationship describes the relationship to the document/parent
	// entity
	Rel string `json:"rel,omitempty"`
	// Name is a name for the block. An alternative to "rel" when
	// relationship is a term that doesn't fit.
	Name string `json:"name,omitempty"`
	// Value is a value for the block. Useful when we want to store a
	// primitive value.
	Value string `json:"value,omitempty"`
	// ContentType is used to describe the content type of the
	// block/linked entity if it differs from the type of the block.
	ContentType string `json:"contentType,omitempty"`
	// Links are used to link to other resources and documents.
	Links []Block `json:"links,omitempty"`
	// Content is used to embed content blocks.
	Content []Block `json:"content,omitempty"`
	// Meta is used to embed metadata
	Meta []Block `json:"meta,omitempty"`
	// Role is used for
	Role string `json:"role,omitempty"`
}

type DataMap map[string]string

// MarshalJSON implements a custom marshaler to make the JSON output of a
// document deterministic. Maps are unordered.
func (bd DataMap) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer

	keys := make([]string, 0, len(bd))

	for k := range bd {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	buf.WriteString("{")
	for i, k := range keys {
		if i != 0 {
			buf.WriteString(",")
		}

		// marshal key
		key, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}

		buf.Write(key)
		buf.WriteString(":")

		val, err := json.Marshal(bd[k])
		if err != nil {
			return nil, err
		}

		buf.Write(val)
	}

	buf.WriteString("}")

	return buf.Bytes(), nil
}
