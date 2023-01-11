package docformat

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve/v2"
)

type SearchIndex struct {
	index bleve.Index
}

func NewSearchIndex(indexPath string) (*SearchIndex, error) {
	indexExists := true

	_, err := os.Stat(indexPath)
	if errors.Is(err, os.ErrNotExist) {
		indexExists = false
	} else if err != nil {
		return nil, fmt.Errorf("failed to check if index exists: %w", err)
	}

	var index bleve.Index

	if indexExists {
		index, err = bleve.Open(indexPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create index: %w", err)
		}
	} else {
		mapping := bleve.NewIndexMapping()

		index, err = bleve.New(indexPath, mapping)
		if err != nil {
			return nil, fmt.Errorf("failed to create index: %w", err)
		}
	}

	return &SearchIndex{
		index: index,
	}, nil
}

func (si SearchIndex) Fields(uuid string) (map[string][]string, error) {
	res, err := si.index.Search(&bleve.SearchRequest{
		Query:  bleve.NewDocIDQuery([]string{uuid}),
		Size:   20,
		Fields: []string{"*"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query index: %w", err)
	}

	fields := make(map[string][]string)

	for _, hit := range res.Hits {
		for key := range hit.Fields {
			stringValue, _ := hit.Fields[key].(string)
			sliceValue, _ := hit.Fields[key].([]interface{})

			if len(stringValue) > 0 {
				sliceValue = append(sliceValue, stringValue)
			}

			for _, v := range sliceValue {
				fields[key] = append(fields[key], fmt.Sprintf(
					"%v", v))
			}
		}
	}

	return fields, nil
}

type SearchHit struct {
	UUID     string
	Title    string
	Type     string
	Modified time.Time
}

func (si SearchIndex) Search(q string) ([]SearchHit, error) {
	req := bleve.SearchRequest{
		Query:  bleve.NewQueryStringQuery(q),
		Size:   30,
		Fields: []string{"title", "type", "modified"},
	}

	req.SortBy([]string{"-modified"})

	res, err := si.index.Search(&req)
	if err != nil {
		return nil, fmt.Errorf("failed to query index: %w", err)
	}

	var result []SearchHit

	for _, hit := range res.Hits {
		h := SearchHit{
			UUID: hit.ID,
		}

		for key := range hit.Fields {
			stringValue, _ := hit.Fields[key].(string)
			sliceValue, _ := hit.Fields[key].([]interface{})

			if len(sliceValue) > 0 {
				v, ok := sliceValue[0].(string)
				if ok {
					stringValue = v
				}
			}

			switch key {
			case "title":
				h.Title = stringValue
			case "type":
				h.Type = stringValue
			case "modified":
				t, err := time.Parse(time.RFC3339, stringValue)
				if err != nil {
					return nil, fmt.Errorf(
						"invalid modified timestamp in index for %q: %w",
						hit.ID, err)
				}

				h.Modified = t
			}
		}

		result = append(result, h)
	}

	return result, nil
}

type BleveDoc map[string][]any

func (bd BleveDoc) Add(key string, value ...any) {
	for i := range value {
		// Ignore empty strings
		s, ok := value[i].(string)
		if ok && s == "" {
			continue
		}

		bd[key] = append(bd[key], value[i])
	}
}

func (si SearchIndex) IndexDocument(meta DocumentMeta, doc Document) error {
	flat := make(BleveDoc)

	flat.Add("uuid", doc.UUID)
	flat.Add("title", doc.Title)
	flat.Add("type", doc.Type)
	flat.Add("uri", doc.Language)
	flat.Add("url", doc.Language)
	flat.Add("language", doc.Language)
	flat.Add("deleted", strconv.FormatBool(meta.Deleted))
	flat.Add("modified", meta.Updates[0].Created.Format(time.RFC3339))

	var (
		lastStatusName string
		lastStatus     Status
	)

	for name, list := range meta.Statuses {
		if len(list) == 0 {
			continue
		}

		flat.Add("status", name)
		flat.Add(name+".count", len(list))

		if list[len(list)-1].Created.After(lastStatus.Created) {
			lastStatusName = name
			lastStatus = list[len(list)-1]
		}
	}

	flat.Add("last_status", lastStatusName)

	updaters := make(map[string]bool)

	for _, rev := range meta.Updates {
		updaters[rev.Updater.Name] = true
		flat.Add("modified", rev.Created.Format(time.RFC3339))
	}

	flat.Add("versions.count", len(meta.Updates))

	for name := range updaters {
		flat.Add("updater", name)
	}

	addBlocks("links", flat, doc.Links)
	addBlocks("meta", flat, doc.Meta)
	addBlocks("content", flat, doc.Content)

	err := si.index.Index(doc.UUID, flat)
	if err != nil {
		return fmt.Errorf("indexing failed: %w", err)
	}

	return nil
}

func addBlocks(kind string, flat BleveDoc, blocks []Block) {
	for _, b := range blocks {
		local := make(BleveDoc)

		var keys []string

		if b.Rel != "" {
			keys = append(keys, b.Rel)
			local.Add("rel", b.Rel)
		}

		if b.Type != "" {
			keys = append(keys, strings.ReplaceAll(b.Type, "/", "."))
			local.Add("type", b.Type)
		}

		local.Add("title", b.Title)
		local.Add("role", b.Role)
		local.Add("uri", b.URI)
		local.Add("url", b.URL)
		local.Add("uuid", b.UUID)
		local.Add("content_type", b.ContentType)
		local.Add("id", b.ID)
		local.Add("name", b.Name)
		local.Add("value", b.Value)

		data := b.Data
		if data == nil {
			data = make(BlockData)
		}

		flat.Add("text", data["text"])

		for k, v := range b.Data {
			if kind == "content" && k == "text" {
				continue
			}

			local.Add("data."+k, v)
		}

		for _, prefix := range keys {
			key := kind + "." + prefix

			for k, v := range local {
				flat.Add(key+"."+k, v...)
			}
		}
	}
}
