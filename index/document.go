package index

import (
	"strconv"
	"time"

	"github.com/ttab/elephant/doc"
	"github.com/ttab/elephant/repository"
	"golang.org/x/exp/slices"
)

// DocumentState is the full state that we want to index.
type DocumentState struct {
	Created        time.Time                    `json:"created"`
	Modified       time.Time                    `json:"modified"`
	CurrentVersion int64                        `json:"current_version"`
	ACL            []repository.ACLEntry        `json:"acl"`
	Heads          map[string]repository.Status `json:"heads"`
	Document       doc.Document                 `json:"document"`
}

type Document struct {
	Fields map[string]Field
}

func NewDocument() *Document {
	return &Document{
		Fields: make(map[string]Field),
	}
}

func (d *Document) AddTime(name string, value time.Time) {
	v := value.Format(time.RFC3339)

	d.AddField(name, TypeDate, v)
}

func (d *Document) AddInteger(name string, value int64) {
	v := strconv.FormatInt(value, 10)

	d.AddField(name, TypeLong, v)
}

func (d *Document) AddField(name string, t FieldType, value string) {
	e := d.Fields[name]

	if t.Priority() > e.Type.Priority() {
		e.Type = t
	}

	if !slices.Contains(e.Values, value) {
		e.Values = append(e.Values, value)
	}

	d.Fields[name] = e
}

func (d *Document) Mappings() Mappings {
	m := Mappings{
		Properties: make(map[string]Mapping),
	}

	for name, def := range d.Fields {
		m.Properties[name] = Mapping{Type: def.Type}
	}

	return m
}

func (d *Document) Values() map[string][]string {
	v := make(map[string][]string)

	for k := range d.Fields {
		v[k] = d.Fields[k].Values
	}

	return v
}
