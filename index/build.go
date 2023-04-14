package index

import (
	"regexp"
	"strings"
	"time"

	"github.com/microcosm-cc/bluemonday"
	"github.com/ttab/elephant/doc"
	"github.com/ttab/elephant/revisor"
)

func BuildDocument(validator *revisor.Validator, state DocumentState) *Document {
	d := NewDocument()
	doc := &state.Document

	d.AddField("document.title", TypeText, doc.Title)
	d.AddField("document.uri", TypeKeyword, doc.URI)
	d.AddField("document.url", TypeKeyword, doc.URL)
	d.AddField("document.language", TypeKeyword, doc.Language)

	d.AddInteger("current_version", state.CurrentVersion)
	d.AddTime("created", state.Created)
	d.AddTime("modified", state.Modified)

	for name, status := range state.Heads {
		base := "heads." + name

		d.AddInteger(base+".id", status.ID)
		d.AddInteger(base+".version", status.Version)
		d.AddField(base+".creator", TypeKeyword, status.Creator)
		d.AddTime(base+".created", status.Created)

		for k, v := range status.Meta {
			d.AddField(base+".meta."+k, TypeKeyword, v)
		}
	}

	policy := bluemonday.StrictPolicy()

	var text []string

	for i := range doc.Content {
		text = blockText(policy, doc.Content[i], text)
	}

	d.Fields["text"] = Field{
		Type:   TypeText,
		Values: text,
	}

	coll := NewValueCollector()

	_ = validator.ValidateDocument(doc,
		revisor.WithValueCollector(coll))

	for _, a := range coll.Values() {
		var ft FieldType

		val := a.Value

		switch {
		case a.Constraint.Format == revisor.StringFormatFloat:
			ft = TypeDouble
		case a.Constraint.Format == revisor.StringFormatInt:
			ft = TypeLong
		case a.Constraint.Format == revisor.StringFormatBoolean:
			ft = TypeBoolean
		case a.Constraint.Format == revisor.StringFormatUUID:
			ft = TypeKeyword
		case a.Constraint.Format == revisor.StringFormatRFC3339:
			ft = TypeDate
		case a.Constraint.Format == revisor.StringFormatHTML:
			ft = TypeText
			val = policy.Sanitize(val)
		case a.Constraint.Time != "":
			ft = TypeDate

			t, err := time.Parse(a.Constraint.Time, val)
			if err == nil {
				val = t.Format(time.RFC3339)
			}
		case len(a.Constraint.Enum) > 0:
			ft = TypeKeyword
		case a.Constraint.Pattern != nil:
			ft = TypeKeyword
		case len(a.Constraint.Glob) > 0:
			ft = TypeKeyword
		default:
			ft = TypeText
		}

		var parent revisor.EntityRef

		if len(a.Ref) > 1 {
			parent = a.Ref[len(a.Ref)-2]
		}

		tail := a.Ref[len(a.Ref)-1]

		// Omit rel here as it's part of the link field name.
		if isKind(parent, revisor.BlockKindLink) &&
			tail.RefType == revisor.RefTypeAttribute &&
			tail.Name == "rel" {
			continue
		}

		// Omit rel here as it's part of meta and content field names.
		if isKind(parent, revisor.BlockKindMeta, revisor.BlockKindContent) &&
			tail.RefType == revisor.RefTypeAttribute &&
			tail.Name == "type" {
			continue
		}

		// All attributes except title and value should default to
		// keyword if they're just text.
		if tail.RefType == revisor.RefTypeAttribute && ft == TypeText {
			switch tail.Name {
			case "title", "value":
			default:
				ft = TypeKeyword
			}
		}

		d.AddField(
			"document."+entityRefsToPath(doc, a.Ref),
			ft, val,
		)
	}

	return d
}

func blockText(policy *bluemonday.Policy, b doc.Block, text []string) []string {
	if b.Data != nil {
		t := b.Data["text"]
		if t != "" {
			text = append(text, policy.Sanitize(t))
		}
	}

	for i := range b.Content {
		text = blockText(policy, b.Content[i], text)
	}

	return text
}

func isKind(r revisor.EntityRef, kind ...revisor.BlockKind) bool {
	if r.RefType != revisor.RefTypeBlock {
		return false
	}

	for _, k := range kind {
		if r.BlockKind == k {
			return true
		}
	}

	return false
}

var nonAlphaNum = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)

func entityRefsToPath(doc *doc.Document, refs []revisor.EntityRef) string {
	r := make([]string, len(refs))

	var source revisor.BlockSource = revisor.NewDocumentBlocks(doc)

	for i, v := range refs {
		switch v.RefType {
		case revisor.RefTypeData:
			r[i] = "data." + v.Name
		case revisor.RefTypeAttribute:
			r[i] = v.Name
		case revisor.RefTypeBlock:
			blocks := source.GetBlocks(v.BlockKind)
			block := blocks[v.Index]

			switch v.BlockKind {
			case revisor.BlockKindLink:
				key := nonAlphaNum.ReplaceAllString(block.Rel, "_")
				r[i] = "rel." + key
			case revisor.BlockKindMeta:
				key := nonAlphaNum.ReplaceAllString(block.Type, "_")
				r[i] = "meta." + key
			case revisor.BlockKindContent:
				key := nonAlphaNum.ReplaceAllString(block.Type, "_")
				r[i] = "content." + key
			}

			source = revisor.NewNestedBlocks(&block)
		}
	}

	return strings.Join(r, ".")
}

type ValueCollector struct {
	c    *collectorAnnotations
	path []revisor.EntityRef
}

type collectorAnnotations struct {
	List []revisor.ValueAnnotation
}

func NewValueCollector() *ValueCollector {
	return &ValueCollector{
		c: &collectorAnnotations{},
	}
}

func (c *ValueCollector) CollectValue(a revisor.ValueAnnotation) {
	a.Ref = append(c.path[0:len(c.path):len(c.path)], a.Ref...)
	c.c.List = append(c.c.List, a)
}

func (c *ValueCollector) With(ref revisor.EntityRef) revisor.ValueCollector {
	n := ValueCollector{
		c:    c.c,
		path: append(c.path[0:len(c.path):len(c.path)], ref),
	}

	return &n
}

func (c *ValueCollector) Values() []revisor.ValueAnnotation {
	return c.c.List
}

var _ revisor.ValueCollector = &ValueCollector{}
