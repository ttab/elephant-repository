package revisor

import (
	"encoding/json"
	"fmt"
	"strings"
	"text/template"

	"github.com/ttab/elephant/doc"
)

type Template struct {
	pattern string
	tpl     *template.Template
}

func (t *Template) String() string {
	return t.pattern
}

func (t *Template) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.pattern) //nolint:wrapcheck
}

func (t *Template) UnmarshalJSON(data []byte) error {
	var pattern string

	err := json.Unmarshal(data, &pattern)
	if err != nil {
		return err //nolint:wrapcheck
	}

	tpl, err := template.New("value").Parse(pattern)
	if err != nil {
		return err //nolint:wrapcheck
	}

	t.tpl = tpl
	t.pattern = pattern

	return nil
}

func (t *Template) Render(data TemplateValues) (string, error) {
	var sb strings.Builder

	values := make(map[string]interface{})

	for k := range data {
		v, err := data[k]()
		if err != nil {
			return "", fmt.Errorf("failed to prepare %q: %w", k, err)
		}

		values[k] = v
	}

	err := t.tpl.Execute(&sb, values)
	if err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return sb.String(), nil
}

type TemplateValues map[string]TemplateValue

type TemplateValue func() (interface{}, error)

func BlockTemplateValue(b *doc.Block) TemplateValue {
	var value map[string]interface{}

	return func() (interface{}, error) {
		if b == nil {
			return nil, nil
		}

		if value != nil {
			return value, nil
		}

		c := *b
		c.Links = nil
		c.Content = nil
		c.Meta = nil

		data, err := json.Marshal(&c)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal block: %w", err)
		}

		err = json.Unmarshal(data, &value)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal block value %w", err)
		}

		return value, nil
	}
}

func DocumentTemplateValue(d *doc.Document) TemplateValue {
	var value map[string]interface{}

	return func() (interface{}, error) {
		if d == nil {
			return nil, nil
		}

		if value != nil {
			return value, nil
		}

		c := *d
		c.Links = nil
		c.Content = nil
		c.Meta = nil

		data, err := json.Marshal(&c)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal document: %w", err)
		}

		err = json.Unmarshal(data, &value)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal document value %w", err)
		}

		return value, nil
	}
}
