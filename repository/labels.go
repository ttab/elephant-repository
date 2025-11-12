package repository

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/ttab/newsdoc"
)

type LabelConfiguration struct {
	Expression string
	Template   string
}

type labelExtractor struct {
	Extractor *newsdoc.ValueExtractor
	Template  *template.Template
}

func NewLabelsExtractor(labels []LabelConfiguration) (*LabelsExtractor, error) {
	var lex []*labelExtractor

	for _, c := range labels {
		ex, err := newsdoc.ValueExtractorFromString(c.Expression)
		if err != nil {
			return nil, fmt.Errorf("invalid value extractor %q: %w",
				c.Expression, err)
		}

		tpl, err := template.New("label").Parse(c.Template)
		if err != nil {
			return nil, fmt.Errorf("invalid template %q: %w",
				c.Template, err)
		}

		lex = append(lex, &labelExtractor{
			Extractor: ex,
			Template:  tpl,
		})
	}

	return &LabelsExtractor{
		labels: lex,
	}, nil
}

type LabelsExtractor struct {
	labels []*labelExtractor
}

func (l *LabelsExtractor) Extract(
	doc newsdoc.Document,
) ([]string, error) {
	var labels []string

	var buf bytes.Buffer

	for _, ex := range l.labels {
		items := ex.Extractor.Collect(doc)

		for _, item := range items {
			err := ex.Template.Execute(&buf, item)
			if err != nil {
				return nil, fmt.Errorf("render label: %w", err)
			}

			labels = append(labels, buf.String())

			buf.Reset()
		}
	}

	return labels, nil
}
