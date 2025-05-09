package repository

import (
	"html"
	"strings"
	"unicode/utf8"

	"github.com/microcosm-cc/bluemonday"
	"github.com/ttab/newsdoc"
)

type Counter struct {
	policy *bluemonday.Policy
}

func NewCharCounter() *Counter {
	return &Counter{
		policy: bluemonday.StrictPolicy(),
	}
}

func (cc *Counter) GetKind() MetricKind {
	return MetricKind{
		Name:        "charcount",
		Aggregation: AggregationReplace,
	}
}

func (cc *Counter) IsApplicableTo(doc newsdoc.Document) bool {
	return true
}

func (cc *Counter) MeasureDocument(
	doc newsdoc.Document,
) ([]Measurement, error) {
	var count int64

	for _, block := range doc.Content {
		count += cc.countBlock(block)
	}

	return []Measurement{
		{Value: count},
	}, nil
}

func (cc *Counter) countBlock(block newsdoc.Block) int64 {
	if block.Type != "core/text" || block.Role == "vignette" {
		return 0
	}

	return cc.countTextLength(block.Data)
}

func (cc *Counter) countTextLength(data map[string]string) int64 {
	if data == nil {
		return 0
	}

	text, ok := data["text"]
	if !ok {
		return 0
	}

	plain := strings.TrimSpace(html.UnescapeString(cc.policy.Sanitize(text)))

	return int64(utf8.RuneCountInString(plain))
}
