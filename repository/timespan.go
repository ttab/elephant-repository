package repository

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ttab/newsdoc"
)

type Timespan struct {
	From time.Time
	To   time.Time
}

func (ts *Timespan) Accomodate(t time.Time) {
	if ts.From.IsZero() || ts.From.After(t) {
		ts.From = t
	}

	if ts.To.IsZero() || ts.To.Before(t) {
		ts.To = t
	}
}

func (ts *Timespan) IsZero() bool {
	return ts.From.IsZero() && ts.To.IsZero()
}

func (ts *Timespan) InLocation(tz *time.Location) Timespan {
	return Timespan{
		From: ts.From.In(tz),
		To:   ts.To.In(tz),
	}
}

// NormaliseTimespans so that all spans are in the given timezone.
func NormaliseTimespans(spans []Timespan, tz *time.Location) []Timespan {
	norm := make([]Timespan, len(spans))

	for i := range spans {
		norm[i] = spans[i].InLocation(tz)
	}

	return norm
}

// MergeTimespans takes a slice of Timespan and merges any that overlap or are
// separated by the tolerance or less.
func MergeTimespans(spans []Timespan, tolerance time.Duration) []Timespan {
	if len(spans) <= 1 {
		return spans
	}

	sort.Slice(spans, func(i, j int) bool {
		return spans[i].From.Before(spans[j].From)
	})

	result := []Timespan{spans[0]}

	// Iterate through the rest of the sorted timespans.
	for i := 1; i < len(spans); i++ {
		last := &result[len(result)-1]
		current := spans[i]

		// Check if the current timespan should be merged with the last
		// one.
		//
		// Overlap: `current.From` is before last To, so the
		// subtraction results in a negative duration, which is less
		// than tolerance.
		//
		// Adjacency: `current.From` equals last To, resulting
		// in 0s, which is less than tolerance.
		//
		// Nearness: The gap is a positive duration but still less than
		// tolerance.
		if current.From.Sub(last.To) < tolerance {
			if current.To.After(last.To) {
				last.To = current.To
			}
		} else {
			result = append(result, current)
		}
	}

	return result
}

func NewDocumentTimespanExtractor(
	configuration []TimespanConfiguration,
	defaultTZ *time.Location,
) (*DocumentTimespanExtractor, error) {
	extractors := make([]*TimespanExtractor, len(configuration))

	for i := range configuration {
		ex, err := NewTimespanExtractor(
			configuration[i], defaultTZ)
		if err != nil {
			return nil, fmt.Errorf("create extractor: %w", err)
		}

		extractors[i] = ex
	}

	return &DocumentTimespanExtractor{
		extractors: extractors,
	}, nil
}

type DocumentTimespanExtractor struct {
	extractors []*TimespanExtractor
}

func (de *DocumentTimespanExtractor) Extract(
	doc newsdoc.Document,
) ([]Timespan, error) {
	var spans []Timespan

	for _, e := range de.extractors {
		s, err := e.Extract(doc)
		if err != nil {
			return nil, err
		}

		spans = append(spans, s...)
	}

	return NormaliseTimespans(
		MergeTimespans(spans, 1*time.Second), time.UTC,
	), nil
}

type TimespanConfiguration struct {
	Expression string `json:"exp"`
	Layout     string `json:"layout,omitempty"`
	Timezone   string `json:"timezone,omitempty"`
}

const (
	defaultDateLayout = "2006-01-02"
)

func NewTimespanExtractor(
	conf TimespanConfiguration,
	defaultTZ *time.Location,
) (*TimespanExtractor, error) {
	ex, err := newsdoc.ValueExtractorFromString(conf.Expression)
	if err != nil {
		return nil, fmt.Errorf("invalid expression: %w", err)
	}

	tz := defaultTZ

	if conf.Timezone != "" {
		l, err := loadTZ(conf.Timezone)
		if err != nil {
			return nil, fmt.Errorf("load timezone: %w", err)
		}

		tz = l
	}

	if tz == nil {
		tz = time.UTC
	}

	return &TimespanExtractor{
		extractor: ex,
		layout:    conf.Layout,
		tz:        tz,
	}, nil
}

type TimespanExtractor struct {
	extractor *newsdoc.ValueExtractor
	layout    string
	tz        *time.Location
}

func (te *TimespanExtractor) Extract(
	doc newsdoc.Document,
) ([]Timespan, error) {
	var spans []Timespan

	extracted := te.extractor.Collect(doc)

	for _, values := range extracted {
		tz := te.tz

		// Find dynamic TZ value.
		for _, c := range values {
			if c.Role != "tz" {
				continue
			}

			z, err := loadTZ(c.Value)
			if err != nil {
				return nil, err
			}

			tz = z

			break
		}

		var span Timespan

		for _, v := range values {
			if v.Role != "" {
				continue
			}

			isDate := v.Annotation == "date"

			layout := te.layout

			switch {
			case layout == "" && isDate:
				layout = defaultDateLayout
			case layout == "":
				layout = time.RFC3339
			}

			t, err := time.ParseInLocation(layout, v.Value, tz)
			if err != nil {
				return nil, fmt.Errorf(
					"invalid %q time: %w", v.Name, err)
			}

			switch isDate {
			case true:
				// Set to the beginning of the day of the
				// timestamp.
				t = time.Date(t.Year(), t.Month(), t.Day(),
					0, 0, 0, 0, t.Location())

				span.Accomodate(t)

				// Postgres has a microsecond resolution for
				// timestamps. The last instant of the day is is
				// tomorrow - 1 microsecond.
				lastInstant := t.AddDate(0, 0, 1).Add(-1 * time.Microsecond)

				span.Accomodate(lastInstant)
			case false:
				span.Accomodate(t)
			}
		}

		if !span.IsZero() {
			spans = append(spans, span)
		}
	}

	return spans, nil
}

var (
	tzOnce  sync.Once
	tzM     sync.RWMutex
	tzCache map[string]*time.Location
)

// loadTZ is a cached timezone loader to prevent us from hitting disk all the
// time when dealing with dynamic timezone values.
func loadTZ(name string) (*time.Location, error) {
	tzOnce.Do(func() {
		tzCache = make(map[string]*time.Location)
	})

	tzM.RLock()
	tz, ok := tzCache[name]
	tzM.RUnlock()

	switch {
	case ok && tz != nil:
		return tz, nil
	case ok && tz == nil:
		return nil, fmt.Errorf("unknown timezone %q", name)
	}

	tzM.Lock()
	defer tzM.Unlock()

	tz, err := time.LoadLocation(name)
	if err != nil {
		tzCache[name] = nil

		return nil, fmt.Errorf("unknown timezone %q", name)
	}

	tzCache[name] = tz

	return tz, nil
}
