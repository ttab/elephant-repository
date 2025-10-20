package repository_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"github.com/ttab/newsdoc"
)

func TestTimespan(t *testing.T) {
	regenerate := regenerateTestFixtures()
	dataDir := filepath.Join("..", "testdata", t.Name())

	cases := map[string]timespanCase{
		"constructed":   {},
		"event":         {},
		"planning-item": {},
	}

	defaultTZ := time.UTC

	for name, tc := range cases {
		if tc.Document == "" {
			tc.Document = fmt.Sprintf("%s.json", name)
		}

		if tc.Config == "" {
			tc.Config = fmt.Sprintf("%s-conf.json", name)
		}

		t.Run(name, func(t *testing.T) {
			var (
				doc  newsdoc.Document
				conf tsTestConfigFile
			)

			err := elephantine.UnmarshalFile(
				filepath.Join(dataDir, tc.Document),
				&doc)
			test.Must(t, err, "unmarshal document")

			err = elephantine.UnmarshalFile(
				filepath.Join(dataDir, tc.Config),
				&conf)
			test.Must(t, err, "unmarshal configuration")

			var result []timespanResult

			for i := range conf.Times {
				ex, err := repository.NewTimespanExtractor(
					conf.Times[i], defaultTZ)
				test.Must(t, err, "create extractor")

				spans, err := ex.Extract(doc)
				test.Must(t, err, "extract timespans")

				result = append(result, timespanResult{
					Config: conf.Times[i],
					Timespans: repository.MergeTimespans(
						spans, 1*time.Second,
					),
				})
			}

			test.TestAgainstGolden(t, regenerate, result,
				filepath.Join(dataDir, name+".result.json"))

			docEx, err := repository.NewDocumentTimespanExtractor(
				conf.Times, defaultTZ)
			test.Must(t, err, "create document extractor")

			docSpans, err := docEx.Extract(doc)
			test.Must(t, err, "extract document timestamps")

			test.TestAgainstGolden(t, regenerate, docSpans,
				filepath.Join(dataDir, name+".docspans.json"))
		})
	}
}

type timespanCase struct {
	Config   string
	Document string
}

type timespanResult struct {
	Config    repository.TimespanConfiguration `json:"config"`
	Timespans []repository.Timespan            `json:"timespans"`
}

type tsTestConfigFile struct {
	Times []repository.TimespanConfiguration `json:"times"`
}
