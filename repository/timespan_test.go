package repository_test

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	rpc_newsdoc "github.com/ttab/elephant-api/newsdoc"
	rpc "github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
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

func TestTimespanIntegration(t *testing.T) {
	regenerate := regenerateTestFixtures()

	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	dataDir := filepath.Join("..", "testdata", t.Name())

	outputDir := filepath.Join(dataDir, "golden")

	err := os.MkdirAll(outputDir, 0o770)
	test.Must(t, err, "create output dir")

	ctx := t.Context()

	logger := slog.New(test.NewLogHandler(t, slog.LevelError))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunEventlogBuilder: true,
		ConfigDirectory:    dataDir,
	})

	docClient := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_read doc_write eventlog_read"))

	writeDoc(t, docClient, dataDir, "nvidia", []*rpc.StatusUpdate{
		{Name: "usable"},
	})
	writeDoc(t, docClient, dataDir, "cars", nil)

	getEvents, err := docClient.GetMatching(ctx, &rpc.GetMatchingRequest{
		Type: "core/event",
		Timespan: &rpc.Timespan{
			From: "2025-10-28T00:00:00+02:00",
			To:   "2025-10-28T23:59:59+02:00",
		},
		IncludeDocuments: true,
		IncludeMeta:      true,
	})
	test.Must(t, err, "get matching events for 2025-10-28")

	// The order is not defined, sort by UUID for consistency.
	slices.SortFunc(getEvents.Matches, func(a, b *rpc.DocumentMatch) int {
		return strings.Compare(a.Uuid, b.Uuid)
	})

	test.TestMessageAgainstGolden(t, regenerate, getEvents,
		filepath.Join(outputDir, "2025-10-28.v1.json"),
		test.IgnoreTimestamps{},
		test.IgnoreField[string]{
			Name:       "nonce",
			DummyValue: "e380cbcd-f10c-4edb-911f-56986866a51b",
			Validator: func(v string) error {
				_, err := uuid.Parse(v)

				return err //nolint: wrapcheck
			},
		},
	)

	emptyDay, err := docClient.GetMatching(ctx, &rpc.GetMatchingRequest{
		Type: "core/event",
		Timespan: &rpc.Timespan{
			From: "2025-10-21T00:00:00+02:00",
			To:   "2025-10-21T23:59:59+02:00",
		},
		IncludeDocuments: true,
		IncludeMeta:      true,
	})
	test.Must(t, err, "get matching events for 2025-10-21")

	test.Equal(t, 0, len(emptyDay.Matches), "get no matches for 2025-10-21")

	// Event that occurs on 2025-10-21
	writeDoc(t, docClient, dataDir, "woodsy", nil)

	popDay, err := docClient.GetMatching(ctx, &rpc.GetMatchingRequest{
		Type: "core/event",
		Timespan: &rpc.Timespan{
			From: "2025-10-21T00:00:00+02:00",
			To:   "2025-10-21T23:59:59+02:00",
		},
		IncludeDocuments: true,
		IncludeMeta:      true,
	})
	test.Must(t, err, "get matching events for 2025-10-21")

	test.Equal(t, 1, len(popDay.Matches), "get one match for 2025-10-21")
}

func writeDoc(
	t *testing.T, docClient rpc.Documents,
	dataDir string, name string,
	status []*rpc.StatusUpdate,
) string {
	t.Helper()

	var doc newsdoc.Document

	err := elephantine.UnmarshalFile(
		filepath.Join(dataDir, name+".json"), &doc)
	test.Must(t, err, "unmarshal %s document", name)

	_, err = docClient.Update(t.Context(), &rpc.UpdateRequest{
		Uuid:     doc.UUID,
		Document: rpc_newsdoc.DocumentToRPC(doc),
		Status:   status,
	})
	test.Must(t, err, "write %s document", name)

	return doc.UUID
}
