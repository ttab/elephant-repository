package repository_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/ttab/elephant/internal/test"
	"github.com/ttab/elephant/rpc/repository"
	"golang.org/x/exp/slog"
)

func TestReporting(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, true)

	client := tc.ReportsClient(t,
		test.StandardClaims(t, "report_admin"))

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	docClient := tc.DocumentsClient(t,
		test.StandardClaims(t, "doc_write"))

	doc := baseDocument(docUUID, docURI)

	ctx := test.Context(t)

	_, err := docClient.Update(ctx, &repository.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "create article")

	reportSpec := repository.Report{
		Name:           "today",
		Title:          "Documents created today",
		GenerateSheet:  true,
		CronExpression: "0 18 * * *",
		SlackChannels:  []string{"chachachan"},
		Queries: []*repository.ReportQuery{
			{
				Name: "Documents",
				Sql: `
SELECT v.type, COUNT(*)
FROM document AS d
  INNER JOIN document_version AS v
    ON v.uuid = d.uuid AND v.version = d.current_version
GROUP BY v.type`,
			},
		},
	}

	badSpec := test.CloneMessage(&reportSpec)

	badSpec.Queries[0].Sql = `SELECT whatever FROM somewhere`

	_, err = client.Update(ctx, &repository.UpdateReportRequest{
		Report:  badSpec,
		Enabled: true,
	})
	test.MustNot(t, err, "create nonsense report")

	now := time.Now()

	repRes, err := client.Update(ctx, &repository.UpdateReportRequest{
		Report:  &reportSpec,
		Enabled: true,
	})
	test.Must(t, err, "create report")

	nextExec, err := time.Parse(time.RFC3339, repRes.NextExecution)
	test.Must(t, err, "parse next exec time")

	if !nextExec.After(now) {
		t.Fatalf("next exec must be in the future, got %v", nextExec)
	}

	retrieved, err := client.Get(ctx, &repository.GetReportRequest{
		Name: "today",
	})
	test.Must(t, err, "get the saved report")

	test.EqualMessage(t, &repository.GetReportResponse{
		Enabled:       true,
		NextExecution: nextExec.Format(time.RFC3339),
		Report:        &reportSpec,
	}, retrieved, "get the correct definition back")

	testRes, err := client.Test(ctx, &repository.TestReportRequest{
		Name: reportSpec.Name,
	})
	test.Must(t, err, "make test run of report")

	test.Equal(t, 1, len(testRes.Tables), "get one report table back")

	pattern := regexp.MustCompile("core/article.* 1 ")

	if !pattern.MatchString(testRes.Tables[0]) {
		t.Fatalf("the table didn't match the expression %q:\n%s",
			pattern, testRes.Tables[0])
	}

	if len(testRes.Spreadsheet) == 0 {
		t.Fatalf("no spreadsheet data was returned: %v", err)
	}
}
