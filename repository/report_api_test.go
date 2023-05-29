package repository_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant/internal/test"
	"github.com/ttab/elephantine/test"
	"golang.org/x/exp/slog"
)

func TestIntegrationReporting(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{
		RunArchiver: true,
	})

	client := tc.ReportsClient(t,
		itest.StandardClaims(t, "report_admin"))

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	docClient := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_write"))

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
		CronTimezone:   "Asia/Tokyo",
		SlackChannels:  []string{"chachachan"},
		Queries: []*repository.ReportQuery{
			{
				Name: "Documents",
				Sql: `
SELECT type, COUNT(*)
FROM document
WHERE created >= date(now())
GROUP BY type`,
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

	wrongTzSpec := test.CloneMessage(&reportSpec)

	wrongTzSpec.CronTimezone = "foo"

	_, err = client.Update(ctx, &repository.UpdateReportRequest{
		Report:  wrongTzSpec,
		Enabled: true,
	})
	test.MustNot(t, err, "accept bogus time zones")

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

	hours := nextExec.Format("15:04")
	if hours != "18:00" {
		t.Fatalf("expected nextExec to be 18:00 in the specified time zone, got %v",
			hours)
	}

	_, offset := nextExec.Zone()
	if offset != 3600*9 {
		t.Fatalf("expected 9 hours offset, got %v", offset/3600)
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

	runRes, err := client.Run(ctx, &repository.RunReportRequest{
		Name: reportSpec.Name,
	})
	test.Must(t, err, "make test run of report")

	test.Equal(t, 1, len(runRes.Tables), "get one report table back")

	pattern := regexp.MustCompile("core/article.* 1 ")

	if !pattern.MatchString(runRes.Tables[0]) {
		t.Fatalf("the table didn't match the expression %q:\n%s",
			pattern, runRes.Tables[0])
	}

	if len(runRes.Spreadsheet) == 0 {
		t.Fatalf("no spreadsheet data was returned: %v", err)
	}

	testRes, err := client.Test(ctx, &repository.TestReportRequest{
		Report: &repository.Report{
			Queries: []*repository.ReportQuery{
				{
					Name: "Document types",
					Sql: `
SELECT type, COUNT(*)
FROM document
GROUP by type`,
				},
			},
		},
	})
	test.Must(t, err, "make test run of report")
	test.Equal(t, 1, len(testRes.Tables), "got one report table back")
}
