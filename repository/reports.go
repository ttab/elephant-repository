package repository

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ttab/elephant/internal"
	"golang.org/x/exp/slog"
)

type Report struct {
	Name           string        `json:"name"`
	Title          string        `json:"title"`
	CronExpression string        `json:"cron_expression"`
	GenerateSheet  bool          `json:"generate_sheet,omitempty"`
	Queries        []ReportQuery `json:"queries"`
	SlackChannels  []string      `json:"slack_channels,omitempty"`
}

type ReportResult struct {
	Tables      []string
	Spreadsheet *bytes.Buffer
}

type ReportObject struct {
	Specification Report    `json:"specification"`
	Tables        []string  `json:"tables"`
	Created       time.Time `json:"created"`
}

type ReportQuery struct {
	Name            string                          `json:"name"`
	SQL             string                          `json:"sql"`
	ValueProcessing map[string][]ReportValueProcess `json:"value_processing,omitempty"`
	Summarise       []int                           `json:"summarize,omitempty"`
}

type ReportValueProcess string

const (
	ReportHTMLDecode ReportValueProcess = "html_decode"
)

type Reporter interface {
	AddHeader(q ReportQuery, columns []string) error
	AddRow(values []any) error
	QueryDone() error
}

type Queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func GenerateReport(
	ctx context.Context, logger *slog.Logger, r Report,
	conn Queryer,
) (*ReportResult, error) {
	reporters := map[string]Reporter{}

	var (
		spreadsheet *SpreadsheetReporter
		tables      *TableReporter
	)

	if r.GenerateSheet {
		spreadsheet = NewSpreadsheetReporter()
		defer func() {
			if err := spreadsheet.File.Close(); err != nil {
				logger.ErrorCtx(ctx,
					"failed to close spreadsheet",
					internal.LogKeyError, err,
				)
			}
		}()

		reporters["spreadsheet"] = spreadsheet
	}

	tables = NewTableReporter()

	reporters["tables"] = tables

	// Keep track of total row count so that we don't exhaust all resources
	// doing reporting.
	//
	// TODO: We could be smarter here and save to /tmp and stream to S3 and
	// the like, keeping it simple for now.
	var (
		rowCount int
		maxRows  = 1000
	)

	for _, query := range r.Queries {
		rows, err := conn.Query(ctx, query.SQL)
		if err != nil {
			return nil, fmt.Errorf("failed to execute report query: %w", err)
		}

		firstRow := true

		for rows.Next() {
			fields := rows.FieldDescriptions()

			rowCount++

			if rowCount > maxRows {
				return nil, fmt.Errorf(
					"as a safety feature we don't allow reports that generate more than %d rows",
					maxRows)
			}

			var names []string

			for i := range fields {
				names = append(names, fields[i].Name)
			}

			if firstRow {
				for name, rep := range reporters {
					err := rep.AddHeader(query, names)
					if err != nil {
						return nil, fmt.Errorf(
							"failed to add header for %q: %w",
							name, err)
					}
				}

				firstRow = false
			}

			values, err := rows.Values()
			if err != nil {
				return nil, fmt.Errorf("failed to read row values: %w", err)
			}

			for i, value := range values {
				if query.ValueProcessing != nil {
					for _, vp := range query.ValueProcessing[fields[i].Name] {
						proc, ok := valueProcessors[vp]
						if !ok {
							return nil, fmt.Errorf(
								"unknown value processor %q", vp)
						}

						value = proc(value)
					}

					values[i] = value
				}
			}

			for name, rep := range reporters {
				err := rep.AddRow(values)
				if err != nil {
					return nil, fmt.Errorf(
						"failed to add row for %q: %w",
						name, err)
				}
			}
		}

		for name, rep := range reporters {
			err := rep.QueryDone()
			if err != nil {
				return nil, fmt.Errorf(
					"failed to finish query for %q: %w",
					name, err)
			}
		}
	}

	var result ReportResult

	if tables != nil {
		for _, t := range tables.Tables {
			result.Tables = append(result.Tables, t.Render())
		}
	}

	if spreadsheet != nil {
		buf, err := spreadsheet.File.WriteToBuffer()
		if err != nil {
			return nil, fmt.Errorf(
				"failed to render spreadsheet: %w", err)
		}

		result.Spreadsheet = buf
	}

	return &result, nil
}
