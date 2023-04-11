package repository

import (
	context "context"
	"fmt"
	"time"

	"github.com/adhocore/gronx"
	"github.com/ttab/elephant/rpc/repository"
	"github.com/twitchtv/twirp"
	"golang.org/x/exp/slog"
)

type ReportsService struct {
	logger      *slog.Logger
	store       ReportStore
	reportingDB Queryer
}

func NewReportsService(
	logger *slog.Logger, store ReportStore, reportingDB Queryer,
) *ReportsService {
	return &ReportsService{
		logger:      logger,
		store:       store,
		reportingDB: reportingDB,
	}
}

// Interface guard.
var _ repository.Reports = &ReportsService{}

// Update or create a report.
func (s *ReportsService) Update(
	ctx context.Context, req *repository.UpdateReportRequest,
) (*repository.UpdateReportResponse, error) {
	err := requireAnyScope(ctx, "report_admin")
	if err != nil {
		return nil, err
	}

	if req.Report == nil {
		return nil, twirp.RequiredArgumentError("report")
	}

	if req.Report.Name == "" {
		return nil, twirp.RequiredArgumentError("report.name")
	}

	if req.Report.Title == "" {
		return nil, twirp.RequiredArgumentError("report.title")
	}

	if req.Report.CronExpression == "" {
		return nil, twirp.RequiredArgumentError("report.cron_expression")
	}

	if req.Report.CronTimezone == "" {
		return nil, twirp.RequiredArgumentError("report.cron_timezone")
	}

	if len(req.Report.Queries) == 0 {
		return nil, twirp.RequiredArgumentError("report.queries")
	}

	for _, query := range req.Report.Queries {
		if query == nil {
			return nil, twirp.InvalidArgumentError(
				"report.queries", "a query cannot be nil")
		}

		if query.Name == "" {
			return nil, twirp.RequiredArgumentError(
				"report.queries.name")
		}

		if query.Sql == "" {
			return nil, twirp.RequiredArgumentError(
				"report.queries.sql")
		}
	}

	_, err = gronx.NextTick(req.Report.CronExpression, false)
	if err != nil {
		return nil, twirp.InvalidArgumentError(
			"report.cron_expression", err.Error())
	}

	_, err = time.LoadLocation(req.Report.CronTimezone)
	if err != nil {
		return nil, twirp.InvalidArgumentError(
			"report.cron_timezone", err.Error())
	}

	report, err := ReportFromRPC(req.Report)
	if err != nil {
		return nil, twirp.InvalidArgumentError("report", err.Error())
	}

	_, err = GenerateReport(ctx, s.logger, report, s.reportingDB)
	if err != nil {
		return nil, twirp.InvalidArgumentError("report", fmt.Sprintf(
			"failed to perform test execution: %v", err))
	}

	nextExecution, err := s.store.UpdateReport(ctx, report, req.Enabled)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to store report: %w", err)
	}

	return &repository.UpdateReportResponse{
		NextExecution: nextExecution.Format(time.RFC3339),
	}, nil
}

// Get a report.
func (s *ReportsService) Get(
	ctx context.Context, req *repository.GetReportRequest,
) (*repository.GetReportResponse, error) {
	err := requireAnyScope(ctx, "report_admin")
	if err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	res, err := s.store.GetReport(ctx, req.Name)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.NotFound.Error(err.Error())
	} else if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to load report: %w", err)
	}

	return &repository.GetReportResponse{
		Report:        ReportToRPC(res.Report),
		Enabled:       res.Enabled,
		NextExecution: res.NextExecution.Format(time.RFC3339),
	}, nil
}

func ReportToRPC(r Report) *repository.Report {
	res := repository.Report{
		Name:           r.Name,
		Title:          r.Title,
		CronExpression: r.CronExpression,
		CronTimezone:   r.CronTimezone,
		GenerateSheet:  r.GenerateSheet,
		SlackChannels:  r.SlackChannels,
	}

	res.Queries = make([]*repository.ReportQuery, len(r.Queries))

	for i, q := range r.Queries {
		vp := ValueProcessingToRPC(q.ValueProcessing)

		rq := repository.ReportQuery{
			Name:            q.Name,
			Sql:             q.SQL,
			ValueProcessing: vp,
			Summarise:       intSliceToInt32(q.Summarise),
		}

		res.Queries[i] = &rq
	}

	return &res
}

func ReportFromRPC(r *repository.Report) (Report, error) {
	res := Report{
		Name:           r.Name,
		Title:          r.Title,
		CronExpression: r.CronExpression,
		CronTimezone:   r.CronTimezone,
		GenerateSheet:  r.GenerateSheet,
		SlackChannels:  r.SlackChannels,
	}

	res.Queries = make([]ReportQuery, len(r.Queries))

	for i, q := range r.Queries {
		vp, err := ValueProcessingFromRPC(q.ValueProcessing)
		if err != nil {
			return Report{}, err
		}

		rq := ReportQuery{
			Name:            q.Name,
			SQL:             q.Sql,
			ValueProcessing: vp,
			Summarise:       int32SliceToInt(q.Summarise),
		}

		res.Queries[i] = rq
	}

	return res, nil
}

// Test a report. This will run the report and return the results instead of
// sending it to any outputs.
func (s *ReportsService) Run(
	ctx context.Context, req *repository.RunReportRequest,
) (*repository.RunReportResponse, error) {
	err := requireAnyScope(ctx, "report_admin", "report_run")
	if err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	res, err := s.store.GetReport(ctx, req.Name)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.NotFound.Error(err.Error())
	} else if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to load report: %w", err)
	}

	report, err := GenerateReport(ctx, s.logger, res.Report, s.reportingDB)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to generate report: %w", err)
	}

	response := repository.RunReportResponse{
		Tables: report.Tables,
	}

	if report.Spreadsheet != nil {
		response.Spreadsheet = report.Spreadsheet.Bytes()
	}

	return &response, nil
}

func (s *ReportsService) Test(
	ctx context.Context, req *repository.TestReportRequest,
) (*repository.TestReportResponse, error) {
	err := requireAnyScope(ctx, "report_admin")
	if err != nil {
		return nil, err
	}

	res, err := ReportFromRPC(req.Report)
	if err != nil {
		return nil, twirp.InvalidArgumentError("report", err.Error())
	}

	report, err := GenerateReport(ctx, s.logger, res, s.reportingDB)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to generate report: %w", err)
	}

	response := repository.TestReportResponse{
		Tables: report.Tables,
	}

	if report.Spreadsheet != nil {
		response.Spreadsheet = report.Spreadsheet.Bytes()
	}

	return &response, nil
}

func ValueProcessingFromRPC(
	r []*repository.ReportValue,
) (map[string][]ReportValueProcess, error) {
	res := make(map[string][]ReportValueProcess)

	for _, rv := range r {
		proc := make([]ReportValueProcess, len(rv.Processors))

		for i := range proc {
			switch rv.Processors[i] {
			case string(ReportHTMLDecode):
				proc[i] = ReportHTMLDecode
			default:
				return nil, fmt.Errorf(
					"unknown report value processor %q",
					rv.Processors[i])
			}
		}

		res[rv.Column] = proc
	}

	return res, nil
}

func ValueProcessingToRPC(
	v map[string][]ReportValueProcess,
) []*repository.ReportValue {
	var res []*repository.ReportValue

	for col, proc := range v {
		rvp := make([]string, len(proc))

		for i := range proc {
			rvp[i] = string(proc[i])
		}

		res = append(res, &repository.ReportValue{
			Column:     col,
			Processors: rvp,
		})
	}

	return res
}

// Converts an int32 slice to int, normalises zero length slices to nil.
func int32SliceToInt(s []int32) []int {
	if len(s) == 0 {
		return nil
	}

	res := make([]int, len(s))

	for i := range s {
		res[i] = int(s[i])
	}

	return res
}

// Converts an int slice to int32, normalises zero length slices to nil.
func intSliceToInt32(s []int) []int32 {
	if len(s) == 0 {
		return nil
	}

	res := make([]int32, len(s))

	for i := range s {
		res[i] = int32(s[i])
	}

	return res
}
