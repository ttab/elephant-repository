package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
)

type ReportRunnerOptions struct {
	Logger *slog.Logger
	S3     *s3.Client
	Bucket string
	// ReportQueryer should be a read-only connection to the database with
	// access to the tables `document`, `delete_record`, `document_version`,
	// `document_status`, `status_heads`, `acl`, `acl_audit`.
	ReportQueryer Queryer
	// DB should be a normal database connection with full repository
	// access.
	DB                *pgxpool.Pool
	MetricsRegisterer prometheus.Registerer
}

type ReportRunner struct {
	logger  *slog.Logger
	s3      *s3.Client
	bucket  string
	queryer Queryer
	pool    *pgxpool.Pool

	restarts prometheus.Counter
	duration *prometheus.HistogramVec

	cancel  func()
	stopped chan struct{}
}

func NewReportRunner(opts ReportRunnerOptions) (*ReportRunner, error) {
	if opts.MetricsRegisterer == nil {
		opts.MetricsRegisterer = prometheus.DefaultRegisterer
	}

	restarts := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "elephant_reporter_restarts_total",
			Help: "Number of times the reporter has restarted.",
		},
	)
	if err := opts.MetricsRegisterer.Register(restarts); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "elephant_report_duration_seconds",
		Help:    "Duration for generating a report.",
		Buckets: prometheus.ExponentialBuckets(0.100, 2, 11),
	}, []string{"name"})
	if err := opts.MetricsRegisterer.Register(duration); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	return &ReportRunner{
		logger:   opts.Logger,
		s3:       opts.S3,
		bucket:   opts.Bucket,
		queryer:  opts.ReportQueryer,
		pool:     opts.DB,
		restarts: restarts,
		duration: duration,
	}, nil
}

func (r *ReportRunner) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)

	r.cancel = cancel
	r.stopped = make(chan struct{})

	r.run(ctx)
}

func (r *ReportRunner) Stop() {
	r.cancel()

	<-r.stopped
}

func (r *ReportRunner) run(ctx context.Context) {
	const restartWaitSeconds = 10

	defer close(r.stopped)

	for {
		r.logger.Debug("starting reporter")

		err := r.loop(ctx)
		if errors.Is(err, context.Canceled) {
			return
		} else if err != nil {
			r.restarts.Inc()

			r.logger.ErrorContext(
				ctx, "reporter error, restarting",
				elephantine.LogKeyError, err,
				elephantine.LogKeyDelay, slog.DurationValue(restartWaitSeconds),
			)
		}

		select {
		case <-time.After(restartWaitSeconds * time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func (r *ReportRunner) loop(ctx context.Context) error {
	maxWait := 5 * time.Minute

	for {
		err := r.runNext(ctx)
		if err != nil {
			return err
		}

		next, err := postgres.New(r.pool).GetNextReportDueTime(ctx)
		if err != nil {
			return fmt.Errorf(
				"failed to get next due time: %w", err)
		}

		wait := maxWait
		if next.Valid && time.Until(next.Time) < maxWait {
			wait = time.Until(next.Time)
		}

		r.logger.Debug("waiting for next report run",
			elephantine.LogKeyDelay, wait.Seconds())

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil
		}
	}
}

func (r *ReportRunner) runNext(ctx context.Context) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf(
			"failed to begin transaction: %w", err)
	}

	defer pg.SafeRollback(ctx, r.logger, tx,
		"report run")

	q := postgres.New(tx)

	row, err := q.GetDueReport(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to get due report: %w", err)
	}

	var report Report

	err = json.Unmarshal(row.Spec, &report)
	if err != nil {
		return fmt.Errorf("failed to unmarshal stored report: %w", err)
	}

	start := time.Now()

	nextExecution, err := report.NextTick()
	if err != nil {
		return fmt.Errorf("failed to calculate next execution: %w", err)
	}

	result, err := GenerateReport(ctx, r.logger, report, r.queryer)
	if err != nil {
		return fmt.Errorf("failed to generate report: %w", err)
	}

	r.duration.WithLabelValues(
		report.Name,
	).Observe(time.Since(start).Seconds())

	obj := ReportObject{
		Specification: report,
		Tables:        result.Tables,
		Created:       start,
	}

	objBody, err := json.MarshalIndent(&obj, "", "  ")
	if err != nil {
		return fmt.Errorf(
			"failed to marshal result object: %w", err)
	}

	if result.Spreadsheet != nil {
		_, err = r.s3.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(r.bucket),
			Key: aws.String(fmt.Sprintf(
				"reports/%s/%s/result.xlsx",
				report.Name, start.Format(time.RFC3339))),
			Body: result.Spreadsheet,
		})
		if err != nil {
			return fmt.Errorf(
				"failed to store result spreadsheet: %w", err)
		}
	}

	_, err = r.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.bucket),
		Key: aws.String(fmt.Sprintf(
			"reports/%s/%s/result.json",
			report.Name, start.Format(time.RFC3339))),
		Body: bytes.NewReader(objBody),
	})
	if err != nil {
		return fmt.Errorf(
			"failed to store result manifest: %w", err)
	}

	err = q.SetNextReportExecution(ctx, postgres.SetNextReportExecutionParams{
		Name:          report.Name,
		NextExecution: pg.Time(nextExecution),
	})
	if err != nil {
		return fmt.Errorf(
			"failed to set next execution time for report: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf(
			"failed to commit transaction: %w", err)
	}

	return nil
}
