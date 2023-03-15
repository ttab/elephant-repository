package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"errors"
	_ "expvar" // Register the expvar handlers
	"fmt"
	"net/http"
	_ "net/http/pprof" //nolint:gosec
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ttab/elephant/internal"
	"github.com/ttab/elephant/internal/cmd"
	"github.com/ttab/elephant/repository"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"
)

func main() {
	runCmd := cli.Command{
		Name:        "run",
		Description: "Runs the repository server",
		Action:      runServer,
		Flags: append([]cli.Flag{
			&cli.StringFlag{
				Name:  "addr",
				Value: ":1080",
			},
			&cli.StringFlag{
				Name:  "profile-addr",
				Value: ":1081",
			},
			&cli.StringFlag{
				Name:    "log-level",
				EnvVars: []string{"LOG_LEVEL"},
				Value:   "error",
			},
		}, cmd.BackendFlags()...),
	}

	var app = cli.App{
		Name:  "repository",
		Usage: "The Elephant repository",
		Commands: []*cli.Command{
			&runCmd,
		},
	}

	if err := app.Run(os.Args); err != nil {
		slog.Error("failed to run server", err)
		os.Exit(1)
	}
}

func runServer(c *cli.Context) error {
	var (
		addr        = c.String("addr")
		profileAddr = c.String("profile-addr")
		logLevel    = c.String("log-level")
		conf        = cmd.BackendConfigFromContext(c)
	)

	logger := internal.SetUpLogger(logLevel, os.Stdout)

	var signingKey *ecdsa.PrivateKey

	if conf.JWTSigningKey != "" {
		keyData, err := base64.RawURLEncoding.DecodeString(
			conf.JWTSigningKey)
		if err != nil {
			return fmt.Errorf(
				"invalid base64 encoding for JWT signing key: %w", err)
		}

		k, err := x509.ParseECPrivateKey(keyData)
		if err != nil {
			return fmt.Errorf(
				"invalid JWT signing key: %w", err)
		}

		signingKey = k
	} else {
		logger.Warn("no configured signing key")

		key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		if err != nil {
			return fmt.Errorf("failed to generate key: %w", err)
		}

		signingKey = key
	}

	dbpool, err := pgxpool.New(c.Context, conf.DB)
	if err != nil {
		return fmt.Errorf("unable to create connection pool: %w", err)
	}

	defer func() {
		// Don't block for close
		go dbpool.Close()
	}()

	err = dbpool.Ping(c.Context)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	store, err := repository.NewPGDocStore(
		logger, dbpool, repository.PGDocStoreOptions{})
	if err != nil {
		return fmt.Errorf("failed to create doc store: %w", err)
	}

	go store.RunListener(c.Context)

	reportDB, err := pgxpool.New(c.Context, conf.ReportingDB)
	if err != nil {
		return fmt.Errorf(
			"unable to create reporting connection pool: %w", err)
	}

	defer func() {
		// Don't block for close
		go reportDB.Close()
	}()

	err = reportDB.Ping(c.Context)
	if err != nil {
		return fmt.Errorf(
			"failed to connect to reporting database: %w", err)
	}

	setupCtx, cancel := context.WithTimeout(c.Context, 10*time.Second)
	defer cancel()

	group, gCtx := errgroup.WithContext(setupCtx)

	if !conf.NoReplicator {
		group.Go(func() error {
			log := logger.With(internal.LogKeyComponent, "replicator")

			log.Debug("setting up replication")

			repl, err := repository.NewPGReplication(
				log, dbpool, conf.DB,
				prometheus.DefaultRegisterer)
			if err != nil {
				return fmt.Errorf(
					"failed to create replicator: %w", err)
			}

			// TODO: Inconsistent Run functions for subsystems. The
			// reporter and archivers don't block. Though maybe it's
			// the better behaviour to actually block so that it
			// becomes obvious at the callsite that a goroutine is
			// spawned.
			go repl.Run(c.Context)

			return nil
		})
	}

	if !conf.NoReporter {
		rS3, err := repository.ArchiveS3Client(setupCtx, conf.S3Options)
		if err != nil {
			return fmt.Errorf(
				"failed to create S3 client for reporter: %w", err)
		}

		reporter, err := repository.NewReportRunner(repository.ReportRunnerOptions{
			Logger: logger,
			S3:     rS3,
			// TODO: separate bucket.
			Bucket:            conf.ArchiveBucket,
			ReportQueryer:     reportDB,
			DB:                dbpool,
			MetricsRegisterer: prometheus.DefaultRegisterer,
		})
		if err != nil {
			return fmt.Errorf("failed to create report runner: %w", err)
		}

		reporter.Run(c.Context)
	}

	if !conf.NoArchiver {
		log := logger.With(internal.LogKeyComponent, "archiver")

		logger.Debug("starting archiver")

		group.Go(func() error {
			return startArchiver(c.Context, gCtx,
				log, conf, dbpool)
		})
	}

	err = group.Wait()
	if err != nil {
		return fmt.Errorf("subsystem setup failed: %w", err)
	}

	validator, err := repository.NewValidator(
		c.Context, logger, store)
	if err != nil {
		return fmt.Errorf("failed to create validator: %w", err)
	}

	workflows, err := repository.NewWorkflows(c.Context, logger, store)
	if err != nil {
		return fmt.Errorf("failed to create workflows: %w", err)
	}

	docService := repository.NewDocumentsService(store, validator, workflows)
	schemaService := repository.NewSchemasService(store)
	workflowService := repository.NewWorkflowsService(store)
	reportsService := repository.NewReportsService(logger, store, reportDB)

	logger.Debug("starting API server")

	go func() {
		mux := http.DefaultServeMux

		mux.Handle("/metrics", promhttp.Handler())

		profileServer := http.Server{
			Addr:              profileAddr,
			Handler:           http.DefaultServeMux,
			ReadHeaderTimeout: 1 * time.Second,
		}

		go func() {
			<-c.Context.Done()
			profileServer.Close()
		}()

		err := profileServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			logger.Debug("profile server closed")
		} else if err != nil {
			logger.Error("failed to start profile server", err)
		}
	}()

	router := httprouter.New()

	var opts repository.ServerOptions

	opts.SetJWTValidation(signingKey)

	metrics, err := internal.NewTwirpMetricsHooks()
	if err != nil {
		return fmt.Errorf("failed to create twirp metrics hook: %w", err)
	}

	opts.Hooks = metrics

	err = repository.SetUpRouter(router,
		repository.WithTokenEndpoint(logger, signingKey, conf.SharedSecret),
		repository.WithDocumentsAPI(logger, docService, opts),
		repository.WithSchemasAPI(logger, schemaService, opts),
		repository.WithWorkflowsAPI(logger, workflowService, opts),
		repository.WithReportsAPI(logger, reportsService, opts),
	)
	if err != nil {
		return fmt.Errorf("failed to set up router: %w", err)
	}

	err = repository.ListenAndServe(c.Context, addr, router)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	return nil
}

func startArchiver(
	ctx context.Context, setupCtx context.Context, logger *slog.Logger,
	conf cmd.BackendConfig, dbpool *pgxpool.Pool,
) error {
	aS3, err := repository.ArchiveS3Client(setupCtx, conf.S3Options)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	archiver, err := repository.NewArchiver(repository.ArchiverOptions{
		Logger: logger,
		S3:     aS3,
		Bucket: conf.ArchiveBucket,
		DB:     dbpool,
	})
	if err != nil {
		return fmt.Errorf("failed to create archiver: %w", err)
	}

	err = archiver.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run archiver: %w", err)
	}

	return nil
}
