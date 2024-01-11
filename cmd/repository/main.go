package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-repository/internal/cmd"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephant-repository/sinks"
	"github.com/ttab/elephantine"
	"github.com/ttab/langos"
	"github.com/ttab/revisor"
	"github.com/twitchtv/twirp"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

func main() {
	runCmd := cli.Command{
		Name:        "run",
		Description: "Runs the repository server",
		Action:      runServer,
		Flags: append([]cli.Flag{
			&cli.StringFlag{
				Name:    "addr",
				Value:   ":1080",
				EnvVars: []string{"LISTEN_ADDR"},
			},
			&cli.StringFlag{
				Name:    "profile-addr",
				Value:   ":1081",
				EnvVars: []string{"PROFILE_ADDR"},
			},
			&cli.StringFlag{
				Name:    "log-level",
				EnvVars: []string{"LOG_LEVEL"},
				Value:   "error",
			},
			&cli.StringFlag{
				Name:    "default-language",
				EnvVars: []string{"DEFAULT_LANGUAGE"},
				Value:   "sv-se",
			},
			&cli.StringSliceFlag{
				Name:    "ensure-schema",
				EnvVars: []string{"ENSURE_SCHEMA"},
			},
		}, cmd.BackendFlags()...),
	}

	app := cli.App{
		Name:  "repository",
		Usage: "The Elephant repository",
		Commands: []*cli.Command{
			&runCmd,
		},
	}

	if err := app.Run(os.Args); err != nil {
		slog.Error("failed to run server",
			elephantine.LogKeyError, err)
		os.Exit(1)
	}
}

func runServer(c *cli.Context) error {
	var (
		addr            = c.String("addr")
		profileAddr     = c.String("profile-addr")
		logLevel        = c.String("log-level")
		ensureSchemas   = c.StringSlice("ensure-schema")
		defaultLanguage = c.String("default-language")
	)

	logger := elephantine.SetUpLogger(logLevel, os.Stdout)
	grace := elephantine.NewGracefulShutdown(logger, 20*time.Second)
	paramSource := elephantine.NewLazySSM()

	_, err := langos.GetLanguage(defaultLanguage)
	if err != nil {
		return fmt.Errorf("invalid default language: %w", err)
	}

	conf, err := cmd.BackendConfigFromContext(c, paramSource.GetParameterValue)
	if err != nil {
		return fmt.Errorf("failed to read configuration: %w", err)
	}

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

	instrument, err := elephantine.NewHTTPClientIntrumentation(prometheus.DefaultRegisterer)
	if err != nil {
		return fmt.Errorf(
			"failed to set up HTTP client instrumentation: %w", err)
	}

	s3Conf := conf.S3Options

	s3Conf.HTTPClient = &http.Client{
		Timeout: 1 * time.Minute,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	err = instrument.Client("s3", s3Conf.HTTPClient)
	if err != nil {
		return fmt.Errorf(
			"failed to instrument S3 HTTP client: %w", err)
	}

	s3Client, err := repository.S3Client(c.Context, conf.S3Options)
	if err != nil {
		return fmt.Errorf(
			"failed to create S3 client: %w", err)
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
	go store.RunCleaner(c.Context, 5*time.Minute)

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

	if !conf.NoCoreSchema {
		err = repository.EnsureCoreSchema(c.Context, store)
		if err != nil {
			return fmt.Errorf(
				"failed to ensure core schema: %w", err)
		}
	}

	// Spec format name@version:URL
	for _, spec := range ensureSchemas {
		reference, rawURL, ok := strings.Cut(spec, ":")
		if !ok {
			return errors.New("expected a specification in the format name@version:URL")
		}

		name, version, ok := strings.Cut(reference, "@")
		if !ok {
			return errors.New("expected a specification in the format name@version:URL")
		}

		uri, err := url.Parse(rawURL)
		if err != nil {
			return fmt.Errorf("invalid URL for the schema %s: %w", name, err)
		}

		var schema revisor.ConstraintSet

		switch uri.Scheme {
		case "file":
			err := elephantine.UnmarshalFile(uri.Opaque, &schema)
			if err != nil {
				return fmt.Errorf("failed to load the schema file for %s: %w",
					name, err)
			}
		case "http", "https":
			err := elephantine.UnmarshalHTTPResource(rawURL, &schema)
			if err != nil {
				return fmt.Errorf("failed to load the schema %s over HTTP(S): %w",
					name, err)
			}
		default:
			return fmt.Errorf("unknown schema URL scheme %q", uri.Scheme)
		}

		err = repository.EnsureSchema(c.Context, store, name, version, schema)
		if err != nil {
			return fmt.Errorf(
				"failed to ensure %s schema: %w", name, err)
		}
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

	docService := repository.NewDocumentsService(
		store, validator, workflows, defaultLanguage,
	)

	setupCtx, cancel := context.WithTimeout(c.Context, 10*time.Second)
	defer cancel()

	group, gCtx := errgroup.WithContext(setupCtx)

	if !conf.NoReplicator {
		group.Go(func() error {
			log := logger.With(elephantine.LogKeyComponent, "replicator")

			log.Debug("setting up replication")

			repl, err := repository.NewPGReplication(
				log, dbpool, conf.DB, "eventlogslot",
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

			go func() {
				<-grace.ShouldStop()
				repl.Stop()
				logger.Info("stopped replication")
			}()

			return nil
		})
	}

	if !conf.NoReporter {
		group.Go(func() error {
			reporter, err := repository.NewReportRunner(repository.ReportRunnerOptions{
				Logger:            logger,
				S3:                s3Client,
				Bucket:            conf.ReportBucket,
				ReportQueryer:     reportDB,
				DB:                dbpool,
				MetricsRegisterer: prometheus.DefaultRegisterer,
			})
			if err != nil {
				return fmt.Errorf("failed to create report runner: %w", err)
			}

			go reporter.Run(c.Context)

			go func() {
				<-grace.ShouldStop()
				reporter.Stop()
				logger.Info("stopped reporter")
			}()

			return nil
		})
	}

	if !conf.NoArchiver {
		log := logger.With(elephantine.LogKeyComponent, "archiver")

		logger.Debug("starting archiver")

		group.Go(func() error {
			archiver, err := startArchiver(c.Context, gCtx,
				log, conf, dbpool)
			if err != nil {
				return err
			}

			go func() {
				<-grace.ShouldStop()
				archiver.Stop()
				logger.Info("stopped archiver")
			}()

			return nil
		})
	}

	if !conf.NoEventsink && conf.Eventsink != "" {
		var sink sinks.EventSink

		switch conf.Eventsink {
		case "aws-eventbridge":
			conf, err := config.LoadDefaultConfig(c.Context)
			if err != nil {
				return fmt.Errorf("failed to load AWS SDK config for Eventbridge: %w", err)
			}

			client := eventbridge.NewFromConfig(conf)

			sink = sinks.NewEventBridge(client, sinks.EventBridgeOptions{
				Logger: logger.With(elephantine.LogKeyComponent, "eventsink"),
			})

			q := postgres.New(dbpool)

			err = q.ConfigureEventsink(c.Context, postgres.ConfigureEventsinkParams{
				Name: sink.SinkName(),
			})
			if err != nil {
				return fmt.Errorf("failed to configure eventsink %q: %w",
					sink.SinkName(), err)
			}
		default:
			return fmt.Errorf("unknown event sink %q", conf.Eventsink)
		}

		forwarder, err := sinks.NewEventForwarder(sinks.EventForwarderOptions{
			Logger:            logger.With(elephantine.LogKeyComponent, "event-forwarder"),
			DB:                dbpool,
			Documents:         docService,
			MetricsRegisterer: prometheus.DefaultRegisterer,
			Sink:              sink,
			StateStore:        store,
		})
		if err != nil {
			return fmt.Errorf(
				"failed to create eventsink forwarder: %w", err)
		}

		go forwarder.Run(c.Context)

		go func() {
			<-grace.ShouldStop()
			forwarder.Stop()
			logger.Info("stopped eventsink")
		}()
	}

	err = group.Wait()
	if err != nil {
		return fmt.Errorf("subsystem setup failed: %w", err)
	}

	schemaService := repository.NewSchemasService(logger, store)
	workflowService := repository.NewWorkflowsService(store)
	reportsService := repository.NewReportsService(logger, store, reportDB)
	metricsService := repository.NewMetricsService(store)

	router := httprouter.New()

	var opts repository.ServerOptions

	opts.SetJWTValidation(signingKey)

	metrics, err := elephantine.NewTwirpMetricsHooks()
	if err != nil {
		return fmt.Errorf("failed to create twirp metrics hook: %w", err)
	}

	opts.Hooks = twirp.ChainHooks(
		elephantine.LoggingHooks(logger, func(ctx context.Context) string {
			auth, ok := elephantine.GetAuthInfo(ctx)
			if !ok {
				return ""
			}

			return auth.Claims.Scope
		}),
		metrics,
	)

	sse, err := repository.NewSSE(setupCtx, logger.With(
		elephantine.LogKeyComponent, "sse",
	), store)
	if err != nil {
		return fmt.Errorf("failed to set up SSE server: %w", err)
	}

	err = repository.SetUpRouter(router,
		repository.WithTokenEndpoint(signingKey, conf.SharedSecret),
		repository.WithJWKSEndpoint(signingKey),
		repository.WithDocumentsAPI(docService, opts),
		repository.WithSchemasAPI(schemaService, opts),
		repository.WithWorkflowsAPI(workflowService, opts),
		repository.WithReportsAPI(reportsService, opts),
		repository.WithMetricsAPI(metricsService, opts),
		repository.WithSSE(sse.HTTPHandler(), opts),
	)
	if err != nil {
		return fmt.Errorf("failed to set up router: %w", err)
	}

	healthServer := elephantine.NewHealthServer(profileAddr)

	healthServer.AddReadyFunction("s3", func(ctx context.Context) error {
		testUUID := uuid.New()

		key := fmt.Sprintf(
			"test/ready-probe-%s.txt", testUUID,
		)

		_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(conf.ArchiveBucket),
			Key:         aws.String(key),
			ContentType: aws.String("text/plain"),
			Body:        strings.NewReader("Ready probe healthcheck"),
		})
		if err != nil {
			return fmt.Errorf("failed to write to archive bucket: %w", err)
		}

		res, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(conf.ArchiveBucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return fmt.Errorf("failed to read from archive bucket: %w", err)
		}

		_ = res.Body.Close()

		_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(conf.ArchiveBucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return fmt.Errorf("failed to delete from archive bucket: %w", err)
		}

		return nil
	})

	healthServer.AddReadyFunction("postgres", func(ctx context.Context) error {
		q := postgres.New(dbpool)

		_, err := q.GetActiveSchemas(ctx)
		if err != nil {
			return fmt.Errorf("failed to read schemas: %w", err)
		}

		return nil
	})

	router.GET("/health/alive", func(
		w http.ResponseWriter, req *http.Request, _ httprouter.Params,
	) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)

		_, _ = fmt.Fprintln(w, "I AM ALIVE!")
	})

	healthServer.AddReadyFunction("api_liveness", func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(
			ctx, http.MethodGet, fmt.Sprintf(
				"http://localhost%s/health/alive",
				addr,
			), nil,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to create liveness check request: %w", err)
		}

		var client http.Client

		res, err := client.Do(req)
		if err != nil {
			return fmt.Errorf(
				"failed to perform liveness check request: %w", err)
		}

		_ = res.Body.Close()

		if res.StatusCode != http.StatusOK {
			return fmt.Errorf(
				"api liveness endpoint returned non-ok status:: %s",
				res.Status)
		}

		return nil
	})

	serverGroup, gCtx := errgroup.WithContext(grace.CancelOnQuit(c.Context))

	serverGroup.Go(func() error {
		logger.Debug("starting API server")

		err := repository.ListenAndServe(gCtx, addr, router)
		if err != nil {
			return fmt.Errorf("API server error: %w", err)
		}

		return nil
	})

	serverGroup.Go(func() error {
		logger.Debug("starting SSE server")

		go func() {
			<-grace.ShouldStop()
			sse.Stop()
		}()

		sse.Run(gCtx)

		return nil
	})

	serverGroup.Go(func() error {
		logger.Debug("starting health server")

		err := healthServer.ListenAndServe(gCtx)
		if err != nil {
			return fmt.Errorf("health server error: %w", err)
		}

		return nil
	})

	err = serverGroup.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	} else if err != nil {
		return fmt.Errorf("server failed to start: %w", err)
	}

	return nil
}

func startArchiver(
	ctx context.Context, setupCtx context.Context, logger *slog.Logger,
	conf cmd.BackendConfig, dbpool *pgxpool.Pool,
) (*repository.Archiver, error) {
	aS3, err := repository.S3Client(setupCtx, conf.S3Options)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	archiver, err := repository.NewArchiver(repository.ArchiverOptions{
		Logger: logger,
		S3:     aS3,
		Bucket: conf.ArchiveBucket,
		DB:     dbpool,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create archiver: %w", err)
	}

	err = archiver.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run archiver: %w", err)
	}

	return archiver, nil
}
