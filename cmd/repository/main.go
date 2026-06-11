package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-repository/internal"
	"github.com/ttab/elephant-repository/internal/cmd"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephant-repository/schema"
	"github.com/ttab/elephant-repository/sinks"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/langos"
	"github.com/twitchtv/twirp"
	"github.com/urfave/cli/v3"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

var version string // set via -ldflags at build time

func main() {
	err := godotenv.Load()
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.Error("load .env file",
			elephantine.LogKeyError, err)
		os.Exit(1)
	}

	runCmd := cli.Command{
		Name:        "run",
		Description: "Runs the repository server",
		Action:      runServer,
		Flags: append([]cli.Flag{
			&cli.StringFlag{
				Name:    "addr",
				Value:   ":1080",
				Sources: cli.EnvVars("ADDR", "LISTEN_ADDR"),
			},
			&cli.StringFlag{
				Name:    "tls-addr",
				Value:   ":1443",
				Sources: cli.EnvVars("TLS_ADDR", "TLS_LISTEN_ADDR"),
			},
			&cli.StringFlag{
				Name:    "cert-file",
				Sources: cli.EnvVars("TLS_CERT_PATH"),
			},
			&cli.StringFlag{
				Name:    "key-file",
				Sources: cli.EnvVars("TLS_KEY_PATH"),
			},
			&cli.StringFlag{
				Name:    "profile-addr",
				Value:   ":1081",
				Sources: cli.EnvVars("PROFILE_ADDR"),
			},
			&cli.StringFlag{
				Name:    "log-level",
				Sources: cli.EnvVars("LOG_LEVEL"),
				Value:   "error",
			},
			&cli.StringFlag{
				Name:    "default-language",
				Sources: cli.EnvVars("DEFAULT_LANGUAGE"),
				Value:   "sv-se",
			},
			&cli.StringFlag{
				Name:    "default-timezone",
				Sources: cli.EnvVars("DEFAULT_TIMEZONE"),
				Value:   "Europe/Stockholm",
			},
			//nolint:gosec // G101: Development default, not a real credential.
			&cli.StringFlag{
				Name:    "db",
				Value:   "postgres://elephant-repository:pass@localhost/elephant-repository",
				Sources: cli.EnvVars("CONN_STRING"),
			},
			&cli.StringFlag{
				Name:    "db-bouncer",
				Usage:   "Connection string routed through PgBouncer, used for all DB operations except pubsub",
				Sources: cli.EnvVars("BOUNCER_CONN_STRING"),
			},
			&cli.StringFlag{
				Name:    "db-parameter",
				Sources: cli.EnvVars("CONN_STRING_PARAMETER"),
			},
			&cli.StringFlag{
				Name:    "eventsink",
				Value:   "aws-eventbridge",
				Sources: cli.EnvVars("EVENTSINK"),
			},
			&cli.StringFlag{
				Name:    "archive-bucket",
				Value:   "elephant-archive",
				Sources: cli.EnvVars("ARCHIVE_BUCKET"),
			},
			&cli.StringFlag{
				Name:    "asset-bucket",
				Value:   "elephant-assets",
				Sources: cli.EnvVars("ASSET_BUCKET"),
			},
			&cli.StringFlag{
				Name:    "s3-endpoint",
				Usage:   "Override the S3 endpoint for use with Minio",
				Sources: cli.EnvVars("S3_ENDPOINT"),
			},
			&cli.StringFlag{
				Name:    "s3-key-id",
				Usage:   "Access key ID to use as a static credential with Minio",
				Sources: cli.EnvVars("S3_ACCESS_KEY_ID"),
			},
			&cli.StringFlag{
				Name:    "s3-key-secret",
				Usage:   "Access key secret to use as a static credential with Minio",
				Sources: cli.EnvVars("S3_ACCESS_KEY_SECRET"),
			},
			&cli.BoolFlag{
				Name:    "tolerate-eventlog-gaps",
				Usage:   "Tolerate eventlog gaps when archiving",
				Sources: cli.EnvVars("TOLERATE_EVENTLOG_GAPS"),
			},
			&cli.BoolFlag{
				Name:    "no-archiver",
				Usage:   "Disable the archiver",
				Sources: cli.EnvVars("NO_ARCHIVER"),
			},
			&cli.BoolFlag{
				Name:    "no-eventsink",
				Usage:   "Disable the eventsink",
				Sources: cli.EnvVars("NO_EVENTSINK"),
			},
			&cli.BoolFlag{
				Name:    "no-eventlog-builder",
				Usage:   "Disable the eventlog builder",
				Aliases: []string{"no-replicator"},
				Sources: cli.EnvVars("NO_EVENTLOG_BUILDER"),
			},
			&cli.BoolFlag{
				Name:    "no-scheduler",
				Usage:   "Disable scheduled publishing",
				Sources: cli.EnvVars("NO_SCHEDULER"),
			},
			&cli.BoolFlag{
				Name:    "no-charcounter",
				Usage:   "Disable built in character counter",
				Sources: cli.EnvVars("NO_CHARCOUNTER"),
			},
			&cli.BoolFlag{
				Name:    "no-websocket",
				Usage:   "Disable websocket API",
				Sources: cli.EnvVars("NO_WEBSOCKET"),
			},
			&cli.BoolFlag{
				Name:    "no-sse",
				Usage:   "Disable SSE API",
				Sources: cli.EnvVars("NO_SSE"),
			},
			&cli.StringSliceFlag{
				Name:    "cors-host",
				Usage:   "CORS hosts to allow, supports wildcards",
				Sources: cli.EnvVars("CORS_HOSTS"),
			},
			&cli.IntFlag{
				Name:    "eventlog-buffer-size",
				Value:   repository.DefaultEventlogBufferSize,
				Usage:   "Number of recent eventlog events buffered for socket resume",
				Sources: cli.EnvVars("EVENTLOG_BUFFER_SIZE"),
			},
			&cli.IntFlag{
				Name:    "eventlog-stream-burst",
				Value:   repository.DefaultEventlogStreamBurst,
				Usage:   "Token-bucket burst for an eventlog subscription stream",
				Sources: cli.EnvVars("EVENTLOG_STREAM_BURST"),
			},
			&cli.FloatFlag{
				Name:    "eventlog-stream-rate",
				Value:   float64(repository.DefaultEventlogStreamRate),
				Usage:   "Token-bucket rate (events/sec) for an eventlog subscription stream",
				Sources: cli.EnvVars("EVENTLOG_STREAM_RATE"),
			},
			&cli.BoolFlag{
				Name: "migrate-db",
				Usage: `Perform database migrations.
Intended for bootstrapping disposable environments. Having this always on in
production is a BAD IDEA! Migrations can be expensive and need to be planned.`,
				Sources: cli.EnvVars("MIGRATE_DB"),
			},
			&cli.BoolFlag{
				Name: "emit-workflow-event",
				Usage: `Emit the legacy standalone "workflow" event alongside the
workflow_state fields that are folded onto the triggering document or status
event. Transition aid for external consumers; will be removed in a future
release.`,
				Sources: cli.EnvVars("EMIT_WORKFLOW_EVENT"),
			},
			&cli.BoolFlag{
				Name: "emit-acl-event",
				Usage: `Emit the legacy standalone "acl" event alongside the acl
field that is folded onto the triggering document event. The folded field is
always present; this flag only adds the extra event back. Transition aid for
external consumers; will be removed in a future release.`,
				Sources: cli.EnvVars("EMIT_ACL_EVENT"),
			},
		}, elephantine.AuthenticationCLIFlags()...),
	}

	app := cli.Command{
		Name:  "repository",
		Usage: "The Elephant repository",
		Commands: []*cli.Command{
			&runCmd,
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		slog.Error("failed to run server",
			elephantine.LogKeyError, err)
		os.Exit(1)
	}
}

func runServer(ctx context.Context, c *cli.Command) error {
	var (
		addr              = c.String("addr")
		tlsAddr           = c.String("tls-addr")
		certFile          = c.String("cert-file")
		keyFile           = c.String("key-file")
		profileAddr       = c.String("profile-addr")
		logLevel          = c.String("log-level")
		defaultLanguage   = c.String("default-language")
		defaultTimezone   = c.String("default-timezone")
		noCharCounter     = c.Bool("no-charcounter")
		noWebsocket       = c.Bool("no-websocket")
		noSSE             = c.Bool("no-sse")
		corsHosts         = c.StringSlice("cors-host")
		eventlogBufSize   = c.Int("eventlog-buffer-size")
		eventlogBurst     = c.Int("eventlog-stream-burst")
		eventlogRate      = c.Float("eventlog-stream-rate")
		migrateDB         = c.Bool("migrate-db")
		emitWorkflowEvent = c.Bool("emit-workflow-event")
		emitACLEvent      = c.Bool("emit-acl-event")
	)

	logger := elephantine.SetUpLogger(logLevel, os.Stdout)
	grace := elephantine.NewGracefulShutdown(logger, 20*time.Second)

	stopCtx := grace.CancelOnStop(ctx)

	defer grace.Stop()

	_, err := langos.GetLanguage(defaultLanguage)
	if err != nil {
		return fmt.Errorf("invalid default language: %w", err)
	}

	defaultTZ, err := time.LoadLocation(defaultTimezone)
	if err != nil {
		return fmt.Errorf("invalid default timezone: %w", err)
	}

	conf, err := cmd.BackendConfigFromContext(c)
	if err != nil {
		return fmt.Errorf("failed to read configuration: %w", err)
	}

	auth, err := elephantine.AuthenticationConfigFromCLI(
		ctx, c, nil,
	)
	if err != nil {
		return fmt.Errorf("set up authentication: %w", err)
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

	s3Client, err := repository.S3Client(ctx, s3Conf)
	if err != nil {
		return fmt.Errorf(
			"failed to create S3 client: %w", err)
	}

	presignClient := s3.NewPresignClient(s3Client,
		s3.WithPresignExpires(15*time.Minute))

	dbpool, err := pgxpool.New(ctx, conf.DB)
	if err != nil {
		return fmt.Errorf("unable to create connection pool: %w", err)
	}

	defer func() {
		// Don't block for close.
		go dbpool.Close()
	}()

	err = dbpool.Ping(ctx)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}

	// The pubsub pool uses a direct connection to PostgreSQL, as
	// LISTEN/NOTIFY is not supported through PgBouncer. When no
	// separate bouncer connection string is configured we share a
	// single pool for both.
	pubsubPool := dbpool

	if conf.DBBouncer != conf.DB {
		dbpool, err = pgxpool.New(ctx, conf.DBBouncer)
		if err != nil {
			return fmt.Errorf("unable to create bouncer connection pool: %w", err)
		}

		defer func() {
			go dbpool.Close()
		}()

		err = dbpool.Ping(ctx)
		if err != nil {
			return fmt.Errorf("connect to bouncer database: %w", err)
		}
	}

	if migrateDB {
		logger.Info("migrating database schema")

		err = internal.Migrate(stopCtx, dbpool, schema.Migrations)
		if err != nil {
			return fmt.Errorf("migrate database: %w", err)
		}
	}

	assets := repository.NewAssetBucket(
		logger, presignClient,
		s3Client, conf.AssetBucket)

	var inMet []repository.MetricCalculator

	if !noCharCounter {
		inMet = append(inMet, repository.NewCharCounter())
	}

	typeConfs := repository.NewTypeConfigurations(logger, defaultTZ)

	store, err := repository.NewPGDocStore(
		stopCtx, logger, dbpool, assets,
		repository.PGDocStoreOptions{
			MetricsCalculators: inMet,
			TypeConfigurations: typeConfs,
			DefaultTZ:          defaultTZ,
			EmitWorkflowEvent:  emitWorkflowEvent,
			EmitACLEvent:       emitACLEvent,
		})
	if err != nil {
		return fmt.Errorf("failed to create doc store: %w", err)
	}

	go store.RunListener(stopCtx, pubsubPool)
	go store.RunCleaner(stopCtx, 5*time.Minute)

	bootstrapLock, err := pg.NewJobLock(
		dbpool, logger, "bootstrap-generation",
		pg.JobLockOptions{})
	if err != nil {
		return fmt.Errorf("create bootstrap generation lock: %w", err)
	}

	err = bootstrapLock.RunWithContext(ctx,
		func(ctx context.Context) error {
			return repository.BootstrapGeneration(ctx, store)
		})
	if err != nil {
		return fmt.Errorf("bootstrap schema generation: %w", err)
	}

	validator, err := repository.NewValidator(
		ctx, logger, store, prometheus.DefaultRegisterer)
	if err != nil {
		return fmt.Errorf("failed to create validator: %w", err)
	}

	workflows, err := repository.NewWorkflows(ctx, logger, store)
	if err != nil {
		return fmt.Errorf("failed to create workflows: %w", err)
	}

	docCache := repository.NewDocCache(store, 1000)

	socketKey, err := store.EnsureSocketKey(ctx)
	if err != nil {
		return fmt.Errorf("ensure socket key: %w", err)
	}

	docService, err := repository.NewDocumentsService(
		store,
		repository.NewSchedulePGStore(dbpool),
		validator,
		workflows,
		assets,
		defaultLanguage,
		typeConfs,
		docCache,
		socketKey,
	)
	if err != nil {
		return fmt.Errorf("create documents service: %w", err)
	}

	setupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	group, gCtx := errgroup.WithContext(setupCtx)

	if !conf.NoEventlogBuilder {
		ctx := grace.CancelOnStop(ctx)
		log := logger.With(elephantine.LogKeyComponent, "eventlog-builder")

		log.Debug("setting up eventlog builder")

		updates := make(chan int64, 1)

		store.OnEventOutbox(ctx, updates)

		builder, err := repository.NewEventlogBuilder(
			log, dbpool, prometheus.DefaultRegisterer, updates)
		if err != nil {
			return fmt.Errorf("set up eventlog builder: %w", err)
		}

		go func() {
			err := pg.RunInJobLock(ctx,
				dbpool, log,
				"eventlog-builder", "eventlog-builder",
				pg.JobLockOptions{},
				func(ctx context.Context) error {
					return builder.Run(ctx)
				})
			if err != nil {
				log.ErrorContext(ctx, "eventlog builder has stopped",
					elephantine.LogKeyError, err)
			}
		}()
	}

	if !conf.NoEventsink && conf.Eventsink != "" {
		var sink sinks.EventSink

		switch conf.Eventsink {
		case "aws-eventbridge":
			conf, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				return fmt.Errorf("failed to load AWS SDK config for Eventbridge: %w", err)
			}

			client := eventbridge.NewFromConfig(conf)

			sink = sinks.NewEventBridge(client, sinks.EventBridgeOptions{
				Logger: logger.With(elephantine.LogKeyComponent, "eventsink"),
			})

			q := postgres.New(dbpool)

			err = q.ConfigureEventsink(ctx, postgres.ConfigureEventsinkParams{
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

		go forwarder.Run(ctx)

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

	if !conf.NoScheduler {
		scheduler, err := repository.NewScheduler(
			logger,
			prometheus.DefaultRegisterer,
			repository.NewSchedulePGStore(dbpool),
			docService,
			[]string{"oc"})
		if err != nil {
			return fmt.Errorf("create publish scheduler: %w", err)
		}

		go func() {
			logger.Debug("starting scheduler")

			ctx := grace.CancelOnStop(ctx)

			err := scheduler.RunInJobLock(
				ctx, nil,
				func() (*pg.JobLock, error) {
					return pg.NewJobLock(
						dbpool, logger, "scheduler",
						pg.JobLockOptions{})
				})
			if err != nil {
				logger.Error(
					"scheduled document publishing disabled due to error",
					elephantine.LogKeyError, err)
			}
		}()
	}

	schemaService := repository.NewSchemasService(logger, store)
	workflowService := repository.NewWorkflowsService(store)
	metricsService := repository.NewMetricsService(store)

	router := httprouter.New()

	var opts repository.ServerOptions

	opts.SetJWTValidation(auth.AuthParser)

	metrics, err := elephantine.NewTwirpMetricsHooks()
	if err != nil {
		return fmt.Errorf("failed to create twirp metrics hook: %w", err)
	}

	opts.Hooks = twirp.ChainHooks(
		elephantine.LoggingHooks(logger),
		metrics,
	)

	routerOpts := []repository.RouterOption{
		repository.WithDocumentsAPI(docService, opts),
		repository.WithSchemasAPI(schemaService, opts),
		repository.WithWorkflowsAPI(workflowService, opts),
		repository.WithMetricsAPI(metricsService, opts),
		repository.WithSigningKeys(dbpool),
	}

	var sseSubsystem *repository.SSE

	if !noSSE {
		sse, err := repository.NewSSE(setupCtx, logger.With(
			elephantine.LogKeyComponent, "sse",
		), store)
		if err != nil {
			return fmt.Errorf("failed to set up SSE server: %w", err)
		}

		routerOpts = append(routerOpts,
			repository.WithSSE(sse.HTTPHandler(), opts))

		sseSubsystem = sse
	}

	if !noWebsocket {
		socket, err := repository.NewSocketHandler(
			grace.CancelOnQuit(ctx), logger, prometheus.DefaultRegisterer,
			store, docCache, auth.AuthParser, &socketKey.PublicKey,
			corsHosts,
			repository.EventlogStreamConfig{
				BufferSize: eventlogBufSize,
				Rate:       rate.Limit(eventlogRate),
				Burst:      eventlogBurst,
			},
		)
		if err != nil {
			return fmt.Errorf("set up socket handler: %w", err)
		}

		routerOpts = append(routerOpts,
			repository.WithWebsocket(socket))
	}

	err = repository.SetUpRouter(router, routerOpts...)
	if err != nil {
		return fmt.Errorf("failed to set up router: %w", err)
	}

	var serverOpts []elephantine.APIServerOption

	serverOpts = append(serverOpts,
		elephantine.APIServerVersion(version),
		elephantine.APIServerModules(
			"github.com/ttab/newsdoc",
			"github.com/ttab/revisor",
		))

	if certFile != "" {
		serverOpts = append(serverOpts,
			elephantine.APIServerTLS(tlsAddr, certFile, keyFile))
	}

	srv := elephantine.NewAPIServer(logger, addr, profileAddr, serverOpts...)

	if len(corsHosts) > 0 {
		srv.CORS.Hosts = corsHosts
	}

	srv.CORS.AllowedHeaders = append(srv.CORS.AllowedHeaders, "Last-Event-ID")

	srv.Mux.Handle("/", router)

	srv.Health.AddReadyFunction("s3", func(ctx context.Context) error {
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

	srv.Health.AddReadyFunction("postgres", func(ctx context.Context) error {
		q := postgres.New(dbpool)

		_, err := q.GetActiveSchemas(ctx)
		if err != nil {
			return fmt.Errorf("failed to read schemas: %w", err)
		}

		return nil
	})

	serverGroup, gCtx := errgroup.WithContext(grace.CancelOnQuit(ctx))

	if !conf.NoArchiver {
		log := logger.With(elephantine.LogKeyComponent, "archiver")

		logger.Debug("starting archiver")

		serverGroup.Go(func() error {
			err := startArchiver(
				grace.CancelOnStop(gCtx),
				log, conf, dbpool, store,
				typeConfs,
			)
			if err != nil {
				return err
			}

			return nil
		})
	}

	serverGroup.Go(func() error {
		err := typeConfs.Run(gCtx, store)
		if err != nil {
			return fmt.Errorf("run type configurations: %w", err)
		}

		return nil
	})

	serverGroup.Go(func() error {
		logger.Debug("starting API server")

		err := srv.ListenAndServe(gCtx)
		if err != nil {
			return fmt.Errorf("API server error: %w", err)
		}

		return nil
	})

	serverGroup.Go(func() error {
		if sseSubsystem == nil {
			return nil
		}

		logger.Debug("starting SSE server")

		go func() {
			<-grace.ShouldStop()
			sseSubsystem.Stop()
		}()

		sseSubsystem.Run(gCtx)

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
	ctx context.Context, logger *slog.Logger,
	conf cmd.BackendConfig, dbpool *pgxpool.Pool,
	store *repository.PGDocStore, typeConf *repository.TypeConfigurations,
) error {
	aS3, err := repository.S3Client(ctx, conf.S3Options)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	archiver, err := repository.NewArchiver(repository.ArchiverOptions{
		Logger:             logger,
		S3:                 aS3,
		Bucket:             conf.ArchiveBucket,
		AssetBucket:        conf.AssetBucket,
		DB:                 dbpool,
		Store:              store,
		TolerateGaps:       conf.TolerateEventlogGaps,
		TypeConfigurations: typeConf,
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
