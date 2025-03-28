package repository_test

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tmaxmax/go-sse"
	rpc "github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/elephantine/test"
	"github.com/ttab/revisor"
	"github.com/twitchtv/twirp"
)

func regenerateTestFixtures() bool {
	return os.Getenv("REGENERATE") == "true"
}

const bearerPrefix = "Bearer "

type TestContext struct {
	SigningKey       *ecdsa.PrivateKey
	Server           *httptest.Server
	WorkflowProvider repository.WorkflowProvider
	Documents        rpc.Documents
	Schemas          rpc.Schemas
	Workflows        rpc.Workflows
	Env              itest.Environment
}

func (tc *TestContext) SSEConnect(
	t *testing.T, topics []string, claims elephantine.JWTClaims,
) *sse.Connection {
	t.Helper()

	token, err := itest.AccessKey(tc.SigningKey, claims)
	test.Must(t, err, "create access key")

	u, err := url.Parse(tc.Server.URL)
	test.Must(t, err, "parse server URL")

	u = u.JoinPath("sse")
	u.RawQuery = url.Values{
		"topic": topics,
	}.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	test.Must(t, err, "create SSE request")

	req.Header.Set("Authorization", bearerPrefix+token)

	client := sse.Client{
		HTTPClient: tc.Server.Client(),
	}

	conn := client.NewConnection(req.WithContext(t.Context()))

	go func() {
		err = conn.Connect()
		if err != nil && !errors.Is(err, context.Canceled) {
			test.Must(t, err, "create connection")
		}
	}()

	return conn
}

func (tc *TestContext) DocumentsClient(
	t *testing.T, claims elephantine.JWTClaims,
) rpc.Documents {
	t.Helper()

	token, err := itest.AccessKey(tc.SigningKey, claims)
	test.Must(t, err, "create access key")

	docClient := rpc.NewDocumentsProtobufClient(
		tc.Server.URL, tc.Server.Client(),
		twirp.WithClientHooks(&twirp.ClientHooks{
			RequestPrepared: func(ctx context.Context, r *http.Request) (context.Context, error) {
				r.Header.Set("Authorization", bearerPrefix+token)

				return ctx, nil
			},
		}))

	return docClient
}

func (tc *TestContext) WorkflowsClient(
	t *testing.T, claims elephantine.JWTClaims,
) rpc.Workflows {
	t.Helper()

	token, err := itest.AccessKey(tc.SigningKey, claims)
	test.Must(t, err, "create access key")

	workflowsClient := rpc.NewWorkflowsProtobufClient(
		tc.Server.URL, tc.Server.Client(),
		twirp.WithClientHooks(&twirp.ClientHooks{
			RequestPrepared: func(ctx context.Context, r *http.Request) (context.Context, error) {
				r.Header.Set("Authorization", bearerPrefix+token)

				return ctx, nil
			},
		}))

	return workflowsClient
}

func (tc *TestContext) ReportsClient(
	t *testing.T, claims elephantine.JWTClaims,
) rpc.Reports {
	t.Helper()

	token, err := itest.AccessKey(tc.SigningKey, claims)
	test.Must(t, err, "create access key")

	reportsClient := rpc.NewReportsProtobufClient(
		tc.Server.URL, tc.Server.Client(),
		twirp.WithClientHooks(&twirp.ClientHooks{
			RequestPrepared: func(ctx context.Context, r *http.Request) (context.Context, error) {
				r.Header.Set("Authorization", bearerPrefix+token)

				return ctx, nil
			},
		}))

	return reportsClient
}

func (tc *TestContext) SchemasClient(
	t *testing.T, claims elephantine.JWTClaims,
) rpc.Schemas {
	t.Helper()

	token, err := itest.AccessKey(tc.SigningKey, claims)
	test.Must(t, err, "create access key")

	schemasClient := rpc.NewSchemasProtobufClient(
		tc.Server.URL, tc.Server.Client(),
		twirp.WithClientHooks(&twirp.ClientHooks{
			RequestPrepared: func(ctx context.Context, r *http.Request) (context.Context, error) {
				r.Header.Set("Authorization", bearerPrefix+token)

				return ctx, nil
			},
		}))

	return schemasClient
}

func (tc *TestContext) MetricsClient(
	t *testing.T, claims elephantine.JWTClaims,
) rpc.Metrics {
	t.Helper()

	token, err := itest.AccessKey(tc.SigningKey, claims)
	test.Must(t, err, "create access key")

	metricsClient := rpc.NewMetricsProtobufClient(
		tc.Server.URL, tc.Server.Client(),
		twirp.WithClientHooks(&twirp.ClientHooks{
			RequestPrepared: func(ctx context.Context, r *http.Request) (context.Context, error) {
				r.Header.Set("Authorization", bearerPrefix+token)

				return ctx, nil
			},
		}))

	return metricsClient
}

type testingServerOptions struct {
	RunArchiver        bool
	RunEventlogBuilder bool
	SharedSecret       string
	ExtraSchemas       []string
	NoStandardStatuses bool
}

func testingAPIServer(
	t *testing.T, logger *slog.Logger, opts testingServerOptions,
) TestContext {
	t.Helper()

	reg := prometheus.NewRegistry()

	instrumentation, err := elephantine.NewHTTPClientIntrumentation(reg)
	test.Must(t, err, "set up HTTP client instrumentation")

	env := itest.SetUpBackingServices(t, instrumentation, false)
	ctx := t.Context()

	dbpool, err := pgxpool.New(ctx, env.PostgresURI)
	test.Must(t, err, "create connection pool")

	t.Cleanup(func() {
		// We don't want to block cleanup waiting for pool.
		go dbpool.Close()
	})

	reportingPool, err := pgxpool.New(ctx, env.ReportingURI)
	test.Must(t, err, "create reporting connection pool")

	t.Cleanup(func() {
		// We don't want to block cleanup waiting for pool.
		go reportingPool.Close()
	})

	store, err := repository.NewPGDocStore(logger, dbpool,
		repository.PGDocStoreOptions{
			DeleteTimeout: 1 * time.Second,
		})
	test.Must(t, err, "create doc store")

	go store.RunListener(ctx)

	err = repository.EnsureCoreSchema(ctx, store)
	test.Must(t, err, "ensure core schema")

	for _, name := range opts.ExtraSchemas {
		var constraints revisor.ConstraintSet

		err := elephantine.UnmarshalFile(name, &constraints)
		test.Must(t, err, "decode schema in %q", name)

		err = repository.EnsureSchema(ctx, store,
			filepath.Base(name), "v0.0.0", constraints)
		test.Must(t, err, "register the schema %q", name)
	}

	sse, err := repository.NewSSE(ctx, logger.With(
		elephantine.LogKeyComponent, "sse",
	), store)
	test.Must(t, err, "failed to set up SSE server")

	go sse.Run(ctx)

	t.Cleanup(sse.Stop)

	if opts.RunArchiver {
		archiver, err := repository.NewArchiver(repository.ArchiverOptions{
			Logger:            logger,
			S3:                env.S3,
			Bucket:            env.Bucket,
			DB:                dbpool,
			MetricsRegisterer: reg,
		})
		test.Must(t, err, "create archiver")

		err = archiver.Run(ctx)
		test.Must(t, err, "run archiver")

		t.Cleanup(archiver.Stop)
	}

	if opts.RunEventlogBuilder {
		log := logger.With(elephantine.LogKeyComponent, "eventlog-builder")

		log.Debug("setting up eventlog builder")

		updates := make(chan int64, 1)

		store.OnEventOutbox(t.Context(), updates)

		builder, err := repository.NewEventlogBuilder(
			log, dbpool, reg, updates)
		test.Must(t, err, "set up eventlog builder")

		go func() {
			err := pg.RunInJobLock(t.Context(),
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

	validator, err := repository.NewValidator(
		ctx, logger, store, prometheus.NewRegistry())
	test.Must(t, err, "create validator")

	t.Cleanup(validator.Stop)

	workflows, err := repository.NewWorkflows(ctx, logger, store)
	test.Must(t, err, "create workflows")

	docService := repository.NewDocumentsService(
		store,
		repository.NewSchedulePGStore(dbpool),
		validator,
		workflows,
		"sv-se",
	)
	schemaService := repository.NewSchemasService(logger, store)
	workflowService := repository.NewWorkflowsService(store)
	metricsService := repository.NewMetricsService(store)

	router := httprouter.New()

	jwtKey, err := itest.NewSigningKey()
	test.Must(t, err, "create signing key")

	var srvOpts repository.ServerOptions

	srvOpts.Hooks = elephantine.LoggingHooks(logger)

	srvOpts.SetJWTValidation(elephantine.NewStaticAuthInfoParser(jwtKey.PublicKey, elephantine.JWTAuthInfoParserOptions{
		Issuer: "test",
	}))

	err = repository.SetUpRouter(router,
		repository.WithDocumentsAPI(docService, srvOpts),
		repository.WithSchemasAPI(schemaService, srvOpts),
		repository.WithWorkflowsAPI(workflowService, srvOpts),
		repository.WithMetricsAPI(metricsService, srvOpts),
		repository.WithSSE(sse.HTTPHandler(), srvOpts),
	)
	test.Must(t, err, "set up router")

	server := httptest.NewServer(router)

	t.Cleanup(server.Close)

	tc := TestContext{
		SigningKey:       jwtKey,
		Server:           server,
		Documents:        docService,
		Workflows:        workflowService,
		Schemas:          schemaService,
		WorkflowProvider: workflows,
		Env:              env,
	}

	if !opts.NoStandardStatuses {
		wf := tc.WorkflowsClient(t,
			itest.StandardClaims(t, repository.ScopeWorkflowAdmin))

		for _, status := range []string{"usable", "done", "approved"} {
			_, err := wf.UpdateStatus(ctx, &rpc.UpdateStatusRequest{
				Type: "core/article",
				Name: status,
			})
			test.Must(t, err, "create status %q", status)
		}

		workflowDeadline := time.After(1 * time.Second)

		// Wait until the workflow provider notices the change.
		for !tc.WorkflowProvider.HasStatus("core/article", "approved") {
			select {
			case <-workflowDeadline:
				t.Fatal("workflow didn't get updated in time")
			case <-time.After(1 * time.Millisecond):
			}
		}
	}

	return tc
}

func TestMain(m *testing.M) {
	exitVal := m.Run()

	err := itest.PurgeBackingServices()
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"failed to clean up backend services: %v\n", err)
	}

	os.Exit(exitVal)
}
