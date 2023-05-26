package repository_test

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	rpc "github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant/internal/test"
	"github.com/ttab/elephant/repository"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"github.com/twitchtv/twirp"
	"golang.org/x/exp/slog"
)

type TestContext struct {
	SigningKey       *ecdsa.PrivateKey
	Server           *httptest.Server
	WorkflowProvider repository.WorkflowProvider
	Documents        rpc.Documents
	Schemas          rpc.Schemas
	Workflows        rpc.Workflows
	Env              itest.Environment
}

func (tc *TestContext) DocumentsClient(
	t *testing.T, claims repository.JWTClaims,
) rpc.Documents {
	t.Helper()

	token, err := itest.AccessKey(tc.SigningKey, claims)
	test.Must(t, err, "create access key")

	docClient := rpc.NewDocumentsProtobufClient(
		tc.Server.URL, tc.Server.Client(),
		twirp.WithClientHooks(&twirp.ClientHooks{
			RequestPrepared: func(ctx context.Context, r *http.Request) (context.Context, error) {
				r.Header.Set("Authorization", "Bearer "+token)

				return ctx, nil
			},
		}))

	return docClient
}

func (tc *TestContext) WorkflowsClient(
	t *testing.T, claims repository.JWTClaims,
) rpc.Workflows {
	t.Helper()

	token, err := itest.AccessKey(tc.SigningKey, claims)
	test.Must(t, err, "create access key")

	workflowsClient := rpc.NewWorkflowsProtobufClient(
		tc.Server.URL, tc.Server.Client(),
		twirp.WithClientHooks(&twirp.ClientHooks{
			RequestPrepared: func(ctx context.Context, r *http.Request) (context.Context, error) {
				r.Header.Set("Authorization", "Bearer "+token)

				return ctx, nil
			},
		}))

	return workflowsClient
}

func (tc *TestContext) ReportsClient(
	t *testing.T, claims repository.JWTClaims,
) rpc.Reports {
	t.Helper()

	token, err := itest.AccessKey(tc.SigningKey, claims)
	test.Must(t, err, "create access key")

	reportsClient := rpc.NewReportsProtobufClient(
		tc.Server.URL, tc.Server.Client(),
		twirp.WithClientHooks(&twirp.ClientHooks{
			RequestPrepared: func(ctx context.Context, r *http.Request) (context.Context, error) {
				r.Header.Set("Authorization", "Bearer "+token)

				return ctx, nil
			},
		}))

	return reportsClient
}

func (tc *TestContext) SchemasClient(
	t *testing.T, claims repository.JWTClaims,
) rpc.Schemas {
	t.Helper()

	token, err := itest.AccessKey(tc.SigningKey, claims)
	test.Must(t, err, "create access key")

	schemasClient := rpc.NewSchemasProtobufClient(
		tc.Server.URL, tc.Server.Client(),
		twirp.WithClientHooks(&twirp.ClientHooks{
			RequestPrepared: func(ctx context.Context, r *http.Request) (context.Context, error) {
				r.Header.Set("Authorization", "Bearer "+token)

				return ctx, nil
			},
		}))

	return schemasClient
}

func (tc *TestContext) MetricsClient(
	t *testing.T, claims repository.JWTClaims,
) rpc.Metrics {
	t.Helper()

	token, err := itest.AccessKey(tc.SigningKey, claims)
	test.Must(t, err, "create access key")

	metricsClient := rpc.NewMetricsProtobufClient(
		tc.Server.URL, tc.Server.Client(),
		twirp.WithClientHooks(&twirp.ClientHooks{
			RequestPrepared: func(ctx context.Context, r *http.Request) (context.Context, error) {
				r.Header.Set("Authorization", "Bearer "+token)

				return ctx, nil
			},
		}))

	return metricsClient
}

type testingServerOptions struct {
	RunArchiver   bool
	RunReplicator bool
	SharedSecret  string
}

func testingAPIServer(
	t *testing.T, logger *slog.Logger, opts testingServerOptions,
) TestContext {
	t.Helper()

	reg := prometheus.NewRegistry()

	instrumentation, err := elephantine.NewHTTPClientIntrumentation(reg)
	test.Must(t, err, "set up HTTP client instrumentation")

	env := itest.SetUpBackingServices(t, instrumentation, false)
	ctx := test.Context(t)

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

	if opts.RunReplicator {
		repl, err := repository.NewPGReplication(
			logger, dbpool, env.PostgresURI, t.Name(),
			prometheus.NewRegistry(),
		)
		test.Must(t, err, "create replicator")

		go repl.Run(ctx)

		t.Cleanup(repl.Stop)

		select {
		case <-ctx.Done():
			t.Fatal("failed to start replicator")
		case <-repl.Started():
		}
	}

	validator, err := repository.NewValidator(
		ctx, logger, store)
	test.Must(t, err, "create validator")

	workflows, err := repository.NewWorkflows(ctx, logger, store)
	test.Must(t, err, "create workflows")

	docService := repository.NewDocumentsService(store, validator, workflows)
	schemaService := repository.NewSchemasService(store)
	workflowService := repository.NewWorkflowsService(store)
	reportsService := repository.NewReportsService(logger, store, reportingPool)
	metricsService := repository.NewMetricsService(store)

	router := httprouter.New()

	jwtKey, err := itest.NewSigningKey()
	test.Must(t, err, "create signing key")

	var srvOpts repository.ServerOptions

	srvOpts.SetJWTValidation(jwtKey)

	err = repository.SetUpRouter(router,
		repository.WithTokenEndpoint(jwtKey, opts.SharedSecret),
		repository.WithDocumentsAPI(docService, srvOpts),
		repository.WithSchemasAPI(schemaService, srvOpts),
		repository.WithWorkflowsAPI(workflowService, srvOpts),
		repository.WithReportsAPI(reportsService, srvOpts),
		repository.WithMetricsAPI(metricsService, srvOpts),
	)
	test.Must(t, err, "set up router")

	server := httptest.NewServer(router)

	t.Cleanup(server.Close)

	return TestContext{
		SigningKey:       jwtKey,
		Server:           server,
		Documents:        docService,
		Workflows:        workflowService,
		Schemas:          schemaService,
		WorkflowProvider: workflows,
		Env:              env,
	}
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
