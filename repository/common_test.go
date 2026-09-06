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

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tmaxmax/go-sse"
	"github.com/ttab/eleconf"
	rpc "github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-api/repository/repositoryconnect"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg/joblock"
	elephantrpc "github.com/ttab/elephantine/rpc"
	"github.com/ttab/elephantine/test"
	"github.com/twitchtv/twirp"
)

func regenerateTestFixtures() bool {
	return os.Getenv("REGENERATE") == "true"
}

const bearerPrefix = "Bearer "

// rpcStack names one of the two protocol stacks the API is served on. The test
// clients are built for the stack the test context carries, so the whole suite
// can be run against either.
type rpcStack string

const (
	stackTwirp   rpcStack = "twirp"
	stackConnect rpcStack = "connect"
)

// stackEnvVar selects the stack the suite runs against when a test does not ask
// for one itself. Both stacks are always mounted; this only decides which
// client constructors the tests get.
const stackEnvVar = "TEST_RPC_STACK"

// defaultStack is the stack the suite runs against unless TEST_RPC_STACK says
// otherwise. Twirp is the default because it is the stack that is in production
// use; the CI test job runs the suite a second time with TEST_RPC_STACK=connect.
func defaultStack(t *testing.T) rpcStack {
	t.Helper()

	switch v := os.Getenv(stackEnvVar); v {
	case "", string(stackTwirp):
		return stackTwirp
	case string(stackConnect):
		return stackConnect
	default:
		t.Fatalf("unknown %s value %q, expected %q or %q",
			stackEnvVar, v, stackTwirp, stackConnect)

		return ""
	}
}

type TestContext struct {
	client *http.Client

	// Stack is the protocol stack the client constructors build for.
	Stack            rpcStack
	SigningKey       *ecdsa.PrivateKey
	Server           *httptest.Server
	Validator        *repository.Validator
	WorkflowProvider *repository.Workflows
	Documents        rpc.Documents
	Schemas          rpc.Schemas
	Workflows        rpc.Workflows
	Env              itest.Environment
}

// bearerTransport attaches an access token to every request. Both stacks
// authenticate with the Authorization header, so putting it in the transport
// rather than in a Twirp client hook is what lets the two client constructors
// share everything but the constructor call.
type bearerTransport struct {
	token string
	next  http.RoundTripper
}

func (bt bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())

	r.Header.Set("Authorization", bearerPrefix+bt.token)

	res, err := bt.next.RoundTrip(r)
	if err != nil {
		return nil, fmt.Errorf("perform request: %w", err)
	}

	return res, nil
}

// authClient returns a copy of the base client that authenticates as claims.
func (tc *TestContext) authClient(
	t *testing.T, base *http.Client, claims elephantine.JWTClaims,
) *http.Client {
	t.Helper()

	token, err := itest.AccessToken(tc.SigningKey, claims)
	test.Mustf(t, err, "create access token")

	next := base.Transport
	if next == nil {
		next = http.DefaultTransport
	}

	client := *base
	client.Transport = bearerTransport{token: token, next: next}

	return &client
}

func (tc *TestContext) SSEConnect(
	t *testing.T, topics []string, claims elephantine.JWTClaims,
) *sse.Connection {
	t.Helper()

	token, err := itest.AccessToken(tc.SigningKey, claims)
	test.Mustf(t, err, "create access token")

	u, err := url.Parse(tc.Server.URL)
	test.Mustf(t, err, "parse server URL")

	u = u.JoinPath("sse")
	u.RawQuery = url.Values{
		"topic": topics,
	}.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	test.Mustf(t, err, "create SSE request")

	req.Header.Set("Authorization", bearerPrefix+token)

	client := sse.Client{
		HTTPClient: tc.client,
	}

	conn := client.NewConnection(req.WithContext(t.Context()))

	go func() {
		err = conn.Connect()
		if err != nil && !errors.Is(err, context.Canceled) {
			test.Mustf(t, err, "create connection")
		}
	}()

	return conn
}

func (tc *TestContext) DocumentsClient(
	t *testing.T, claims elephantine.JWTClaims,
) rpc.Documents {
	t.Helper()

	client := tc.authClient(t, tc.client, claims)

	if tc.Stack == stackConnect {
		return repositoryconnect.NewDocumentsServiceClient(
			client, tc.Server.URL)
	}

	return rpc.NewDocumentsProtobufClient(tc.Server.URL, client)
}

func (tc *TestContext) WorkflowsClient(
	t *testing.T, claims elephantine.JWTClaims,
) rpc.Workflows {
	t.Helper()

	client := tc.authClient(t, tc.client, claims)

	if tc.Stack == stackConnect {
		return repositoryconnect.NewWorkflowsServiceClient(
			client, tc.Server.URL)
	}

	return rpc.NewWorkflowsProtobufClient(tc.Server.URL, client)
}

func (tc *TestContext) SchemasClient(
	t *testing.T, claims elephantine.JWTClaims,
) rpc.Schemas {
	t.Helper()

	// The schema client deliberately uses the untimed server client:
	// activating a generation is slower than the 5s the shared client
	// allows.
	client := tc.authClient(t, tc.Server.Client(), claims)

	if tc.Stack == stackConnect {
		return repositoryconnect.NewSchemasServiceClient(
			client, tc.Server.URL)
	}

	return rpc.NewSchemasProtobufClient(tc.Server.URL, client)
}

func (tc *TestContext) MetricsClient(
	t *testing.T, claims elephantine.JWTClaims,
) rpc.Metrics {
	t.Helper()

	client := tc.authClient(t, tc.client, claims)

	if tc.Stack == stackConnect {
		return repositoryconnect.NewMetricsServiceClient(
			client, tc.Server.URL)
	}

	return rpc.NewMetricsProtobufClient(tc.Server.URL, client)
}

type testingServerOptions struct {
	RunArchiver        bool
	RunEventlogBuilder bool
	SharedSecret       string
	NoCharcount        bool
	ConfigDirectory    string
	Schemas            []eleconf.LoadedSchema
	NoCoreSchemas      bool
	EmitWorkflowEvent  bool
	EmitACLEvent       bool
	// EventlogStream overrides the eventlog stream config for the socket
	// handler. A zero BufferSize defaults to 500.
	EventlogStream repository.EventlogStreamConfig
	// Stack overrides the protocol stack the test clients are built for.
	// Empty means the one TEST_RPC_STACK selects.
	Stack rpcStack
}

func testingAPIServer(
	t *testing.T, logger *slog.Logger, opts testingServerOptions,
) TestContext {
	t.Helper()

	reg := prometheus.NewRegistry()

	instrumentation, err := elephantine.NewHTTPClientIntrumentation(reg)
	test.Mustf(t, err, "set up HTTP client instrumentation")

	env := itest.SetUpBackingServices(t, instrumentation, false)
	ctx := t.Context()

	dbpool, err := pgxpool.New(ctx, env.PostgresURI)
	test.Mustf(t, err, "create connection pool")

	t.Cleanup(func() {
		// We don't want to block cleanup waiting for pool.
		go dbpool.Close()
	})

	assetBucket := repository.NewAssetBucket(
		logger,
		s3.NewPresignClient(env.S3, s3.WithPresignExpires(15*time.Minute)),
		env.S3,
		env.AssetBucket,
	)

	var inMet []repository.MetricCalculator

	if !opts.NoCharcount {
		inMet = append(inMet, repository.NewCharCounter())
	}

	typeConf := repository.NewTypeConfigurations(logger, time.UTC)

	store, err := repository.NewPGDocStore(
		t.Context(),
		logger, dbpool,
		assetBucket,
		repository.PGDocStoreOptions{
			DeleteTimeout:      1 * time.Second,
			MetricsCalculators: inMet,
			TypeConfigurations: typeConf,
			EmitWorkflowEvent:  opts.EmitWorkflowEvent,
			EmitACLEvent:       opts.EmitACLEvent,
		})
	test.Mustf(t, err, "create doc store")

	go store.RunListener(ctx, dbpool)

	go func() {
		err := typeConf.Run(ctx, store)
		test.Mustf(t, err, "run type configurations")
	}()

	sse, err := repository.NewSSE(ctx, logger.With(
		elephantine.LogKeyComponent, "sse",
	), store)
	test.Mustf(t, err, "set up SSE server")

	go sse.Run(ctx)

	t.Cleanup(sse.Stop)

	if opts.RunArchiver {
		archiver, err := repository.NewArchiver(repository.ArchiverOptions{
			Logger:             logger,
			S3:                 env.S3,
			Bucket:             env.Bucket,
			AssetBucket:        env.AssetBucket,
			DB:                 dbpool,
			MetricsRegisterer:  reg,
			Store:              store,
			TypeConfigurations: typeConf,
		})
		test.Mustf(t, err, "create archiver")

		go func() {
			err = archiver.Run(ctx)
			if !errors.Is(err, context.Canceled) {
				test.Mustf(t, err, "run archiver")
			}
		}()

		t.Cleanup(func() {
			err := archiver.Stop(context.Background())
			test.Mustf(t, err, "stop archiver")
		})
	}

	if opts.RunEventlogBuilder {
		log := logger.With(elephantine.LogKeyComponent, "eventlog-builder")

		log.Debug("setting up eventlog builder")

		updates := make(chan int64, 1)

		store.OnEventOutbox(t.Context(), updates)

		builder, err := repository.NewEventlogBuilder(
			log, dbpool, reg, updates)
		test.Mustf(t, err, "set up eventlog builder")

		go func() {
			err := joblock.Run(t.Context(),
				dbpool, log,
				"eventlog-builder", "eventlog-builder",
				joblock.Options{},
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
		ctx, logger, store, reg)
	test.Mustf(t, err, "create validator")

	t.Cleanup(validator.Stop)

	workflows, err := repository.NewWorkflows(ctx, logger, store)
	test.Mustf(t, err, "create workflows")

	socketKey, err := store.EnsureSocketKey(ctx)
	test.Mustf(t, err, "ensure socket key")

	docCache := repository.NewDocCache(store, 1000)

	docService, err := repository.NewDocumentsService(
		store,
		repository.NewSchedulePGStore(dbpool),
		validator,
		workflows,
		assetBucket,
		"sv-se",
		typeConf,
		docCache,
		socketKey,
	)
	test.Mustf(t, err, "create documents service")

	schemaService := repository.NewSchemasService(logger, store)
	workflowService := repository.NewWorkflowsService(store)
	metricsService := repository.NewMetricsService(store)

	router := httprouter.New()

	jwtKey, err := itest.NewSigningKey()
	test.Mustf(t, err, "create signing key")

	var srvOpts repository.ServerOptions

	// Both stacks are wired up with their metrics, which is also what
	// asserts that they share the collectors: registering the same RPC
	// metric twice against reg would fail here.
	twirpMetrics, err := elephantine.NewTwirpMetricsHooks(
		elephantine.WithTwirpMetricsRegisterer(reg))
	test.Mustf(t, err, "create twirp metrics hooks")

	srvOpts.Hooks = twirp.ChainHooks(
		elephantine.LoggingHooks(logger),
		twirpMetrics,
	)

	connectMetrics, err := elephantrpc.MetricsInterceptor(reg)
	test.Mustf(t, err, "create connect metrics interceptor")

	srvOpts.Interceptors = []connect.Interceptor{
		connectMetrics,
		elephantrpc.LoggingInterceptor(logger),
		elephantrpc.LegacyTwirpErrors(),
	}

	authParser := elephantine.NewStaticAuthInfoParser(
		t.Context(),
		jwtKey.PublicKey,
		elephantine.JWTAuthInfoParserOptions{
			Issuer: "test",
		})

	srvOpts.SetJWTValidation(authParser)

	socket, err := repository.NewSocketHandler(
		ctx, logger, reg,
		store, docCache, authParser, &socketKey.PublicKey,
		[]string{"localhost", "example.ecms.se"},
		opts.EventlogStream)
	test.Mustf(t, err, "set up socket handler")

	err = repository.SetUpRouter(router,
		repository.WithDocumentsAPI(docService, srvOpts),
		repository.WithSchemasAPI(schemaService, srvOpts),
		repository.WithWorkflowsAPI(workflowService, srvOpts),
		repository.WithMetricsAPI(metricsService, srvOpts),
		repository.WithSSE(sse.HTTPHandler(), srvOpts),
		repository.WithWebsocket(socket),
		repository.WithSigningKeys(dbpool),
	)
	test.Mustf(t, err, "set up router")

	server := httptest.NewServer(router)

	t.Cleanup(server.Close)

	client := server.Client()

	client.Timeout = 5 * time.Second

	stack := opts.Stack
	if stack == "" {
		stack = defaultStack(t)
	}

	tc := TestContext{
		client:           client,
		Stack:            stack,
		SigningKey:       jwtKey,
		Server:           server,
		Validator:        validator,
		Documents:        docService,
		Workflows:        workflowService,
		Schemas:          schemaService,
		WorkflowProvider: workflows,
		Env:              env,
	}

	wf := tc.WorkflowsClient(t,
		itest.StandardClaims(t, repository.ScopeWorkflowAdmin))

	if opts.ConfigDirectory == "" {
		opts.ConfigDirectory = filepath.Join("..", "testdata", "config", "base")
	}

	schemas := opts.Schemas

	if !opts.NoCoreSchemas {
		core, err := repository.LoadEmbeddedSchemaSet("se.ecms", "se.ecms.metadoc", "se.ecms.planning")
		test.Mustf(t, err, "load core schemas")

		schemas = append(schemas, core...)
	}

	config, err := eleconf.ReadConfigFromDirectory(opts.ConfigDirectory)
	test.Mustf(t, err, "read repository configuration")

	clients := eleconf.StaticClients{
		Workflows: wf,
		Schemas: tc.SchemasClient(t,
			itest.StandardClaims(t, repository.ScopeSchemaAdmin)),
		Metrics: tc.MetricsClient(t,
			itest.StandardClaims(t, repository.ScopeMetricsAdmin)),
	}

	err = repository.BootstrapGeneration(ctx, store)
	test.Mustf(t, err, "bootstrap generation")

	changes, err := eleconf.GetChanges(ctx, &clients, config, schemas,
		nil, rpc.SchemaActivation_ACTIVATION_ACTIVE)
	test.Mustf(t, err, "get changes")

	for _, change := range changes {
		err := change.Execute(ctx, &clients)
		test.Mustf(t, err, "apply configuration")
	}

	err = validator.RefreshSchemas(ctx)
	test.Mustf(t, err, "refresh validator")

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
