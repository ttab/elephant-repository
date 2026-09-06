package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/julienschmidt/httprouter"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-api/repository/repositoryconnect"
	"github.com/ttab/elephant-repository/internal"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/rpc"
	"github.com/twitchtv/twirp"
	"golang.org/x/sync/errgroup"
)

func SetUpRouter(
	router *httprouter.Router,
	opts ...RouterOption,
) error {
	for _, opt := range opts {
		err := opt(router)
		if err != nil {
			return err
		}
	}

	return nil
}

func ListenAndServe(
	ctx context.Context, addr string, tlsAddr string, h http.Handler,
	corsHosts []string, certFile string, keyFile string,
) error {
	handler := elephantine.LogMetadataMiddleware(h)

	corsHandler := elephantine.CORSMiddleware(elephantine.CORSOptions{
		AllowInsecure:          false,
		AllowInsecureLocalhost: true,
		Hosts:                  corsHosts,
		AllowedMethods:         []string{"GET", "POST"},
		AllowedHeaders: []string{
			"Authorization", "Content-Type", "Last-Event-ID",
			// Sent by every browser Connect client.
			"Connect-Protocol-Version", "Connect-Timeout-Ms",
		},
	}, handler)

	grp, gCtx := errgroup.WithContext(ctx)

	if certFile != "" {
		grp.Go(func() error {
			tlsServer := http.Server{
				Addr:              tlsAddr,
				Handler:           corsHandler,
				ReadHeaderTimeout: 5 * time.Second,
			}

			return elephantine.ListenAndServeContext(
				gCtx, &tlsServer, 10*time.Second,
				elephantine.ListenAndServeTLS(slog.Default(), certFile, keyFile),
			)
		})
	}

	server := http.Server{
		Addr:              addr,
		Handler:           corsHandler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	grp.Go(func() error {
		return elephantine.ListenAndServeContext(
			gCtx, &server, 10*time.Second)
	})

	//nolint:wrapcheck
	return grp.Wait()
}

type ServerOptions struct {
	// Hooks are the Twirp server hooks used for the /twirp/ mount.
	Hooks *twirp.ServerHooks
	// Interceptors are the Connect interceptors used for the Connect
	// mount. They are applied outermost first, so the innermost one is the
	// last in the slice.
	Interceptors   []connect.Interceptor
	AuthMiddleware func(
		w http.ResponseWriter, r *http.Request, next http.Handler,
	) error
}

// twirpOptions are the server options every Twirp mount is created with. The
// interceptor translates a *connect.Error returned by a handler into the Twirp
// error the protocol can render, which is what lets a handler speak one error
// vocabulary while both stacks are mounted.
func (so *ServerOptions) twirpOptions() []any {
	return []any{
		twirp.WithServerJSONSkipDefaults(true),
		twirp.WithServerHooks(so.Hooks),
		twirp.WithServerInterceptors(rpc.TwirpInterceptor()),
	}
}

// connectOptions are the handler options every Connect mount is created with.
// The coding interceptor is innermost, so the metrics and logging interceptors
// see the code the caller will be answered with.
func (so *ServerOptions) connectOptions() []connect.HandlerOption {
	interceptors := make([]connect.Interceptor, 0, len(so.Interceptors)+1)
	interceptors = append(interceptors, so.Interceptors...)
	interceptors = append(interceptors, codeUncodedErrors())

	return []connect.HandlerOption{
		connect.WithInterceptors(interceptors...),
	}
}

// codeUncodedErrors gives an error that carries no RPC code the internal code,
// which is what the Twirp mount does with it through twirp.InternalErrorWith.
// Connect would otherwise answer with unknown, and the two stacks would
// disagree on the code for every handler error that is a plain fmt.Errorf.
//
// This also keeps unknown meaning what docs/observability.md says it means: a
// code Connect itself produced, rather than a server fault a handler returned.
func codeUncodedErrors() connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(
			ctx context.Context, req connect.AnyRequest,
		) (connect.AnyResponse, error) {
			res, err := next(ctx, req)
			if err == nil {
				return res, nil
			}

			var cErr *connect.Error
			if errors.As(err, &cErr) {
				return nil, err
			}

			return nil, connect.NewError(connect.CodeInternal, err)
		}
	})
}

// SetJWTValidation installs the authentication middleware used by the RPC
// services, on both the Twirp and the Connect mount, and by the SSE endpoint. A
// valid token is required: every method behind this middleware asserts its own
// scope requirement, so there is nothing left that legitimately needs anonymous
// access, and rejecting here means a handler that forgets its scope check fails
// closed instead of open.
//
// Being HTTP middleware rather than a Twirp hook is what makes it protocol
// neutral: the handler only ever reads elephantine.GetAuthInfo, so Twirp,
// Connect, gRPC and gRPC-Web are all authenticated by the same code.
//
// Note that this does not cover every route. GET /signing-keys is deliberately
// public and GET /websocket/:token authenticates with its own socket token, so
// neither goes through this middleware.
//
// TODO: This feels like an initial sketch that should be further developed to
// address the JWT cacheing.
func (so *ServerOptions) SetJWTValidation(parser elephantine.AuthInfoParser) {
	so.AuthMiddleware = func(
		w http.ResponseWriter, r *http.Request, next http.Handler,
	) error {
		auth, err := parser.AuthInfoFromHeader(r.Header.Get("Authorization"))
		if err != nil {
			// TODO: Move the response part to a hook instead?
			return elephantine.HTTPErrorf(http.StatusUnauthorized,
				"invalid authorization: %v", err)
		}

		if auth == nil {
			return elephantine.HTTPErrorf(http.StatusInternalServerError,
				"invalid auth info parser response")
		}

		ctx := elephantine.SetAuthInfo(r.Context(), auth)

		elephantine.SetLogMetadata(ctx,
			elephantine.LogKeySubject, auth.Claims.Subject,
		)

		next.ServeHTTP(w, r.WithContext(ctx))

		return nil
	}
}

type RouterOption func(router *httprouter.Router) error

func WithDocumentsAPI(
	service repository.Documents,
	opts ServerOptions,
) RouterOption {
	return func(router *httprouter.Router) error {
		api := repository.NewDocumentsServer(
			service, opts.twirpOptions()...)

		registerAPI(router, opts, api)

		path, handler := repositoryconnect.NewDocumentsServiceHandler(
			service, opts.connectOptions()...)

		registerConnectAPI(router, opts, path, handler)

		return nil
	}
}

func WithSchemasAPI(
	service repository.Schemas,
	opts ServerOptions,
) RouterOption {
	return func(router *httprouter.Router) error {
		api := repository.NewSchemasServer(
			service, opts.twirpOptions()...)

		registerAPI(router, opts, api)

		path, handler := repositoryconnect.NewSchemasServiceHandler(
			service, opts.connectOptions()...)

		registerConnectAPI(router, opts, path, handler)

		return nil
	}
}

func WithWorkflowsAPI(
	service repository.Workflows,
	opts ServerOptions,
) RouterOption {
	return func(router *httprouter.Router) error {
		api := repository.NewWorkflowsServer(
			service, opts.twirpOptions()...)

		registerAPI(router, opts, api)

		path, handler := repositoryconnect.NewWorkflowsServiceHandler(
			service, opts.connectOptions()...)

		registerConnectAPI(router, opts, path, handler)

		return nil
	}
}

func WithSSE(
	handler http.Handler,
	opt ServerOptions,
) RouterOption {
	return func(router *httprouter.Router) error {
		router.GET("/sse", internal.RHandleFunc(func(
			w http.ResponseWriter, r *http.Request, _ httprouter.Params,
		) error {
			token := r.URL.Query().Get("token")
			if token != "" {
				r.Header.Set("Authorization", "Bearer "+token)
			}

			if opt.AuthMiddleware != nil {
				return opt.AuthMiddleware(w, r, handler)
			}

			handler.ServeHTTP(w, r)

			return nil
		}))

		return nil
	}
}

func WithWebsocket(
	handler http.Handler,
) RouterOption {
	return func(router *httprouter.Router) error {
		router.GET("/websocket/:token", internal.RHandleFunc(func(
			w http.ResponseWriter, r *http.Request, _ httprouter.Params,
		) error {
			handler.ServeHTTP(w, r)

			return nil
		}))

		return nil
	}
}

func WithMetricsAPI(
	service repository.Metrics,
	opts ServerOptions,
) RouterOption {
	return func(router *httprouter.Router) error {
		api := repository.NewMetricsServer(
			service, opts.twirpOptions()...)

		registerAPI(router, opts, api)

		path, handler := repositoryconnect.NewMetricsServiceHandler(
			service, opts.connectOptions()...)

		registerConnectAPI(router, opts, path, handler)

		return nil
	}
}

// MarshalPublicSigningKey marshals a SigningKey into a public JWK JSON
// representation with iat/nbf/exp timestamps.
func MarshalPublicSigningKey(sk SigningKey) (json.RawMessage, error) {
	pub, err := sk.Spec.PublicOnly()
	if err != nil {
		return nil, fmt.Errorf(
			"extract public key %q: %w",
			sk.Spec.KeyID, err)
	}

	jwkJSON, err := pub.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf(
			"marshal key %q: %w",
			sk.Spec.KeyID, err)
	}

	var entry map[string]json.RawMessage

	err = json.Unmarshal(jwkJSON, &entry)
	if err != nil {
		return nil, fmt.Errorf(
			"re-parse key %q: %w",
			sk.Spec.KeyID, err)
	}

	entry["iat"] = marshalUnixTime(sk.IssuedAt)
	entry["nbf"] = marshalUnixTime(sk.NotBefore)
	entry["exp"] = marshalUnixTime(sk.NotAfter)

	raw, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf(
			"marshal key entry %q: %w",
			sk.Spec.KeyID, err)
	}

	return raw, nil
}

// WithSigningKeys registers a public endpoint that exposes the archive
// signing public keys as a JWKS document.
func WithSigningKeys(pool *pgxpool.Pool) RouterOption {
	return func(router *httprouter.Router) error {
		router.GET("/signing-keys", internal.RHandleFunc(func(
			w http.ResponseWriter, r *http.Request, _ httprouter.Params,
		) error {
			q := postgres.New(pool)

			keys, err := q.GetSigningKeys(r.Context())
			if err != nil {
				return fmt.Errorf("get signing keys: %w", err)
			}

			entries := make([]json.RawMessage, 0, len(keys))

			for i := range keys {
				var sk SigningKey

				err := json.Unmarshal(keys[i].Spec, &sk)
				if err != nil {
					return fmt.Errorf(
						"unmarshal key %q: %w",
						keys[i].Kid, err)
				}

				raw, err := MarshalPublicSigningKey(sk)
				if err != nil {
					return fmt.Errorf(
						"marshal public key %q: %w",
						keys[i].Kid, err)
				}

				entries = append(entries, raw)
			}

			resp := struct {
				Keys []json.RawMessage `json:"keys"`
			}{
				Keys: entries,
			}

			data, err := json.MarshalIndent(resp, "", "  ")
			if err != nil {
				return fmt.Errorf("marshal response: %w", err)
			}

			w.Header().Set("Content-Type", "application/json")

			_, err = w.Write(data)
			if err != nil {
				return fmt.Errorf("write response: %w", err)
			}

			return nil
		}))

		return nil
	}
}

func marshalUnixTime(t time.Time) json.RawMessage {
	if t.IsZero() {
		return json.RawMessage("0")
	}

	return json.RawMessage(fmt.Sprintf("%d", t.Unix()))
}

type apiServerForRouter interface {
	http.Handler

	PathPrefix() string
}

func registerAPI(
	router *httprouter.Router, opt ServerOptions,
	api apiServerForRouter,
) {
	registerRPCHandler(router, opt, api.PathPrefix()+":method", api)
}

// registerConnectAPI mounts the (path, handler) pair a generated Connect
// constructor returns. The path is the service root ("/elephant.repository.
// Documents/"), so the method becomes a catch-all segment. Connect serves the
// Connect, gRPC and gRPC-Web protocols on it, all of them over POST, and all of
// them behind the same authentication middleware as the Twirp mount.
func registerConnectAPI(
	router *httprouter.Router, opt ServerOptions,
	path string, handler http.Handler,
) {
	registerRPCHandler(router, opt, path+"*method", handler)
}

func registerRPCHandler(
	router *httprouter.Router, opt ServerOptions,
	route string, handler http.Handler,
) {
	router.POST(route, internal.RHandleFunc(func(
		w http.ResponseWriter, r *http.Request, _ httprouter.Params,
	) error {
		if opt.AuthMiddleware != nil {
			return opt.AuthMiddleware(w, r, handler)
		}

		handler.ServeHTTP(w, r)

		return nil
	}))
}
