package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/julienschmidt/httprouter"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-repository/internal"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine"
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
		AllowedHeaders:         []string{"Authorization", "Content-Type", "Last-Event-ID"},
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
				elephantine.ListenAndServeTLS(certFile, keyFile),
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
	Hooks          *twirp.ServerHooks
	AuthMiddleware func(
		w http.ResponseWriter, r *http.Request, next http.Handler,
	) error
}

func (so *ServerOptions) SetJWTValidation(parser elephantine.AuthInfoParser) {
	// TODO: This feels like an initial sketch that should be further
	// developed to address the JWT cacheing.
	so.AuthMiddleware = func(
		w http.ResponseWriter, r *http.Request, next http.Handler,
	) error {
		auth, err := parser.AuthInfoFromHeader(r.Header.Get("Authorization"))
		if err != nil && !errors.Is(err, elephantine.ErrNoAuthorization) {
			// TODO: Move the response part to a hook instead?
			return elephantine.HTTPErrorf(http.StatusUnauthorized,
				"invalid authorization method: %v", err)
		}

		if auth != nil {
			ctx := elephantine.SetAuthInfo(r.Context(), auth)

			elephantine.SetLogMetadata(ctx,
				elephantine.LogKeySubject, auth.Claims.Subject,
			)

			r = r.WithContext(ctx)
		}

		next.ServeHTTP(w, r)

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
			service,
			twirp.WithServerJSONSkipDefaults(true),
			twirp.WithServerHooks(opts.Hooks),
		)

		registerAPI(router, opts, api)

		return nil
	}
}

func WithSchemasAPI(
	service repository.Schemas,
	opts ServerOptions,
) RouterOption {
	return func(router *httprouter.Router) error {
		api := repository.NewSchemasServer(
			service,
			twirp.WithServerJSONSkipDefaults(true),
			twirp.WithServerHooks(opts.Hooks),
		)

		registerAPI(router, opts, api)

		return nil
	}
}

func WithWorkflowsAPI(
	service repository.Workflows,
	opts ServerOptions,
) RouterOption {
	return func(router *httprouter.Router) error {
		api := repository.NewWorkflowsServer(
			service,
			twirp.WithServerJSONSkipDefaults(true),
			twirp.WithServerHooks(opts.Hooks),
		)

		registerAPI(router, opts, api)

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
			service,
			twirp.WithServerJSONSkipDefaults(true),
			twirp.WithServerHooks(opts.Hooks),
		)

		registerAPI(router, opts, api)

		return nil
	}
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

				pub, err := sk.Spec.PublicOnly()
				if err != nil {
					return fmt.Errorf(
						"extract public key %q: %w",
						keys[i].Kid, err)
				}

				jwkJSON, err := pub.MarshalJSON()
				if err != nil {
					return fmt.Errorf(
						"marshal key %q: %w",
						keys[i].Kid, err)
				}

				var entry map[string]json.RawMessage

				err = json.Unmarshal(jwkJSON, &entry)
				if err != nil {
					return fmt.Errorf(
						"re-parse key %q: %w",
						keys[i].Kid, err)
				}

				entry["iat"] = marshalUnixTime(sk.IssuedAt)
				entry["nbf"] = marshalUnixTime(sk.NotBefore)
				entry["exp"] = marshalUnixTime(sk.NotAfter)

				raw, err := json.Marshal(entry)
				if err != nil {
					return fmt.Errorf(
						"marshal key entry %q: %w",
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
	router.POST(api.PathPrefix()+":method", internal.RHandleFunc(func(
		w http.ResponseWriter, r *http.Request, _ httprouter.Params,
	) error {
		if opt.AuthMiddleware != nil {
			return opt.AuthMiddleware(w, r, api)
		}

		api.ServeHTTP(w, r)

		return nil
	}))
}
