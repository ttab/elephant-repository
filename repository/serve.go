package repository

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-repository/internal"
	"github.com/ttab/elephantine"
	"github.com/twitchtv/twirp"
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
	ctx context.Context, addr string, h http.Handler,
	corsHosts []string,
) error {
	var handler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		ctx := elephantine.WithLogMetadata(r.Context())

		h.ServeHTTP(w, r.WithContext(ctx))
	}

	corsHandler := elephantine.CORSMiddleware(elephantine.CORSOptions{
		AllowInsecure:          false,
		AllowInsecureLocalhost: true,
		Hosts:                  corsHosts,
		AllowedMethods:         []string{"GET", "POST"},
		AllowedHeaders:         []string{"Authorization", "Content-Type", "Last-Event-ID"},
	}, handler)

	server := http.Server{
		Addr:              addr,
		Handler:           corsHandler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	//nolint:wrapcheck
	return elephantine.ListenAndServeContext(ctx, &server, 10*time.Second)
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
