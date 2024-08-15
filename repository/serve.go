package repository

import (
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/julienschmidt/httprouter"
	"github.com/rakutentech/jwk-go/jwk"
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

func ListenAndServe(ctx context.Context, addr string, h http.Handler) error {
	var handler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		ctx := elephantine.WithLogMetadata(r.Context())

		h.ServeHTTP(w, r.WithContext(ctx))
	}

	// TODO: Configurable hosts
	corsHandler := elephantine.CORSMiddleware(elephantine.CORSOptions{
		AllowInsecure:          false,
		AllowInsecureLocalhost: true,
		Hosts:                  []string{"localhost", "tt.se"},
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

func (so *ServerOptions) SetJWTValidation(parser *elephantine.AuthInfoParser) {
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

func WithReportsAPI(
	service repository.Reports,
	opts ServerOptions,
) RouterOption {
	return func(router *httprouter.Router) error {
		api := repository.NewReportsServer(
			service,
			twirp.WithServerJSONSkipDefaults(true),
			twirp.WithServerHooks(opts.Hooks),
		)

		registerAPI(router, opts, api)

		return nil
	}
}

func WithJWKSEndpoint(jwtKey *ecdsa.PrivateKey) RouterOption {
	return func(router *httprouter.Router) error {
		key := jwk.NewSpec(jwtKey)

		key.Algorithm = jwt.SigningMethodES384.Alg()
		key.Use = "sig"

		keySet := jwk.KeySpecSet{
			Keys: []jwk.KeySpec{*key},
		}

		payload, err := keySet.MarshalPublicJSON()
		if err != nil {
			return fmt.Errorf("failed to create JWKs payload: %w", err)
		}

		router.GET("/.well-known/jwks.json", httprouter.Handle(func(
			w http.ResponseWriter, _ *http.Request, _ httprouter.Params,
		) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(payload)
		}))

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

func WithTokenEndpoint(
	jwtKey *ecdsa.PrivateKey,
	sharedSecret string,
	issuer string,
	audience string,
) RouterOption {
	return func(router *httprouter.Router) error {
		router.POST("/token", internal.RHandleFunc(func(
			w http.ResponseWriter, r *http.Request, _ httprouter.Params,
		) error {
			err := r.ParseForm()
			if err != nil {
				return elephantine.HTTPErrorf(http.StatusBadRequest,
					"invalid form data: %v", err)
			}

			form := r.Form

			switch form.Get("grant_type") {
			case "password":
			case "refresh_token":
				if form.Get("refresh_token") == "" {
					return elephantine.HTTPErrorf(http.StatusBadRequest,
						"missing 'refresh_token'")
				}

				rData, err := base64.RawStdEncoding.DecodeString(
					form.Get("refresh_token"))
				if err != nil {
					return elephantine.HTTPErrorf(http.StatusBadRequest,
						"invalid refresh token: %v", err)
				}

				f, err := url.ParseQuery(string(rData))
				if err != nil {
					return elephantine.HTTPErrorf(http.StatusBadRequest,
						"invalid refresh contents: %v", err)
				}

				form = f
			default:
				return elephantine.HTTPErrorf(http.StatusBadRequest,
					"we only support the \"password\" and \"refresh_token\" grant_type")
			}

			username := form.Get("username")
			if username == "" {
				return elephantine.HTTPErrorf(http.StatusBadRequest,
					"missing 'username'")
			}

			password := form.Get("password")
			if password != sharedSecret {
				return elephantine.HTTPErrorf(http.StatusUnauthorized,
					"invalid password")
			}

			scope := form.Get("scope")

			name, uriPart, ok := strings.Cut(username, " <")
			if !ok || !strings.HasSuffix(uriPart, ">") {
				return elephantine.HTTPErrorf(http.StatusBadRequest,
					"username must be in the format \"Name <some://sub/uri>\"")
			}

			subURI := strings.TrimSuffix(uriPart, ">")
			expiresIn := 10 * time.Minute

			sub, units, _ := strings.Cut(subURI, ", ")

			aud := []string{}
			if audience != "" {
				aud = []string{audience}
			}

			claims := elephantine.JWTClaims{
				RegisteredClaims: jwt.RegisteredClaims{
					ExpiresAt: jwt.NewNumericDate(
						time.Now().Add(expiresIn)),
					Issuer:   issuer,
					Audience: aud,
					Subject:  sub,
				},
				Name:  name,
				Scope: scope,
			}

			if len(units) > 0 {
				claims.Units = strings.Split(units, ", ")
			}

			token := jwt.NewWithClaims(jwt.SigningMethodES384, claims)

			ss, err := token.SignedString(jwtKey)
			if err != nil {
				return elephantine.HTTPErrorf(http.StatusInternalServerError,
					"failed to sign JWT token")
			}

			w.Header().Set("Content-Type", "application/json")

			err = json.NewEncoder(w).Encode(TokenResponse{
				AccessToken: ss,
				RefreshToken: base64.RawURLEncoding.EncodeToString(
					[]byte(form.Encode()),
				),
				TokenType: "Bearer",
				ExpiresIn: int(expiresIn.Seconds()),
			})
			if err != nil {
				return elephantine.HTTPErrorf(http.StatusInternalServerError,
					"failed encode token response")
			}

			return nil
		}))

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
