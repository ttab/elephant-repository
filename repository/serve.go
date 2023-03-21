package repository

import (
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/julienschmidt/httprouter"
	"github.com/ttab/elephant/internal"
	"github.com/ttab/elephant/rpc/repository"
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
	server := http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
	}

	//nolint:wrapcheck
	return internal.ListenAndServeContext(ctx, &server)
}

type ServerOptions struct {
	Hooks          *twirp.ServerHooks
	AuthMiddleware func(
		w http.ResponseWriter, r *http.Request, next http.Handler,
	) error
}

func (so *ServerOptions) SetJWTValidation(jwtKey *ecdsa.PrivateKey) {
	// TODO: This feels like an initial sketch that should be further
	// developed to address the JWT cacheing.
	so.AuthMiddleware = func(
		w http.ResponseWriter, r *http.Request, next http.Handler,
	) error {
		auth, err := AuthInfoFromHeader(&jwtKey.PublicKey,
			r.Header.Get("Authorization"))
		if err != nil && !errors.Is(err, ErrNoAuthorization) {
			// TODO: Move the response part to a hook instead?
			return internal.HTTPErrorf(http.StatusUnauthorized,
				"invalid authorization method: %v", err)
		}

		if auth != nil {
			r = r.WithContext(SetAuthInfo(r.Context(), auth))
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
		opts.Hooks = twirp.ChainHooks(
			authCheckHook(""), opts.Hooks,
		)

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
		opts.Hooks = twirp.ChainHooks(
			authCheckHook("schema_admin"), opts.Hooks,
		)

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
		opts.Hooks = twirp.ChainHooks(
			authCheckHook("workflow_admin"), opts.Hooks,
		)

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
		opts.Hooks = twirp.ChainHooks(
			authCheckHook(""), opts.Hooks,
		)

		api := repository.NewReportsServer(
			service,
			twirp.WithServerJSONSkipDefaults(true),
			twirp.WithServerHooks(opts.Hooks),
		)

		registerAPI(router, opts, api)

		return nil
	}
}

func WithTokenEndpoint(
	jwtKey *ecdsa.PrivateKey, sharedSecret string,
) RouterOption {
	return func(router *httprouter.Router) error {
		router.POST("/token", internal.RHandleFunc(func(
			w http.ResponseWriter, r *http.Request, _ httprouter.Params,
		) error {
			err := r.ParseForm()
			if err != nil {
				return internal.HTTPErrorf(http.StatusBadRequest,
					"invalid form data: %v", err)
			}

			form := r.Form

			switch form.Get("grant_type") {
			case "password":
			case "refresh_token":
				if form.Get("refresh_token") == "" {
					return internal.HTTPErrorf(http.StatusBadRequest,
						"missing 'refresh_token'")
				}

				rData, err := base64.RawStdEncoding.DecodeString(
					form.Get("refresh_token"))
				if err != nil {
					return internal.HTTPErrorf(http.StatusBadRequest,
						"invalid refresh token: %v", err)
				}

				f, err := url.ParseQuery(string(rData))
				if err != nil {
					return internal.HTTPErrorf(http.StatusBadRequest,
						"invalid refresh contents: %v", err)
				}

				form = f
			default:
				return internal.HTTPErrorf(http.StatusBadRequest,
					"we only support the \"password\" and \"refresh_token\" grant_type")
			}

			username := form.Get("username")
			if username == "" {
				return internal.HTTPErrorf(http.StatusBadRequest,
					"missing 'username'")
			}

			password := form.Get("password")
			if password != sharedSecret {
				return internal.HTTPErrorf(http.StatusUnauthorized,
					"invalid password")
			}

			scope := form.Get("scope")

			name, uriPart, ok := strings.Cut(username, " <")
			if !ok || !strings.HasSuffix(uriPart, ">") {
				return internal.HTTPErrorf(http.StatusBadRequest,
					"username must be in the format \"Name <some://sub/uri>\"")
			}

			subURI := strings.TrimSuffix(uriPart, ">")
			expiresIn := 10 * time.Minute

			sub, units, _ := strings.Cut(subURI, ", ")

			claims := JWTClaims{
				RegisteredClaims: jwt.RegisteredClaims{
					ExpiresAt: jwt.NewNumericDate(
						time.Now().Add(expiresIn)),
					Issuer:  "test",
					Subject: sub,
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
				return internal.HTTPErrorf(http.StatusInternalServerError,
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
				return internal.HTTPErrorf(http.StatusInternalServerError,
					"failed encode token response")
			}

			return nil
		}))

		return nil
	}
}

func authCheckHook(scope string) *twirp.ServerHooks {
	return &twirp.ServerHooks{
		RequestRouted: func(
			ctx context.Context,
		) (context.Context, error) {
			// Require auth for all methods
			auth, ok := GetAuthInfo(ctx)
			if !ok {
				return ctx, twirp.Unauthenticated.Error(
					"no anonymous access allowed")
			}

			if scope != "" && !auth.Claims.HasScope(scope) {
				return ctx, twirp.PermissionDenied.Errorf(
					"the scope %q is required", scope)
			}

			return ctx, nil
		},
	}
}

func requireAnyScope(ctx context.Context, scopes ...string) error {
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return twirp.Unauthenticated.Error(
			"no anonymous access allowed")
	}

	if !auth.Claims.HasAnyScope(scopes...) {
		return twirp.PermissionDenied.Errorf(
			"one of the the scopes %s is required",
			strings.Join(scopes, ", "))
	}

	return nil
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
