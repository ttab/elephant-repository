package repository

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"
	"github.com/ttab/elephant/internal"
	"github.com/ttab/elephant/rpc/repository"
	"github.com/twitchtv/twirp"
)

func RunServer(
	ctx context.Context, addr string,
	opts ...ServerOption,
) error {
	router := httprouter.New()

	server := http.Server{
		Addr:    addr,
		Handler: router,
	}

	for _, opt := range opts {
		err := opt(router)
		if err != nil {
			return err
		}
	}

	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()

	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	} else if err != nil {
		return err
	}

	return nil
}

type ServerOption func(router *httprouter.Router) error

func WithAPIServer(
	logger *logrus.Logger,
	jwtKey *ecdsa.PrivateKey, server repository.Documents,
) ServerOption {
	return func(router *httprouter.Router) error {
		return apiServer(logger, router, jwtKey, server)
	}
}

func apiServer(
	logger *logrus.Logger,
	router *httprouter.Router, jwtKey *ecdsa.PrivateKey,
	server repository.Documents,
) error {
	if jwtKey == nil {
		var err error

		jwtKey, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		if err != nil {
			return fmt.Errorf("failed to generate key: %w", err)
		}

		keyData, err := x509.MarshalECPrivateKey(jwtKey)
		if err != nil {
			return fmt.Errorf(
				"failed to marshal private key: %w", err)
		}

		logger.WithField(
			"key", base64.RawURLEncoding.EncodeToString(keyData),
		).Warn(
			"running with temporary signing key for JWTs, tokens won't be valid across intances or restarts")
	}

	api := repository.NewDocumentsServer(
		server,
		twirp.WithServerJSONSkipDefaults(true),
		twirp.WithServerHooks(&twirp.ServerHooks{
			RequestReceived: func(
				ctx context.Context,
			) (context.Context, error) {
				// Require auth for all methods
				_, ok := GetAuthInfo(ctx)
				if !ok {
					return ctx, twirp.Unauthenticated.Error(
						"no anonymous access allowed")
				}

				return ctx, nil
			},
		}),
	)

	router.POST(api.PathPrefix()+":method", internal.RHandleFunc(func(
		w http.ResponseWriter, r *http.Request, _ httprouter.Params,
	) error {
		auth, err := AuthInfoFromHeader(&jwtKey.PublicKey,
			r.Header.Get("Authorization"))
		if err != nil {
			return internal.HTTPErrorf(http.StatusUnauthorized,
				"invalid authorization method: %v", err)
		}

		if auth != nil {
			r = r.WithContext(SetAuthInfo(r.Context(), auth))
		}

		api.ServeHTTP(w, r)

		return nil
	}))

	router.POST("/token", internal.RHandleFunc(func(
		w http.ResponseWriter, r *http.Request, _ httprouter.Params,
	) error {
		err := r.ParseForm()
		if err != nil {
			return internal.HTTPErrorf(http.StatusBadRequest,
				"invalid form data: %v", err)
		}

		form := r.Form

		if form.Get("grant_type") != "password" {
			return internal.HTTPErrorf(http.StatusBadRequest,
				"we only support the \"password\" grant_type")
		}

		username := form.Get("username")
		if username == "" {
			return internal.HTTPErrorf(http.StatusBadRequest,
				"missing 'username'")
		}

		scope := form.Get("scope")

		name, uriPart, ok := strings.Cut(username, " <")
		if !ok || !strings.HasSuffix(uriPart, ">") {
			return internal.HTTPErrorf(http.StatusBadRequest,
				"username must be in the format \"Name <some://sub/uri>\"")
		}

		subURI := strings.TrimSuffix(uriPart, ">")
		expiresIn := 10 * time.Minute

		sub, units, ok := strings.Cut(subURI, ", ")

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
			TokenType:   "Bearer",
			ExpiresIn:   int(expiresIn.Seconds()),
		})
		if err != nil {
			return internal.HTTPErrorf(http.StatusInternalServerError,
				"failed encode token response")
		}

		return nil
	}))

	return nil
}
