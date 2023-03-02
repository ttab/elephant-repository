package repository

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/jellydator/ttlcache/v3"
)

type JWTClaims struct {
	jwt.RegisteredClaims

	Name  string   `json:"sub_name"`
	Scope string   `json:"scope"`
	Units []string `json:"units,omitempty"`
}

func (c JWTClaims) HasScope(name string) bool {
	scopes := strings.Split(c.Scope, " ")

	for i := range scopes {
		if scopes[i] == name {
			return true
		}
	}

	return false
}

func (c JWTClaims) HasAnyScope(names ...string) bool {
	scopes := strings.Split(c.Scope, " ")

	for i := range scopes {
		for j := range names {
			if scopes[i] == names[j] {
				return true
			}
		}
	}

	return false
}

func (c JWTClaims) Valid() error {
	return c.RegisteredClaims.Valid() //nolint:wrapcheck
}

type ctxKey int

const authInfoCtxKey ctxKey = 1

type AuthInfo struct {
	Claims JWTClaims
}

var ErrNoAuthorization = errors.New("no authorization provided")

// TODO: this global state is obviously bad. The auth method and any caches
// should be instantiated at application instantiation.
var cache = ttlcache.New[string, AuthInfo]()

func AuthInfoFromHeader(key *ecdsa.PublicKey, authorization string) (*AuthInfo, error) {
	if authorization == "" {
		return nil, ErrNoAuthorization
	}

	tokenType, token, _ := strings.Cut(authorization, " ")

	tokenType = strings.ToLower(tokenType)
	if tokenType != "bearer" {
		return nil, errors.New("only bearer tokens are supported")
	}

	item := cache.Get(token)
	if item != nil && !item.IsExpired() {
		value := item.Value()

		return &value, nil
	}

	var claims JWTClaims

	_, err := jwt.ParseWithClaims(token, &claims,
		func(t *jwt.Token) (interface{}, error) {
			return key, nil
		},
		jwt.WithValidMethods([]string{jwt.SigningMethodES384.Name}))
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	if claims.Issuer != "test" {
		return nil, fmt.Errorf("invalid issuer %q", claims.Issuer)
	}

	auth := AuthInfo{
		Claims: claims,
	}

	cache.Set(token, auth, time.Until(auth.Claims.ExpiresAt.Time))

	return &auth, nil
}

func SetAuthInfo(ctx context.Context, info *AuthInfo) context.Context {
	return context.WithValue(ctx, authInfoCtxKey, info)
}

func GetAuthInfo(ctx context.Context) (*AuthInfo, bool) {
	info, ok := ctx.Value(authInfoCtxKey).(*AuthInfo)

	return info, ok && info != nil
}

type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
}
