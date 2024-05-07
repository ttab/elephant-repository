package test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ttab/elephantine"
)

func NewSigningKey() (*ecdsa.PrivateKey, error) {
	jwtKey, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	return jwtKey, nil
}

func Claims(t *testing.T, user string, scope string, units ...string) elephantine.JWTClaims {
	t.Helper()

	return elephantine.JWTClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(
				time.Now().Add(10 * time.Minute)),
			Issuer:  "test",
			Subject: "user://test/" + user,
		},
		Name:  t.Name(),
		Units: units,
		Scope: scope,
	}
}

func StandardClaims(t *testing.T, scope string, units ...string) elephantine.JWTClaims {
	t.Helper()

	return Claims(t, strings.ToLower(t.Name()), scope, units...)
}

func AccessKey(key *ecdsa.PrivateKey, claims elephantine.JWTClaims) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodES384, claims)

	ss, err := token.SignedString(key)
	if err != nil {
		return "", fmt.Errorf("failed to sign JWT token: %w", err)
	}

	return ss, nil
}
