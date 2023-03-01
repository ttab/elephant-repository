package test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/ttab/elephant/repository"
)

func NewSigningKey() (*ecdsa.PrivateKey, error) {
	jwtKey, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}

	return jwtKey, nil
}

func StandardClaims(t *testing.T, scope string, units ...string) repository.JWTClaims {
	t.Helper()

	return repository.JWTClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(
				time.Now().Add(10 * time.Minute)),
			Issuer:  "test",
			Subject: "user://test/" + strings.ToLower(t.Name()),
		},
		Name:  t.Name(),
		Units: units,
		Scope: scope,
	}
}

func AccessKey(key *ecdsa.PrivateKey, claims repository.JWTClaims) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodES384, claims)

	ss, err := token.SignedString(key)
	if err != nil {
		return "", fmt.Errorf("failed to sign JWT token: %w", err)
	}

	return ss, nil
}