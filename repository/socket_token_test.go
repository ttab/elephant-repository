package repository_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"
	"time"

	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine/test"
)

func TestSocketToken(t *testing.T) {
	keyA, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	test.Must(t, err, "create key A")

	keyB, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	test.Must(t, err, "create key B")

	subjectA := "core://user/a"
	subjectB := "core://user/b"

	expires := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)

	token := repository.NewSocketToken(subjectA, expires)

	signedToken, err := token.Sign(keyA)
	test.Must(t, err, "sign token")

	test.Equal(t, 183, len(signedToken), "token must be 183 characters long")

	verified, err := repository.VerifySocketToken(signedToken, &keyA.PublicKey)
	test.Must(t, err, "verify socket token")

	_, err = repository.VerifySocketToken(signedToken, &keyB.PublicKey)
	test.MustNot(t, err, "verify socket token with wrong key")

	if !verified.Expires.Equal(token.Expires) {
		t.Error("verified token expiry differs from original token")
	}

	if !verified.ValidFor(subjectA) {
		t.Error("verified token is not valid for subject A")
	}

	if verified.ValidFor(subjectB) {
		t.Error("verified token should not be valid for subject B")
	}
}
