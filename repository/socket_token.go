package repository

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

const (
	p384Size   = 384
	uint64size = 8
)

func VerifySocketToken(
	token string, key *ecdsa.PublicKey,
) (*SocketToken, error) {
	if key.Curve.Params().BitSize != p384Size {
		return nil, errors.New("invalid key")
	}

	payload, sig, ok := strings.Cut(token, ".")
	if !ok {
		return nil, errors.New("invalid token")
	}

	data, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return nil, fmt.Errorf("invalid token payload: %w", err)
	}

	if len(data) != sha256.Size+16 {
		return nil, errors.New("invalid payload size")
	}

	sigData, err := base64.RawStdEncoding.DecodeString(sig)
	if err != nil {
		return nil, fmt.Errorf("invalid signature data: %w", err)
	}

	keySize := len(sigData) / 2

	r := big.NewInt(0).SetBytes(sigData[:keySize])
	s := big.NewInt(0).SetBytes(sigData[keySize:])

	hasher := sha256.New()
	hasher.Write(data)

	// Verify the signature
	validSig := ecdsa.Verify(key, hasher.Sum(nil), r, s)
	if !validSig {
		return nil, errors.New("invalid signature")
	}

	var off int

	// Read ID.
	id := binary.BigEndian.Uint64(data[off:])
	off += uint64size

	// Read expiry.
	ts := binary.BigEndian.Uint64(data[off:])
	off += uint64size

	// Read the subject hash.
	subjectHash := [sha256.Size]byte(data[off : off+sha256.Size])

	return &SocketToken{
		ID:          id,
		SubjectHash: subjectHash,
		Expires:     time.Unix(int64(ts), 0), //nolint: gosec
	}, nil
}

func NewSocketToken(subject string, expiry time.Time) *SocketToken {
	subHash := sha256.Sum256([]byte(subject))

	id := make([]byte, 8)

	_, _ = rand.Read(id)

	return &SocketToken{
		ID:          binary.BigEndian.Uint64(id),
		SubjectHash: subHash,
		Expires:     expiry,
	}
}

type SocketToken struct {
	ID          uint64
	SubjectHash [sha256.Size]byte
	Expires     time.Time
}

func (t *SocketToken) ValidFor(subject string) bool {
	subHash := sha256.Sum256([]byte(subject))

	return subHash == t.SubjectHash
}

func (t *SocketToken) Sign(key *ecdsa.PrivateKey) (string, error) {
	if key.Curve.Params().BitSize != p384Size {
		return "", errors.New("invalid key")
	}

	ts := t.Expires.Unix()

	buf := make([]byte, sha256.Size+16)

	var off int

	// Write ID.
	binary.BigEndian.PutUint64(buf[off:], t.ID)
	off += uint64size

	// Write the expiry.
	binary.BigEndian.PutUint64(buf[off:], uint64(ts)) //nolint: gosec
	off += uint64size

	// Write the subject hash.
	copy(buf[off:], t.SubjectHash[:])

	hasher := sha256.New()

	_, _ = hasher.Write(buf)

	var sig []byte

	r, s, err := ecdsa.Sign(rand.Reader, key, hasher.Sum(nil))
	if err == nil {
		curveBits := key.Curve.Params().BitSize

		keyBytes := curveBits / 8
		if curveBits%8 > 0 {
			keyBytes++
		}

		// We serialize the outputs (r and s) into big-endian byte arrays
		// padded with zeros on the left to make sure the sizes work out.
		// Output must be 2*keyBytes long.
		out := make([]byte, 2*keyBytes)
		r.FillBytes(out[0:keyBytes]) // r is assigned to the first half of output.
		s.FillBytes(out[keyBytes:])  // s is assigned to the second half of output.

		sig = out
	}

	return base64.RawURLEncoding.EncodeToString(buf) + "." +
		base64.RawStdEncoding.EncodeToString(sig), nil
}
