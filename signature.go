package docformat

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rakutentech/jwk-go/jwk"
)

type SigningKey struct {
	Spec *jwk.KeySpec `json:"spec"`

	// Key timestamps
	IssuedAt  time.Time `json:"iat"`
	NotBefore time.Time `json:"nbf"`
}

type SigningKeySet struct {
	m    sync.RWMutex
	keys []SigningKey `json:"keys"`
}

func (s *SigningKeySet) Replace(keys []SigningKey) {
	s.m.Lock()
	defer s.m.Unlock()

	s.keys = keys
}

func (s *SigningKeySet) LatestKey() *SigningKey {
	s.m.RLock()
	defer s.m.RUnlock()

	var l *SigningKey

	for i := range s.keys {
		if l == nil {
			l = &s.keys[i]
			continue
		}

		if s.keys[i].IssuedAt.After(l.IssuedAt) {
			l = &s.keys[i]
		}
	}

	return l
}

func (s *SigningKeySet) CurrentKey(t time.Time) *SigningKey {
	s.m.RLock()
	defer s.m.RUnlock()

	var c *SigningKey

	for i := range s.keys {
		valid := t.After(s.keys[i].NotBefore)

		if c == nil {
			if valid {
				c = &s.keys[i]
			}
			continue
		}

		if valid && s.keys[i].IssuedAt.After(c.IssuedAt) {
			c = &s.keys[i]
		}
	}

	return c
}

func (s *SigningKeySet) GetKeyByID(kid string) *SigningKey {
	s.m.RLock()
	defer s.m.RUnlock()

	for i := range s.keys {
		if s.keys[i].Spec.KeyID == kid {
			return &s.keys[i]
		}
	}

	return nil
}

type ArchiveSignature struct {
	KeyID     string
	Hash      [sha256.Size]byte
	Signature []byte
}

func NewArchiveSignature(
	key *SigningKey, hash [sha256.Size]byte,
) (*ArchiveSignature, error) {
	private, ok := key.Spec.Key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("key is not a private ECDSA key")
	}

	sig, err := ecdsa.SignASN1(rand.Reader, private, hash[:])
	if err != nil {
		return nil, fmt.Errorf("failed to sign hash: %w", err)
	}

	return &ArchiveSignature{
		KeyID:     key.Spec.KeyID,
		Hash:      hash,
		Signature: sig,
	}, nil
}

func (as *ArchiveSignature) String() string {
	hash := base64.RawURLEncoding.EncodeToString(as.Hash[:])
	sig := base64.RawURLEncoding.EncodeToString(as.Signature)

	return fmt.Sprintf("v1.%s.%s.%s", as.KeyID, hash, sig)
}

func (as *ArchiveSignature) Verify(key *SigningKey) error {
	if key.Spec.KeyID != as.KeyID {
		return fmt.Errorf("key ID mismatch")
	}

	var pub *ecdsa.PublicKey

	switch k := key.Spec.Key.(type) {
	case *ecdsa.PrivateKey:
		pub = &k.PublicKey
	case *ecdsa.PublicKey:
		pub = k
	default:
		return errors.New("not a valid key type")
	}

	ok := ecdsa.VerifyASN1(pub, as.Hash[:], as.Signature)
	if !ok {
		return errors.New("invalid signature")
	}

	return nil
}

func ParseArchiveSignature(sg string) (*ArchiveSignature, error) {
	segs := strings.Split(sg, ".")

	if len(segs) != 4 {
		return nil, errors.New(
			"a signature must have 4 parts separated by '.'")
	}

	if segs[0] != "v1" {
		return nil, fmt.Errorf("unknown signature version %q", segs[0])
	}

	if len(segs[1]) == 0 {
		return nil, errors.New("missing key ID")
	}

	hash, err := base64.RawURLEncoding.DecodeString(segs[2])
	if err != nil {
		return nil, fmt.Errorf("invalid hash data: %w", err)
	}

	if len(hash) != sha256.Size {
		return nil, fmt.Errorf("invalid hash length %d, must be %d",
			len(hash), sha256.Size)
	}

	signature, err := base64.RawURLEncoding.DecodeString(segs[3])
	if err != nil {
		return nil, fmt.Errorf("invalid signature data: %w", err)
	}

	sig := ArchiveSignature{
		KeyID:     segs[1],
		Hash:      [sha256.Size]byte(hash),
		Signature: signature,
	}

	return &sig, nil
}
