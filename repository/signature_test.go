package repository_test

import (
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine/test"
)

const testKeys = `
{
  "keys": [
    {
      "spec": {
        "kid": "1",
        "kty": "EC",
        "crv": "P-384",
        "x": "ZUDlr2yupapraQCNJoQFkjWzRUWOyGhDgAyH13JXUSpOnbAXj8nVTvnkb4sylvBn",
        "y": "0yBvbGVpI6zedg6-L557otpzAismJtUo3sT0TXNZ3V1cvK1kolF676AYrWr4O5La",
        "d": "mAitE7EdUT309X6p7_HhEznZ4drWLIgt0-mo4rapGLB25HPyM2E0VV7ubbuImnwW"
      },
      "iat": "2023-02-07T09:41:28.982611157+01:00",
      "nbf": "2023-02-07T09:41:28.978040057+01:00",
      "naf": "2023-08-07T09:41:28.978040057+01:00"
    },
    {
      "spec": {
        "kid": "2",
        "kty": "EC",
        "crv": "P-384",
        "x": "v-zN5KxfjvEN_u6zsSzQWbwIq2fTTT2kF_6j_f49fCZe95bQyBUNMlke1BQBsyvC",
        "y": "IWcfPxJbSnvJqqNZJD3VbR2IyVYlJBbwm4zwvGJrRjeRekRnNwqdoJgzrFF1jcmD",
        "d": "JzE_htB--5rJKvG6ju3-yG9ryLJL0qQnwiLvfesuktsxukhULklo7qvl0SDLFniS"
      },
      "iat": "2023-02-07T09:41:36.937793126+01:00",
      "nbf": "2023-03-09T09:41:36.937433654+01:00",
      "naf": "2023-09-09T09:41:36.937433654+01:00"
    },
    {
      "spec": {
        "kid": "3",
        "kty": "EC",
        "crv": "P-384",
        "x": "u-l15_9AxxZ1_mBXH19nFXIn26FXkTWYnduAa4YxgwyC6GejOe5Fd8Nd5KC0DiYE",
        "y": "hpVgQrkuq2rrjL3AbC-DekT0qDGDtqiD-Clyxo8GriEj9rFWebdQd0goSA_lBAwt",
        "d": "L7QQrqufQuI7RwLyl7x-dpLEAwNxJdu_11wNeWwdA8eY61ziJZ0zI7XxRCUd8guE"
      },
      "iat": "2023-02-07T10:40:56.863462372+01:00",
      "nbf": "2023-04-09T10:40:56.863106953+01:00",
      "naf": "2023-10-09T10:40:56.863106953+01:00"
    }
  ]
}`

//nolint:lll
const (
	testSignature = `v1.1.vZr1mhSxTiT_LNBP4S8eXfGUrmazfdjVZRGWoV2bhYE.MGUCMQDAPPOW2qn7XtSEsvr0Iy5u9n12vZZTZVruKj15AC7x5u2uzzbgCIXHvgjP9rksNS4CMAH_XbIUBXxtZDr-rRL-q53w0UnGZmszWp6W__reZuZ5kazFXjoc_4Fr1uA1T-kGcw`
	badSignature  = `v1.1.vZr1mhSxTiT_LNBP4S8eXfGUrmazfdjVZRGWoV2bhYE.MGYCMQDqIFIWo2gE9n2Hp7mzsfvFK2E-i0A-sa6pJSXSbpwiUjwi32OIsfFPHdO9_C-bescCMQCGr_xCyqGk1vqyt3q4Qxa-SpcK9ESu4gYKeeBx86kndRCkT7pBTL8VezOJ-W2K8FU`
)

func getTestKeys(t test.TestingT) *repository.SigningKeySet {
	t.Helper()

	var set repository.SigningKeySet

	err := json.Unmarshal([]byte(testKeys), &set)
	test.Must(t, err, "unmarshal test keys")

	return &set
}

func TestArchiveSignature_GenerateAndVerify(t *testing.T) {
	keys := getTestKeys(t)

	keySelection := map[string]time.Time{
		"1": time.Date(2023, 2, 10, 0, 0, 0, 0, time.UTC),
		"2": time.Date(2023, 3, 10, 0, 0, 0, 0, time.UTC),
	}

	for kid, t0 := range keySelection {
		key := keys.CurrentKey(t0)
		test.NotNil(t, key,
			"expected there to be a current key for %v", t0)
		test.Equal(t, kid, key.Spec.KeyID, "correct key selected")

		someData := []byte(`{"my":"json"}`)
		hashData := sha256.Sum256(someData)

		sig, err := repository.NewArchiveSignature(key, hashData)
		test.Must(t, err, "generate signature")
		test.Equal(t, sig.KeyID, key.Spec.KeyID,
			"correct key ID declared by signature")

		err = sig.Verify(key)
		test.Must(t, err, "verify generated signature")
	}
}

func TestArchiveSignature_ParseAndVerify(t *testing.T) {
	keys := getTestKeys(t)

	sig, err := repository.ParseArchiveSignature(testSignature)
	test.Must(t, err, "parse signature")

	key := keys.GetKeyByID(sig.KeyID)
	test.NotNil(t, key, "look up key")

	err = sig.Verify(key)
	test.Must(t, err, "verify parsed signature")
}

func TestArchiveSignature_ParseAndDetectBadSignature(t *testing.T) {
	keys := getTestKeys(t)

	sig, err := repository.ParseArchiveSignature(badSignature)
	test.Must(t, err, "parse signature")

	key := keys.GetKeyByID(sig.KeyID)
	test.NotNil(t, key, "look up key")

	err = sig.Verify(key)
	test.MustNot(t, err, "expect to get error for bad signature")
}

func FuzzArchiveSignatureParsing(f *testing.F) {
	keys := getTestKeys(f)
	time := time.Date(2023, 2, 10, 0, 0, 0, 0, time.UTC)

	key := keys.CurrentKey(time)
	test.NotNil(f, key, "get current key")

	f.Add(testSignature)
	f.Add(badSignature)
	f.Add("v2.1234.asdiasdjasdijwqei")
	f.Add("")
	f.Add("4")
	f.Add("v1...")
	f.Add("v1.2..")
	f.Add("v1.2.vZr1mhSxTiT_LNBP4S8eXfGUrmazfdjVZRGWoV2bhYE.")

	f.Fuzz(func(_ *testing.T, a string) {
		s, err := repository.ParseArchiveSignature(a)
		if err != nil {
			return
		}

		_ = s.Verify(key)
	})
}
