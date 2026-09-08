package repository_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/ttab/elephantine/test"
)

// TestIntegrationEndpointAuthentication pins how the three endpoints that are
// not RPC authenticate. They are mounted on the shared elephantine.APIServer
// now, so /sse goes through the same fail-closed middleware as the RPC mounts
// while /signing-keys stays public. A regression in either one is a security
// hole, not a cosmetic slip.
func TestIntegrationEndpointAuthentication(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	logger := slog.New(test.NewLogHandler(t, slog.LevelError))

	tc := testingAPIServer(t, logger, testingServerOptions{})

	get := func(t *testing.T, path string) (int, string) {
		t.Helper()

		req, err := http.NewRequestWithContext(t.Context(),
			http.MethodGet, tc.Server.URL+path, nil)
		test.Mustf(t, err, "create the request")

		res, err := tc.Server.Client().Do(req)
		test.Mustf(t, err, "perform the request")

		defer func() {
			_ = res.Body.Close()
		}()

		body, err := io.ReadAll(res.Body)
		test.Mustf(t, err, "read the response body")

		return res.StatusCode, string(body)
	}

	// The token is read from the query parameter, so a bad one is a bad
	// token and not a missing one.
	t.Run("sse with an invalid token", func(t *testing.T) {
		status, body := get(t, "/sse?token=not-a-token")

		test.Equalf(t, http.StatusUnauthorized, status,
			"refuse an invalid token")

		var payload struct {
			Code string `json:"code"`
		}

		err := json.Unmarshal([]byte(body), &payload)
		test.Mustf(t, err, "unmarshal the error body %q", body)

		test.Equalf(t, "unauthenticated", payload.Code,
			"answer with an unauthenticated error")
	})

	t.Run("sse without a token", func(t *testing.T) {
		status, _ := get(t, "/sse")

		test.Equalf(t, http.StatusUnauthorized, status,
			"refuse a request with no token")
	})

	t.Run("signing keys are public", func(t *testing.T) {
		status, body := get(t, "/signing-keys")

		test.Equalf(t, http.StatusOK, status,
			"serve the signing keys without a token")

		var payload struct {
			Keys []json.RawMessage `json:"keys"`
		}

		err := json.Unmarshal([]byte(body), &payload)
		test.Mustf(t, err, "unmarshal the JWKS body %q", body)
	})
}
