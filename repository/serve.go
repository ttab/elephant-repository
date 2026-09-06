package repository

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-api/repository/repositoryconnect"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine"
)

// RegisterAPIs mounts the four RPC services on the API server, once on the
// Twirp paths and once on the Connect ones. Both mounts wrap the same service
// implementation and are configured from the same elephantine.ServiceOptions,
// so authentication, logging and metrics are identical by construction rather
// than by two chains that have to be kept in step.
//
// Production and the test suite both go through this function, so a test
// measures the server shape the service actually serves.
func RegisterAPIs(
	srv *elephantine.APIServer,
	opt elephantine.ServiceOptions,
	documents repository.Documents,
	schemas repository.Schemas,
	workflows repository.Workflows,
	metrics repository.Metrics,
) {
	srv.RegisterAPIs(opt,
		repository.NewDocumentsServer(documents, opt.ServerOptions()),
		repository.NewSchemasServer(schemas, opt.ServerOptions()),
		repository.NewWorkflowsServer(workflows, opt.ServerOptions()),
		repository.NewMetricsServer(metrics, opt.ServerOptions()),
	)

	// The generated Connect constructors return the service root
	// ("/elephant.repository.Documents/") and a handler that serves the
	// Connect, gRPC and gRPC-Web protocols on it.
	path, handler := repositoryconnect.NewDocumentsServiceHandler(
		documents, opt.HandlerOptions()...)
	srv.RegisterConnect(path, handler, opt)

	path, handler = repositoryconnect.NewSchemasServiceHandler(
		schemas, opt.HandlerOptions()...)
	srv.RegisterConnect(path, handler, opt)

	path, handler = repositoryconnect.NewWorkflowsServiceHandler(
		workflows, opt.HandlerOptions()...)
	srv.RegisterConnect(path, handler, opt)

	path, handler = repositoryconnect.NewMetricsServiceHandler(
		metrics, opt.HandlerOptions()...)
	srv.RegisterConnect(path, handler, opt)
}

// RegisterSSE mounts the event stream on the API server, behind the same
// authentication middleware as the RPC services.
//
// The token may be passed as a "token" query parameter as well as in the
// Authorization header: an EventSource cannot set headers, so a browser client
// has nowhere else to put it. It is copied into the header before the
// middleware runs, so there is one authentication path and not two.
func RegisterSSE(
	srv *elephantine.APIServer,
	opt elephantine.ServiceOptions,
	handler http.Handler,
) {
	srv.Mux.Handle("GET /sse", elephantine.HTTPErrorHandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) error {
		token := r.URL.Query().Get("token")
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}

		if opt.AuthMiddleware == nil {
			handler.ServeHTTP(w, r)

			return nil
		}

		err := opt.AuthMiddleware(w, r, handler)
		if err != nil {
			return fmt.Errorf("authenticate the request: %w", err)
		}

		return nil
	}))
}

// RegisterWebsocket mounts the websocket endpoint on the API server. It
// deliberately bypasses the authentication middleware: the socket token in the
// path is signed with the server's own socket key and is verified by the
// handler, and the session authenticates with a JWT once it is up.
func RegisterWebsocket(
	srv *elephantine.APIServer,
	handler http.Handler,
) {
	srv.Mux.Handle("GET /websocket/{token}", handler)
}

// RegisterSigningKeys mounts the public endpoint that exposes the archive
// signing public keys as a JWKS document. It bypasses the authentication
// middleware by design: independent verification of the archive has to be
// possible without a token.
func RegisterSigningKeys(
	srv *elephantine.APIServer,
	pool *pgxpool.Pool,
) {
	srv.Mux.Handle("GET /signing-keys", elephantine.HTTPErrorHandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) error {
		q := postgres.New(pool)

		keys, err := q.GetSigningKeys(r.Context())
		if err != nil {
			return fmt.Errorf("get signing keys: %w", err)
		}

		entries := make([]json.RawMessage, 0, len(keys))

		for i := range keys {
			var sk SigningKey

			err := json.Unmarshal(keys[i].Spec, &sk)
			if err != nil {
				return fmt.Errorf(
					"unmarshal key %q: %w",
					keys[i].Kid, err)
			}

			raw, err := MarshalPublicSigningKey(sk)
			if err != nil {
				return fmt.Errorf(
					"marshal public key %q: %w",
					keys[i].Kid, err)
			}

			entries = append(entries, raw)
		}

		resp := struct {
			Keys []json.RawMessage `json:"keys"`
		}{
			Keys: entries,
		}

		data, err := json.MarshalIndent(resp, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal response: %w", err)
		}

		w.Header().Set("Content-Type", "application/json")

		_, err = w.Write(data)
		if err != nil {
			return fmt.Errorf("write response: %w", err)
		}

		return nil
	}))
}

// MarshalPublicSigningKey marshals a SigningKey into a public JWK JSON
// representation with iat/nbf/exp timestamps.
func MarshalPublicSigningKey(sk SigningKey) (json.RawMessage, error) {
	pub, err := sk.Spec.PublicOnly()
	if err != nil {
		return nil, fmt.Errorf(
			"extract public key %q: %w",
			sk.Spec.KeyID, err)
	}

	jwkJSON, err := pub.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf(
			"marshal key %q: %w",
			sk.Spec.KeyID, err)
	}

	var entry map[string]json.RawMessage

	err = json.Unmarshal(jwkJSON, &entry)
	if err != nil {
		return nil, fmt.Errorf(
			"re-parse key %q: %w",
			sk.Spec.KeyID, err)
	}

	entry["iat"] = marshalUnixTime(sk.IssuedAt)
	entry["nbf"] = marshalUnixTime(sk.NotBefore)
	entry["exp"] = marshalUnixTime(sk.NotAfter)

	raw, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf(
			"marshal key entry %q: %w",
			sk.Spec.KeyID, err)
	}

	return raw, nil
}

func marshalUnixTime(t time.Time) json.RawMessage {
	if t.IsZero() {
		return json.RawMessage("0")
	}

	return json.RawMessage(fmt.Sprintf("%d", t.Unix()))
}
