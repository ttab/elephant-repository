package repository_test

import (
	"log/slog"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/golang-jwt/jwt/v5"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-api/repository/repositoryconnect"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
)

// delegateDocumentsClient returns a documents client that presents token, for
// tokens the test signs itself rather than through authClient.
func (tc *TestContext) delegateDocumentsClient(
	t *testing.T, token string,
) repository.Documents {
	t.Helper()

	client := rawTokenClient(tc.client, token)

	if tc.Stack == stackConnect {
		return repositoryconnect.NewDocumentsServiceClient(
			client, tc.Server.URL)
	}

	return repository.NewDocumentsProtobufClient(tc.Server.URL, client)
}

// TestIntegrationDelegateTokens checks that a token narrowed by
// elephant-delegate is accepted when the delegate is a trusted issuer, and that
// one bound to a device key is refused, since the repository verifies no DPoP
// proofs.
func TestIntegrationDelegateTokens(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))
	ctx := t.Context()
	tc := testingAPIServer(t, logger, testingServerOptions{})

	const (
		user = "owner"
		docA = "2f6b1c3d-4e5a-4b6c-8d7e-9f0a1b2c3d4e"
		docB = "6a7b8c9d-0e1f-4a2b-9c3d-4e5f6a7b8c9d"
	)

	owner := tc.DocumentsClient(t,
		itest.Claims(t, user, "doc_read doc_write"))

	for _, id := range []string{docA, docB} {
		_, err := owner.Update(ctx, &repository.UpdateRequest{
			Uuid:     id,
			Document: baseDocument(id, "article://test/"+id),
		})
		test.Mustf(t, err, "create document %s", id)
	}

	// narrowed returns the claims of a session token narrowed by the
	// delegate to reading docA: the session's subject, the delegate as
	// issuer, the repository as audience.
	narrowed := func() elephantine.JWTClaims {
		claims := itest.Claims(t, user, "doc_read:"+docA)

		claims.Issuer = itest.DelegateIssuer
		claims.Audience = jwt.ClaimStrings{itest.DelegateAudience}
		claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(5 * time.Minute))

		return claims
	}

	client := func(claims elephantine.JWTClaims) repository.Documents {
		token, err := itest.DelegateToken(tc.DelegateKey, claims)
		test.Mustf(t, err, "sign delegate token")

		return tc.delegateDocumentsClient(t, token)
	}

	t.Run("Narrowed", func(t *testing.T) {
		c := client(narrowed())

		_, err := c.Get(ctx, &repository.GetDocumentRequest{Uuid: docA})
		test.Mustf(t, err, "read the document the scope names")

		_, err = c.Get(ctx, &repository.GetDocumentRequest{Uuid: docB})
		test.IsRPCError(t, err, connect.CodePermissionDenied)
	})

	refused := map[string]func(c *elephantine.JWTClaims){
		"AnotherAudience": func(c *elephantine.JWTClaims) {
			c.Audience = jwt.ClaimStrings{"elephant-stream"}
		},
		"NoAudience": func(c *elephantine.JWTClaims) {
			c.Audience = nil
		},
		"BoundToADeviceKey": func(c *elephantine.JWTClaims) {
			c.Confirmation = &elephantine.Confirmation{
				JKT: "0ZcOCORZNYy-DWpqq30jZyJGHTN0d2HglBV3uiguA4I",
			}
		},
		// A token the delegate signed is checked against the delegate's
		// keys whatever issuer it names, so claiming to be the identity
		// provider gets it nothing.
		"ForgedIssuer": func(c *elephantine.JWTClaims) {
			c.Issuer = "test"
		},
	}

	for name, mutate := range refused {
		t.Run(name, func(t *testing.T) {
			claims := narrowed()

			mutate(&claims)

			_, err := client(claims).Get(ctx,
				&repository.GetDocumentRequest{Uuid: docA})
			test.IsRPCError(t, err, connect.CodeUnauthenticated)
		})
	}
}
