package repository_test

import (
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/ttab/elephant-api/newsdoc"
	rpc_repository "github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine/test"
)

func TestDeprecations(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelError))

	dataDir := filepath.Join("..", "testdata", t.Name())

	schemas, err := repository.LoadSchemasFromDir(
		dataDir, "v1.0.0", "deprecation")
	test.Must(t, err, "load deprecation schema")

	tc := testingAPIServer(t, logger, testingServerOptions{
		Schemas:         schemas,
		ConfigDirectory: dataDir,
		NoCoreSchemas:   true,
	})

	client := tc.SchemasClient(t, itest.StandardClaims(t, "schema_admin"))

	documentsClient := tc.DocumentsClient(t, itest.StandardClaims(t, "doc_write"))

	ctx := t.Context()

	doc := &newsdoc.Document{
		Uuid: "d98d2c21-980c-4c7f-b0b5-9ed9feba291b",
		Type: "test/deprecation",
		Uri:  "test://123",
		Meta: []*newsdoc.Block{
			{
				Type: "test/meta",
				Data: map[string]string{
					"value": "2",
				},
			},
		},
		Language: "en",
	}

	_, err = documentsClient.Update(ctx, &rpc_repository.UpdateRequest{
		Uuid:     doc.Uuid,
		Document: doc,
	})
	test.Must(t, err, "create a test document")

	_, err = client.UpdateDeprecation(ctx, &rpc_repository.UpdateDeprecationRequest{
		Deprecation: &rpc_repository.Deprecation{
			Label:    "data-value",
			Enforced: true,
		},
	})
	test.Must(t, err, "create a deprecation")

	deprecations, err := client.GetDeprecations(ctx, &rpc_repository.GetDeprecationsRequest{})
	test.Must(t, err, "get deprecations")
	test.EqualMessage(t, &rpc_repository.GetDeprecationsResponse{
		Deprecations: []*rpc_repository.Deprecation{
			{
				Label:    "data-value",
				Enforced: true,
			},
		},
	}, deprecations, "expected to get list of created deprecations")

	// Wait for validator to update its state of enforced deprecations
	deadline := time.Now().Add(5 * time.Second)
	succeeded := false

	for !succeeded {
		_, err = documentsClient.Update(ctx, &rpc_repository.UpdateRequest{
			Uuid:     doc.Uuid,
			Document: doc,
		})

		switch {
		case err == nil && deadline.After(time.Now()):
			time.Sleep(10 * time.Millisecond)
		case err == nil:
			t.Fatal("timeout waiting for deprecation to be enforced")
		default:
			succeeded = true
		}
	}

	_, err = client.UpdateDeprecation(ctx, &rpc_repository.UpdateDeprecationRequest{
		Deprecation: &rpc_repository.Deprecation{
			Label:    "data-value",
			Enforced: false,
		},
	})
	test.Must(t, err, "update a deprecation")

	deprecations, err = client.GetDeprecations(ctx, &rpc_repository.GetDeprecationsRequest{})
	test.Must(t, err, "get deprecations")
	test.EqualMessage(t, &rpc_repository.GetDeprecationsResponse{
		Deprecations: []*rpc_repository.Deprecation{
			{
				Label:    "data-value",
				Enforced: false,
			},
		},
	}, deprecations, "expected to get updated deprecation")

	// Wait for validator to update its state of enforced deprecations
	deadline = time.Now().Add(5 * time.Second)
	succeeded = false

	for !succeeded {
		_, err = documentsClient.Update(ctx, &rpc_repository.UpdateRequest{
			Uuid:     doc.Uuid,
			Document: doc,
		})

		switch {
		case err != nil && deadline.After(time.Now()):
			time.Sleep(10 * time.Millisecond)
		case err != nil:
			t.Fatal("timeout waiting for deprecation to be unenforced")
		default:
			succeeded = true
		}
	}
}
