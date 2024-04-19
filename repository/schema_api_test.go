package repository_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephantine/test"
)

func TestDeprecations(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	tc := testingAPIServer(t, logger, testingServerOptions{})

	client := tc.SchemasClient(t, itest.StandardClaims(t, "schema_admin"))

	documentsClient := tc.DocumentsClient(t, itest.StandardClaims(t, "doc_write"))

	ctx := test.Context(t)

	doc := &newsdoc.Document{
		Uuid: "d98d2c21-980c-4c7f-b0b5-9ed9feba291b",
		Type: "core/article",
		Uri:  "article://test/123",
		Meta: []*newsdoc.Block{
			{
				Type: "core/newsvalue",
				Data: map[string]string{
					"score": "5",
				},
			},
		},
		Language: "en",
	}

	_, err := documentsClient.Update(ctx, &repository.UpdateRequest{
		Uuid:     doc.Uuid,
		Document: doc,
	})
	test.Must(t, err, "create a test document")

	_, err = client.UpdateDeprecation(ctx, &repository.UpdateDeprecationRequest{
		Deprecation: &repository.Deprecation{
			Label:    "newsvalue-score",
			Enforced: true,
		},
	})
	test.Must(t, err, "create a deprecation")

	deprecations, err := client.GetDeprecations(ctx, &repository.GetDeprecationsRequest{})
	test.Must(t, err, "get deprecations")
	test.EqualMessage(t, &repository.GetDeprecationsResponse{
		Deprecations: []*repository.Deprecation{
			{
				Label:    "newsvalue-score",
				Enforced: true,
			},
		},
	}, deprecations, "expected to get list of created deprecations")

	// Wait for validator to update its state of enforced deprecations
	deadline := time.Now().Add(5 * time.Second)
	succeeded := false

	for !succeeded {
		_, err = documentsClient.Update(ctx, &repository.UpdateRequest{
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

	_, err = client.UpdateDeprecation(ctx, &repository.UpdateDeprecationRequest{
		Deprecation: &repository.Deprecation{
			Label:    "newsvalue-score",
			Enforced: false,
		},
	})
	test.Must(t, err, "update a deprecation")

	deprecations, err = client.GetDeprecations(ctx, &repository.GetDeprecationsRequest{})
	test.Must(t, err, "get deprecations")
	test.EqualMessage(t, &repository.GetDeprecationsResponse{
		Deprecations: []*repository.Deprecation{
			{
				Label:    "newsvalue-score",
				Enforced: false,
			},
		},
	}, deprecations, "expected to get updated deprecation")

	// Wait for validator to update its state of enforced deprecations
	deadline = time.Now().Add(5 * time.Second)
	succeeded = false

	for !succeeded {
		_, err = documentsClient.Update(ctx, &repository.UpdateRequest{
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
