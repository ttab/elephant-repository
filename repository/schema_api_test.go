package repository_test

import (
	"encoding/json"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/ttab/elephant-api/newsdoc"
	rpc_repository "github.com/ttab/elephant-api/repository"
	itest "github.com/ttab/elephant-repository/internal/test"
	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"github.com/ttab/revisor"
	"github.com/twitchtv/twirp"
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
	test.Mustf(t, err, "load deprecation schema")

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
	test.Mustf(t, err, "create a test document")

	_, err = client.UpdateDeprecation(ctx, &rpc_repository.UpdateDeprecationRequest{
		Deprecation: &rpc_repository.Deprecation{
			Label:    "data-value",
			Enforced: true,
		},
	})
	test.Mustf(t, err, "create a deprecation")

	deprecations, err := client.GetDeprecations(ctx, &rpc_repository.GetDeprecationsRequest{})
	test.Mustf(t, err, "get deprecations")
	test.EqualMessagef(t, &rpc_repository.GetDeprecationsResponse{
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
	test.Mustf(t, err, "update a deprecation")

	deprecations, err = client.GetDeprecations(ctx, &rpc_repository.GetDeprecationsRequest{})
	test.Mustf(t, err, "get deprecations")
	test.EqualMessagef(t, &rpc_repository.GetDeprecationsResponse{
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

func TestVariantValidation(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelError))

	tc := testingAPIServer(t, logger, testingServerOptions{})

	schemasClient := tc.SchemasClient(t,
		itest.StandardClaims(t, "schema_admin"))

	documentsClient := tc.DocumentsClient(t,
		itest.StandardClaims(t, "doc_write"))

	ctx := t.Context()

	// Configure "template" as a valid variant for core/article.
	_, err := schemasClient.ConfigureType(ctx,
		&rpc_repository.ConfigureTypeRequest{
			Type: "core/article",
			Configuration: &rpc_repository.TypeConfiguration{
				Variants: []string{"template"},
			},
		})
	test.Mustf(t, err, "configure type variants")

	// Verify the configuration was stored.
	confResp, err := schemasClient.GetTypeConfiguration(ctx,
		&rpc_repository.GetTypeConfigurationRequest{
			Type: "core/article",
		})
	test.Mustf(t, err, "get type configuration")
	test.EqualDiff(t,
		[]string{"template"}, confResp.Configuration.Variants,
		"expected variants to be stored")

	// Wait for the validator to pick up the variant configuration, then
	// create a document with the variant type.
	templateDoc := baseDocument(
		"b3a7c8e1-1234-4f00-9abc-def012345678",
		"article://test/template-1",
	)
	templateDoc.Type = "core/article#template"

	deadline := time.Now().Add(5 * time.Second)
	succeeded := false

	for !succeeded {
		_, err = documentsClient.Update(ctx, &rpc_repository.UpdateRequest{
			Uuid:     templateDoc.Uuid,
			Document: templateDoc,
		})

		switch {
		case err == nil:
			succeeded = true
		case deadline.After(time.Now()):
			time.Sleep(10 * time.Millisecond)
		default:
			t.Fatalf(
				"timeout waiting for variant to be accepted: %v",
				err)
		}
	}

	// Verify that an unconfigured variant is rejected.
	badDoc := baseDocument(
		"c4b8d9f2-5678-4f00-9abc-def012345679",
		"article://test/bad-variant-1",
	)
	badDoc.Type = "core/article#nonexistent"

	_, err = documentsClient.Update(ctx, &rpc_repository.UpdateRequest{
		Uuid:     badDoc.Uuid,
		Document: badDoc,
	})

	if !elephantine.IsTwirpErrorCode(err, twirp.InvalidArgument) {
		t.Fatalf(
			"expected invalid argument error for unknown variant, got: %v",
			err)
	}
}

// TestGenerationRegistrationActivatesExisting checks that registering a
// generation that already exists still applies the requested activation.
// Registration is idempotent on the schema versions, so a re-registration used
// to return the existing generation ID without activating it, reporting success
// while leaving the previous generation active.
func TestGenerationRegistrationActivatesExisting(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := slog.New(test.NewLogHandler(t, slog.LevelError))

	tc := testingAPIServer(t, logger, testingServerOptions{})

	client := tc.SchemasClient(t, itest.StandardClaims(t, "schema_admin"))

	ctx := t.Context()

	active, err := client.GetAllActive(ctx,
		&rpc_repository.GetAllActiveSchemasRequest{})
	test.Mustf(t, err, "get active schemas")

	baseline := active.GenerationId

	spec := revisor.ConstraintSet{
		Version: 1,
		Name:    "test_reactivation",
		Documents: []revisor.DocumentConstraint{
			{
				Name:     "Reactivation test document",
				Declares: "test/reactivation",
			},
		},
	}

	specPayload, err := json.Marshal(&spec)
	test.Mustf(t, err, "marshal test schema")

	schemas := make([]*rpc_repository.Schema, 0, len(active.Schemas)+1)
	schemas = append(schemas, active.Schemas...)
	schemas = append(schemas, &rpc_repository.Schema{
		Name:    "test/reactivation",
		Version: "v1.0.0",
		Spec:    string(specPayload),
	})

	// Register the generation without activating it.
	pendingRes, err := client.RegisterGeneration(ctx,
		&rpc_repository.RegisterGenerationRequest{
			Activation: rpc_repository.SchemaActivation_ACTIVATION_PENDING,
			Schemas:    schemas,
		})
	test.Mustf(t, err, "register pending generation")

	active, err = client.GetAllActive(ctx,
		&rpc_repository.GetAllActiveSchemasRequest{})
	test.Mustf(t, err, "get active schemas after pending registration")

	test.EqualDiff(t, baseline, active.GenerationId,
		"expected a pending registration to leave the active generation alone")

	// Re-register the same schema versions, this time asking for them to be
	// activated.
	activeRes, err := client.RegisterGeneration(ctx,
		&rpc_repository.RegisterGenerationRequest{
			Activation: rpc_repository.SchemaActivation_ACTIVATION_ACTIVE,
			Schemas:    schemas,
		})
	test.Mustf(t, err, "register the same generation as active")

	test.EqualDiff(t, pendingRes.GenerationId, activeRes.GenerationId,
		"expected registration to be idempotent on the generation ID")

	active, err = client.GetAllActive(ctx,
		&rpc_repository.GetAllActiveSchemasRequest{})
	test.Mustf(t, err, "get active schemas after activation")

	test.EqualDiff(t, activeRes.GenerationId, active.GenerationId,
		"expected the re-registered generation to become active")

	var found bool

	for _, s := range active.Schemas {
		if s.Name == "test/reactivation" {
			found = true
		}
	}

	if !found {
		t.Error("expected the activated generation's schema to be active")
	}
}
