package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephantine"
	"github.com/ttab/revisor"
	"github.com/twitchtv/twirp"
	"golang.org/x/mod/semver"
)

type SchemasService struct {
	logger *slog.Logger
	store  SchemaStore
	client *http.Client
}

func NewSchemasService(logger *slog.Logger, store SchemaStore) *SchemasService {
	return &SchemasService{
		logger: logger,
		store:  store,
		client: &http.Client{},
	}
}

// Interface guard.
var _ repository.Schemas = &SchemasService{}

// RegisterMetaType implements repository.Schemas.
func (a *SchemasService) RegisterMetaType(
	ctx context.Context, req *repository.RegisterMetaTypeRequest,
) (*repository.RegisterMetaTypeResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeSchemaAdmin)
	if err != nil {
		return nil, err
	}

	if req.Type == "" {
		return nil, twirp.RequiredArgumentError("type")
	}

	err = a.store.RegisterMetaType(ctx, req.Type, req.Exclusive)
	if err != nil {
		return nil, fmt.Errorf("failed to register meta type: %w", err)
	}

	return &repository.RegisterMetaTypeResponse{}, nil
}

// RegisterMetaTypeUse implements repository.Schemas.
func (a *SchemasService) RegisterMetaTypeUse(
	ctx context.Context, req *repository.RegisterMetaTypeUseRequest,
) (*repository.RegisterMetaTypeUseResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeSchemaAdmin)
	if err != nil {
		return nil, err
	}

	if req.MainType == "" {
		return nil, twirp.RequiredArgumentError("main_type")
	}

	if req.MetaType == "" {
		return nil, twirp.RequiredArgumentError("meta_type")
	}

	err = a.store.RegisterMetaTypeUse(ctx, req.MainType, req.MetaType)
	if errors.As(err, &DocStoreError{}) {
		return nil, twirp.InvalidArgument.Error(err.Error())
	} else if err != nil {
		return nil, fmt.Errorf("failed to register meta type: %w", err)
	}

	return &repository.RegisterMetaTypeUseResponse{}, nil
}

// GetAllActiveSchemas returns the currently active schemas.
func (a *SchemasService) GetAllActive(
	ctx context.Context, req *repository.GetAllActiveSchemasRequest,
) (*repository.GetAllActiveSchemasResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeSchemaAdmin, ScopeSchemaRead)
	if err != nil {
		return nil, err
	}

	err = a.waitIfSchemasAreUnchanged(ctx, req.Known, req.WaitSeconds)
	if err != nil {
		return nil, fmt.Errorf("wait for schema changes: %w", err)
	}

	schemas, err := a.store.GetActiveSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to retrieve active schemas: %w", err)
	}

	var res repository.GetAllActiveSchemasResponse

	for i := range schemas {
		data, err := json.Marshal(schemas[i].Specification)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to marshal %q@%s specification for response: %w",
				schemas[i].Name, schemas[i].Version, err)
		}

		res.Schemas = append(res.Schemas, &repository.Schema{
			Name:    schemas[i].Name,
			Version: schemas[i].Version,
			Spec:    string(data),
		})
	}

	return &res, nil
}

func (a *SchemasService) waitIfSchemasAreUnchanged(
	ctx context.Context,
	known map[string]string, waitSeconds int64,
) error {
	if len(known) == 0 {
		return nil
	}

	versions, err := a.store.GetSchemaVersions(ctx)
	if err != nil {
		return fmt.Errorf(
			"get current versions: %w", err)
	}

	if len(known) != len(versions) {
		return nil
	}

	for n := range known {
		if known[n] != versions[n] {
			return nil
		}
	}

	ch := make(chan SchemaEvent)

	a.store.OnSchemaUpdate(ctx, ch)

	if waitSeconds == 0 || waitSeconds > 10 {
		waitSeconds = 10
	}

	timeout := time.Duration(waitSeconds) * time.Second

	select {
	case <-ch:
	case <-time.After(timeout):
	}

	return nil
}

// Get retrieves a schema.
func (a *SchemasService) Get(
	ctx context.Context, req *repository.GetSchemaRequest,
) (*repository.GetSchemaResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeSchemaAdmin, ScopeSchemaRead)
	if err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	schema, err := a.store.GetSchema(ctx, req.Name, req.Version)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to retrieve schema: %w", err)
	}

	data, err := json.Marshal(schema.Specification)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to marshal specification for response: %w",
			err)
	}

	return &repository.GetSchemaResponse{
		Version: schema.Version,
		Spec:    data,
	}, nil
}

// Register register a new validation schema version.
func (a *SchemasService) Register(
	ctx context.Context, req *repository.RegisterSchemaRequest,
) (*repository.RegisterSchemaResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeSchemaAdmin)
	if err != nil {
		return nil, err
	}

	if req.Schema == nil {
		return nil, twirp.RequiredArgumentError("schema")
	}

	if req.Schema.Name == "" {
		return nil, twirp.RequiredArgumentError("schema.name")
	}

	if req.Schema.Version == "" {
		return nil, twirp.RequiredArgumentError("schema.version")
	}

	version := semver.Canonical(req.Schema.Version)
	if version == "" {
		return nil, twirp.InvalidArgumentError(
			"schema.version", "invalid semver version")
	}

	if req.Schema.Spec == "" && req.SchemaUrl == "" {
		return nil, twirp.InvalidArgumentError("schema.spec",
			"the schema spec is required if no URL is passed")
	}

	if req.Schema.Spec != "" && req.SchemaUrl != "" {
		return nil, twirp.InvalidArgumentError("schema_url",
			"a schema_url cannot be passed if an inline spec is set")
	}

	var spec revisor.ConstraintSet

	if req.Schema.Spec != "" {
		err = json.Unmarshal([]byte(req.Schema.Spec), &spec)
		if err != nil {
			return nil, twirp.InvalidArgument.Errorf(
				"invalid schema: %w", err)
		}
	} else {
		s, err := a.fetchRemoteSpec(ctx,
			req.SchemaUrl, req.SchemaSha256)
		if err != nil {
			return nil, err
		}

		spec = s
	}

	err = a.store.RegisterSchema(ctx, RegisterSchemaRequest{
		Name:          req.Schema.Name,
		Version:       version,
		Specification: spec,
		Activate:      req.Activate,
	})
	if IsDocStoreErrorCode(err, ErrCodeExists) {
		return nil, twirp.FailedPrecondition.Error(
			"schema version already exists")
	} else if err != nil {
		return nil, fmt.Errorf("failed to register schema: %w", err)
	}

	return &repository.RegisterSchemaResponse{}, nil
}

func (a *SchemasService) fetchRemoteSpec(
	ctx context.Context, schemaURL string, schemaHash string,
) (revisor.ConstraintSet, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, schemaURL, nil)
	if err != nil {
		return revisor.ConstraintSet{}, twirp.InvalidArgumentError(
			"schema_url",
			fmt.Sprintf("failed to create schema request: %v", err))
	}

	res, err := a.client.Do(req) //nolint:bodyclose
	if err != nil {
		return revisor.ConstraintSet{}, twirp.InvalidArgumentError(
			"schema_url",
			fmt.Sprintf("failed to get schema_url: %v", err))
	}

	defer elephantine.SafeClose(a.logger,
		"schema response", res.Body)

	specData, err := io.ReadAll(res.Body)
	if err != nil {
		return revisor.ConstraintSet{}, twirp.InvalidArgumentError(
			"schema_url",
			fmt.Sprintf("failed to read schema_url response: %v", err))
	}

	if schemaHash != "" {
		want, err := hex.DecodeString(schemaHash)
		if err != nil {
			return revisor.ConstraintSet{}, twirp.InvalidArgumentError(
				"schema_sha256",
				fmt.Sprintf("invalid checksum: %v", err))
		}

		if len(want) != sha256.Size {
			return revisor.ConstraintSet{}, twirp.InvalidArgumentError(
				"schema_sha256",
				fmt.Sprintf(
					"invalid checksum length, expected %d bytes, got %d",
					sha256.Size, len(want)))
		}

		got := sha256.Sum256(specData)

		if !bytes.Equal(want, got[:]) {
			return revisor.ConstraintSet{}, twirp.InvalidArgumentError(
				"schema_sha256",
				fmt.Sprintf(
					"checksum mismatch expected %x, got %x",
					want, got))
		}
	}

	var spec revisor.ConstraintSet

	err = json.Unmarshal(specData, &spec)
	if err != nil {
		return revisor.ConstraintSet{}, twirp.InvalidArgument.Errorf(
			"invalid schema: %w", err)
	}

	return spec, nil
}

// SetActive activates schema versions.
func (a *SchemasService) SetActive(
	ctx context.Context, req *repository.SetActiveSchemaRequest,
) (*repository.SetActiveSchemaResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeSchemaAdmin)
	if err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, twirp.RequiredArgumentError("name")
	}

	if !req.Deactivate && req.Version == "" {
		return nil, twirp.RequiredArgumentError("version")
	}

	if req.Deactivate {
		err := a.store.DeactivateSchema(ctx, req.Name)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to deactivate schema: %w", err)
		}
	} else {
		err := a.store.ActivateSchema(ctx, req.Name, req.Version)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to register activation: %w", err)
		}
	}

	return &repository.SetActiveSchemaResponse{}, nil
}

// GetDeprecations implements repository.Schemas.
func (a *SchemasService) GetDeprecations(
	ctx context.Context, _ *repository.GetDeprecationsRequest,
) (*repository.GetDeprecationsResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeSchemaAdmin)
	if err != nil {
		return nil, err
	}

	var res repository.GetDeprecationsResponse

	deprecations, err := a.store.GetDeprecations(ctx)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"failed to list deprecations: %w", err)
	}

	res.Deprecations = make([]*repository.Deprecation, len(deprecations))
	for i, d := range deprecations {
		res.Deprecations[i] = &repository.Deprecation{
			Label:    d.Label,
			Enforced: d.Enforced,
		}
	}

	return &res, nil
}

// UpdateDeprecation implements repository.Schemas.
func (a *SchemasService) UpdateDeprecation(
	ctx context.Context, req *repository.UpdateDeprecationRequest,
) (*repository.UpdateDeprecationResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeSchemaAdmin)
	if err != nil {
		return nil, err
	}

	if req.Deprecation.Label == "" {
		return nil, twirp.RequiredArgumentError("deprecation.label")
	}

	err = a.store.UpdateDeprecation(ctx, Deprecation{
		Label:    req.Deprecation.Label,
		Enforced: req.Deprecation.Enforced,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update deprecation: %w", err)
	}

	return &repository.UpdateDeprecationResponse{}, nil
}
