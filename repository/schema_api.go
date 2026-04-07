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

func NewSchemasService(logger *slog.Logger, store SchemaStore) *SchemasService {
	return &SchemasService{
		logger: logger,
		store:  store,
		client: &http.Client{},
	}
}

// Interface guard.
var _ repository.Schemas = &SchemasService{}

type SchemasService struct {
	logger *slog.Logger
	store  SchemaStore
	client *http.Client
}

// GetDocumentTypes implements repository.Schemas.
func (a *SchemasService) GetDocumentTypes(
	ctx context.Context,
	_ *repository.GetDocumentTypesRequest,
) (*repository.GetDocumentTypesResponse, error) {
	schemas, err := a.store.GetActiveSchemas(ctx)
	if err != nil {
		return nil, twirp.InternalErrorf("get schemas: %v", err)
	}

	var declared []string

	for _, sc := range schemas {
		for _, doc := range sc.Specification.Documents {
			if doc.Declares == "" {
				continue
			}

			declared = append(declared, doc.Declares)
		}
	}

	return &repository.GetDocumentTypesResponse{
		Types: declared,
	}, nil
}

// ConfigureType implements repository.Schemas.
func (a *SchemasService) ConfigureType(
	ctx context.Context, req *repository.ConfigureTypeRequest,
) (*repository.ConfigureTypeResponse, error) {
	_, err := RequireAnyScope(ctx,
		ScopeSchemaAdmin,
	)
	if err != nil {
		return nil, err
	}

	if req.Type == "" {
		return nil, twirp.RequiredArgumentError("type")
	}

	if req.Configuration == nil {
		return nil, twirp.RequiredArgumentError("configuration")
	}

	err = a.store.ConfigureType(ctx, req.Type,
		typeConfigurationFromRPC(req.Configuration))
	if err != nil {
		return nil, twirp.InternalErrorf("store type configuration: %v", err)
	}

	return &repository.ConfigureTypeResponse{}, nil
}

// GetTypeConfiguration implements repository.Schemas.
func (a *SchemasService) GetTypeConfiguration(
	ctx context.Context, req *repository.GetTypeConfigurationRequest,
) (*repository.GetTypeConfigurationResponse, error) {
	_, err := RequireAnyScope(ctx,
		ScopeSchemaAdmin,
	)
	if err != nil {
		return nil, err
	}

	if req.Type == "" {
		return nil, twirp.RequiredArgumentError("type")
	}

	conf, err := a.store.GetTypeConfiguration(ctx, req.Type)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.NewErrorf(twirp.NotFound,
			"could not find type configuration: %v", err)
	}

	return &repository.GetTypeConfigurationResponse{
		Configuration: typeConfigurationToRPC(conf),
	}, nil
}

// GetMetaTypes implements repository.Schemas.
func (a *SchemasService) GetMetaTypes(
	ctx context.Context, _ *repository.GetMetaTypesRequest,
) (*repository.GetMetaTypesResponse, error) {
	types, err := a.store.GetMetaTypes(ctx)
	if err != nil {
		return nil, twirp.InternalErrorf("read meta type info: %v", err)
	}

	var res repository.GetMetaTypesResponse

	for _, t := range types {
		res.Types = append(res.Types, &repository.MetaTypeInfo{
			Name:   t.Name,
			UsedBy: t.UsedBy,
		})
	}

	return &res, nil
}

// ListActive implements repository.Schemas.
func (a *SchemasService) ListActive(
	ctx context.Context,
	_ *repository.ListActiveSchemasRequest,
) (*repository.ListActiveSchemasResponse, error) {
	schemas, err := a.store.ListActiveSchemas(ctx)
	if err != nil {
		return nil, twirp.InternalErrorf("read schema info: %v", err)
	}

	res := repository.ListActiveSchemasResponse{
		Schemas: make([]*repository.Schema, len(schemas)),
	}

	for i, s := range schemas {
		res.Schemas[i] = &repository.Schema{
			Name:    s.Name,
			Version: s.Version,
		}
	}

	return &res, nil
}

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

	changed, err := a.waitForSchemaChange(ctx, req.Known, req.WaitSeconds)
	if err != nil {
		return nil, fmt.Errorf("wait for schema changes: %w", err)
	}

	if !changed && req.OnlyChanged {
		return &repository.GetAllActiveSchemasResponse{
			Unchanged: true,
		}, nil
	}

	schemas, err := a.store.GetActiveSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to retrieve active schemas: %w", err)
	}

	var (
		res   repository.GetAllActiveSchemasResponse
		exist = make(map[string]bool)
	)

	for i := range schemas {
		// Ignore known schema versions.
		kv := req.Known[schemas[i].Name]
		if req.OnlyChanged && kv == schemas[i].Version {
			continue
		}

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

		exist[schemas[i].Name] = true
	}

	for requested := range req.Known {
		if exist[requested] {
			continue
		}

		res.Removed = append(res.Removed, requested)
	}

	return &res, nil
}

func (a *SchemasService) waitForSchemaChange(
	ctx context.Context,
	known map[string]string, waitSeconds int64,
) (bool, error) {
	if waitSeconds == 0 || waitSeconds > 10 {
		waitSeconds = 10
	}

	timeout := time.Duration(waitSeconds) * time.Second

	ch := make(chan SchemaEvent)

	a.store.OnSchemaUpdate(ctx, ch)

	for {
		versions, err := a.store.GetSchemaVersions(ctx)
		if err != nil {
			return false, fmt.Errorf(
				"get current versions: %w", err)
		}

		if len(known) != len(versions) {
			return true, nil
		}

		for n := range known {
			if known[n] != versions[n] {
				return true, nil
			}
		}

		select {
		case <-ch:
		case <-time.After(timeout):
			return false, nil
		}
	}
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
) (_ revisor.ConstraintSet, outErr error) {
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

	defer elephantine.Close("schema response", res.Body, &outErr)

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
		if IsDocStoreErrorCode(err, ErrCodeFailedPrecondition) {
			return nil, twirp.FailedPrecondition.Error(err.Error())
		} else if err != nil {
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
