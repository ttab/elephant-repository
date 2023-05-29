package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/revisor"
	"github.com/twitchtv/twirp"
	"golang.org/x/mod/semver"
)

type SchemasService struct {
	store SchemaStore
}

func NewSchemasService(store SchemaStore) *SchemasService {
	return &SchemasService{
		store: store,
	}
}

// Interface guard.
var _ repository.Schemas = &SchemasService{}

// GetAllActiveSchemas returns the currently active schemas.
func (a *SchemasService) GetAllActive(
	ctx context.Context, req *repository.GetAllActiveSchemasRequest,
) (*repository.GetAllActiveSchemasResponse, error) {
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasAnyScope("schema_read", "schema_admin", "superuser") {
		return nil, twirp.PermissionDenied.Error(
			"no schema_read permission")
	}

	err := a.waitIfSchemasAreUnchanged(ctx, req.Known, req.WaitSeconds)
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
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasAnyScope("schema_read", "schema_admin", "superuser") {
		return nil, twirp.PermissionDenied.Error(
			"no schema_read permission")
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
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasAnyScope("schema_admin", "superuser") {
		return nil, twirp.PermissionDenied.Error(
			"no schema_admin permission")
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

	var spec revisor.ConstraintSet

	err := json.Unmarshal([]byte(req.Schema.Spec), &spec)
	if err != nil {
		return nil, twirp.InvalidArgument.Errorf(
			"invalid schema: %w", err)
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

// SetActive activates schema versions.
func (a *SchemasService) SetActive(
	ctx context.Context, req *repository.SetActiveSchemaRequest,
) (*repository.SetActiveSchemaResponse, error) {
	auth, ok := GetAuthInfo(ctx)
	if !ok {
		return nil, twirp.Unauthenticated.Error(
			"no anonymous requests allowed")
	}

	if !auth.Claims.HasAnyScope("schema_admin", "superuser") {
		return nil, twirp.PermissionDenied.Error(
			"no schema_admin permission")
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
