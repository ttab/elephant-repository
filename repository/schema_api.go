package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	rpc_newsdoc "github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/newsdoc"
	"github.com/ttab/revisor"
	"github.com/twitchtv/twirp"
)

func NewSchemasService(logger *slog.Logger, store SchemaStore) *SchemasService {
	return &SchemasService{
		logger: logger,
		store:  store,
	}
}

// Interface guard.
var _ repository.Schemas = &SchemasService{}

type SchemasService struct {
	logger *slog.Logger
	store  SchemaStore
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

	genID, _ := a.store.GetActiveGenerationID(ctx)

	res.GenerationId = genID

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
		return nil, fmt.Errorf("register meta type: %w", err)
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
		return nil, fmt.Errorf("register meta type use: %w", err)
	}

	return &repository.RegisterMetaTypeUseResponse{}, nil
}

// GetAllActive returns the currently active schemas.
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
			"retrieve active schemas: %w", err)
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
				"marshal %q@%s specification for response: %w",
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

	genID, _ := a.store.GetActiveGenerationID(ctx)
	res.GenerationId = genID

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
			"retrieve schema: %w", err)
	}

	data, err := json.Marshal(schema.Specification)
	if err != nil {
		return nil, fmt.Errorf(
			"marshal specification for response: %w",
			err)
	}

	return &repository.GetSchemaResponse{
		Version: schema.Version,
		Spec:    data,
	}, nil
}

// RegisterGeneration implements repository.Schemas.
func (a *SchemasService) RegisterGeneration(
	ctx context.Context, req *repository.RegisterGenerationRequest,
) (*repository.RegisterGenerationResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeSchemaAdmin)
	if err != nil {
		return nil, err
	}

	if len(req.Schemas) == 0 {
		return nil, twirp.RequiredArgumentError("schemas")
	}

	activation := GenerationStatusDeactivated

	switch req.Activation {
	case repository.SchemaActivation_ACTIVATION_ACTIVE:
		activation = GenerationStatusActive
	case repository.SchemaActivation_ACTIVATION_PENDING:
		activation = GenerationStatusPending
	case repository.SchemaActivation_ACTIVATION_UNKNOWN,
		repository.SchemaActivation_ACTIVATION_DEACTIVATED:
		activation = GenerationStatusDeactivated
	}

	// Parse schemas from RPC.
	schemas := make([]RegisterGenerationSchema, 0, len(req.Schemas))

	for _, s := range req.Schemas {
		if s.Name == "" {
			return nil, twirp.RequiredArgumentError("schemas[].name")
		}

		if s.Version == "" {
			return nil, twirp.RequiredArgumentError("schemas[].version")
		}

		var spec revisor.ConstraintSet

		if s.Spec != "" {
			err = json.Unmarshal([]byte(s.Spec), &spec)
			if err != nil {
				return nil, twirp.InvalidArgument.Errorf(
					"invalid schema spec for %q: %v", s.Name, err)
			}
		}

		schemas = append(schemas, RegisterGenerationSchema{
			Name:          s.Name,
			Version:       s.Version,
			Specification: spec,
		})
	}

	// Process exemplars: canonicalize and hash.
	exemplars := make([]ExemplarInput, 0, len(req.Exemplars))

	for _, rpcDoc := range req.Exemplars {
		doc := rpc_newsdoc.DocumentFromRPC(rpcDoc)

		canonical, cErr := json.Marshal(doc)
		if cErr != nil {
			return nil, twirp.InvalidArgument.Errorf(
				"canonicalize exemplar: %v", cErr)
		}

		h := sha256.Sum256(canonical)
		versionHash := "sha256:" + hex.EncodeToString(h[:])

		name := doc.URI
		if name == "" {
			name = doc.UUID
		}

		exemplars = append(exemplars, ExemplarInput{
			Name:     name,
			DocType:  doc.Type,
			Document: canonical,
			Version:  versionHash,
		})
	}

	// Validate exemplars against the generation's schemas.
	if len(schemas) > 0 && len(exemplars) > 0 {
		var constraints []revisor.ConstraintSet

		for _, s := range schemas {
			constraints = append(constraints, s.Specification)
		}

		val, vErr := revisor.NewValidator(constraints...)
		if vErr != nil {
			return nil, twirp.InvalidArgument.Errorf(
				"schemas cannot form a valid validator: %v", vErr)
		}

		for _, ex := range exemplars {
			var doc newsdoc.Document

			if uErr := json.Unmarshal(ex.Document, &doc); uErr != nil {
				return nil, twirp.InvalidArgument.Errorf(
					"invalid exemplar document %q: %v",
					ex.Name, uErr)
			}

			results, vErr := val.ValidateDocument(ctx, &doc)
			if vErr != nil {
				return nil, fmt.Errorf(
					"validate exemplar %q: %w", ex.Name, vErr)
			}

			if len(results) > 0 {
				return nil, twirp.InvalidArgument.Errorf(
					"exemplar %q has %d validation errors: %s",
					ex.Name, len(results), results[0].String())
			}
		}
	}

	genID, err := a.store.RegisterGeneration(ctx, RegisterGenerationStoreRequest{
		Schemas:    schemas,
		Activation: activation,
		Exemplars:  exemplars,
	})
	if err != nil {
		return nil, fmt.Errorf("register generation: %w", err)
	}

	return &repository.RegisterGenerationResponse{
		GenerationId: genID,
	}, nil
}

// SetActive implements repository.Schemas.
func (a *SchemasService) SetActive(
	ctx context.Context, req *repository.SetActiveSchemasRequest,
) (*repository.SetActiveSchemasResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeSchemaAdmin)
	if err != nil {
		return nil, err
	}

	if req.GenerationId == 0 {
		return nil, twirp.RequiredArgumentError("generation_id")
	}

	var activation SchemaGenerationStatus

	switch req.Activation {
	case repository.SchemaActivation_ACTIVATION_ACTIVE:
		activation = GenerationStatusActive
	case repository.SchemaActivation_ACTIVATION_PENDING:
		activation = GenerationStatusPending
	case repository.SchemaActivation_ACTIVATION_DEACTIVATED:
		activation = GenerationStatusDeactivated
	case repository.SchemaActivation_ACTIVATION_UNKNOWN:
		return nil, twirp.InvalidArgumentError(
			"activation", "invalid activation value")
	}

	err = a.store.SetGenerationStatus(ctx, req.GenerationId, activation)

	switch {
	case IsDocStoreErrorCode(err, ErrCodeBadRequest):
		return nil, twirp.InvalidArgument.Error(err.Error())
	case IsDocStoreErrorCode(err, ErrCodeNotFound):
		return nil, twirp.NotFound.Error(err.Error())
	case err != nil:
		return nil, fmt.Errorf("set generation status: %w", err)
	}

	return &repository.SetActiveSchemasResponse{}, nil
}

// ListGenerations implements repository.Schemas.
func (a *SchemasService) ListGenerations(
	ctx context.Context, req *repository.ListSchemaGenerationsRequest,
) (*repository.ListSchemaGenerationsResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeSchemaAdmin, ScopeSchemaRead)
	if err != nil {
		return nil, err
	}

	generations, err := a.store.ListGenerations(ctx, req.Before)
	if err != nil {
		return nil, fmt.Errorf("list generations: %w", err)
	}

	items := make([]*repository.SchemaGeneration, 0, len(generations))

	for _, g := range generations {
		items = append(items, schemaGenerationToRPC(g))
	}

	return &repository.ListSchemaGenerationsResponse{
		Items: items,
	}, nil
}

// GetExemplars implements repository.Schemas.
func (a *SchemasService) GetExemplars(
	ctx context.Context, req *repository.GetExemplarsRequest,
) (*repository.GetExemplarsResponse, error) {
	_, err := RequireAnyScope(ctx, ScopeSchemaAdmin, ScopeSchemaRead)
	if err != nil {
		return nil, err
	}

	if req.GenerationId == 0 {
		return nil, twirp.RequiredArgumentError("generation_id")
	}

	exemplars, err := a.store.GetExemplars(ctx, req.GenerationId, req.Known)
	if err != nil {
		return nil, fmt.Errorf("get exemplars: %w", err)
	}

	items := make([]*repository.Exemplar, 0, len(exemplars))

	for _, ex := range exemplars {
		var doc newsdoc.Document

		if uErr := json.Unmarshal(ex.Document, &doc); uErr != nil {
			return nil, fmt.Errorf(
				"unmarshal exemplar %q: %w", ex.Name, uErr)
		}

		items = append(items, &repository.Exemplar{
			Name:        ex.Name,
			Document:    rpc_newsdoc.DocumentToRPC(doc),
			VersionHash: ex.VersionHash,
		})
	}

	return &repository.GetExemplarsResponse{
		Exemplars: items,
	}, nil
}

func schemaGenerationToRPC(g SchemaGeneration) *repository.SchemaGeneration {
	rpc := &repository.SchemaGeneration{
		Id:      g.ID,
		Created: g.Created.Format(time.RFC3339),
	}

	switch g.Status {
	case GenerationStatusActive:
		rpc.Status = repository.SchemaActivation_ACTIVATION_ACTIVE
	case GenerationStatusPending:
		rpc.Status = repository.SchemaActivation_ACTIVATION_PENDING
	case GenerationStatusDeactivated:
		rpc.Status = repository.SchemaActivation_ACTIVATION_DEACTIVATED
	}

	if g.Activated != nil {
		rpc.Activated = g.Activated.Format(time.RFC3339)
	}

	if g.Deactivated != nil {
		rpc.Deactivated = g.Deactivated.Format(time.RFC3339)
	}

	rpc.Schemas = make([]*repository.SchemaReference, len(g.Schemas))
	for i, s := range g.Schemas {
		rpc.Schemas[i] = &repository.SchemaReference{
			Name:    s.Name,
			Version: s.Version,
		}
	}

	return rpc
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
			"list deprecations: %w", err)
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
		return nil, fmt.Errorf("update deprecation: %w", err)
	}

	return &repository.UpdateDeprecationResponse{}, nil
}
