package repository

import (
	"context"
	"fmt"

	"github.com/Masterminds/semver/v3"
	"github.com/ttab/elephant/revisor"
	"github.com/ttab/elephant/revisor/constraints"
)

func EnsureCoreSchema(ctx context.Context, store SchemaStore) error {
	core, err := constraints.CoreSchema()
	if err != nil {
		return fmt.Errorf("failed to load core schema: %w", err)
	}

	coreVersion := constraints.CoreSchemaVersion()

	return EnsureSchema(ctx, store, "core", coreVersion, core)
}

func EnsureSchema(
	ctx context.Context, store SchemaStore,
	name string, version string, schema revisor.ConstraintSet,
) error {
	newVersion, err := semver.NewVersion(version)
	if err != nil {
		return fmt.Errorf("invalid version: %w", err)
	}

	versions, err := store.GetSchemaVersions(ctx)
	if err != nil {
		return fmt.Errorf("failed to read current schema versions: %w", err)
	}

	currentVersion, ok := versions[name]

	register := !ok

	if ok {
		cV, err := semver.NewVersion(currentVersion)
		if err != nil {
			return fmt.Errorf("invalid version in database: %w", err)
		}

		register = newVersion.GreaterThan(cV)
	}

	if !register {
		return nil
	}

	err = store.RegisterSchema(ctx, RegisterSchemaRequest{
		Name:          name,
		Version:       version,
		Activate:      true,
		Specification: schema,
	})
	if IsDocStoreErrorCode(err, ErrCodeExists) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}

	return nil
}
