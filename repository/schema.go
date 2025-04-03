package repository

import (
	"context"
	"fmt"

	"github.com/Masterminds/semver/v3"
	"github.com/ttab/revisor"
	"github.com/ttab/revisorschemas"
)

func EnsureCoreSchema(ctx context.Context, store SchemaStore) error {
	core, err := revisor.DecodeConstraintSetsFS(revisorschemas.Files(),
		"core.json", "core-metadoc.json", "core-planning.json")
	if err != nil {
		return fmt.Errorf("invalid embedded core schemas: %w", err)
	}

	coreVersion := revisorschemas.Version()

	for _, schema := range core {
		err := EnsureSchema(ctx, store, schema.Name, coreVersion, schema)
		if err != nil {
			return fmt.Errorf("failed to ensure schema %q: %w",
				schema.Name, err)
		}
	}

	return nil
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
