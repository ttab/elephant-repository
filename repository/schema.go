package repository

import (
	"context"
	"fmt"
	"io/fs"
	"os"

	"github.com/Masterminds/semver/v3"
	"github.com/ttab/eleconf"
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

func LoadSchemasFromFS(
	dir fs.FS, version string, names ...string,
) ([]eleconf.LoadedSchema, error) {
	schemas := make([]eleconf.LoadedSchema, len(names))

	for i, name := range names {
		data, err := fs.ReadFile(dir, name+".json")
		if err != nil {
			return nil, fmt.Errorf("read %q schema: %w", name, err)
		}

		loaded := eleconf.LoadedSchema{
			Lock: eleconf.SchemaLock{
				Name:    name,
				Version: version,
			},
			Data: data,
		}

		schemas[i] = loaded
	}

	return schemas, nil
}

func LoadSchemasFromDir(
	path string, version string, names ...string,
) ([]eleconf.LoadedSchema, error) {
	return LoadSchemasFromFS(os.DirFS(path), version, names...)
}

func LoadEmbeddedSchemaSet(names ...string) ([]eleconf.LoadedSchema, error) {
	return LoadSchemasFromFS(
		revisorschemas.Files(),
		revisorschemas.Version(),
		names...)
}

// BootstrapGeneration checks if an active schema generation exists and, if
// not, creates one from the currently active schemas. This should be called
// at startup under a job lock to handle the migration from per-schema
// activation to generation-based activation.
func BootstrapGeneration(ctx context.Context, store SchemaStore) error {
	gen, err := store.GetActiveGeneration(ctx)
	if err != nil {
		return fmt.Errorf("get active generation: %w", err)
	}

	if gen != nil {
		return nil
	}

	schemas, err := store.ListActiveSchemas(ctx)
	if err != nil {
		return fmt.Errorf("list active schemas: %w", err)
	}

	if len(schemas) == 0 {
		return nil
	}

	// Load full specs for registration.
	genSchemas := make([]RegisterGenerationSchema, 0, len(schemas))

	for _, s := range schemas {
		full, sErr := store.GetSchema(ctx, s.Name, s.Version)
		if sErr != nil {
			return fmt.Errorf("get schema %q v%s: %w",
				s.Name, s.Version, sErr)
		}

		genSchemas = append(genSchemas, RegisterGenerationSchema{
			Name:          full.Name,
			Version:       full.Version,
			Specification: full.Specification,
		})
	}

	_, err = store.RegisterGeneration(ctx, RegisterGenerationStoreRequest{
		Schemas:    genSchemas,
		Activation: GenerationStatusActive,
	})
	if err != nil {
		return fmt.Errorf("register bootstrap generation: %w", err)
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
