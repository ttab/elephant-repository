package repository

import (
	"context"
	"fmt"
	"io/fs"
	"os"

	"github.com/ttab/eleconf"
	"github.com/ttab/revisorschemas"
)

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
