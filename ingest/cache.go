package ingest

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type DocumentCache interface {
	StoreDocument(ref VersionReference, parameters string, doc []byte) error
	FetchDocument(ref VersionReference, parameters string) ([]byte, error)
}

type FSCache struct {
	dir string
}

func NewFSCache(cacheDir string) *FSCache {
	return &FSCache{dir: cacheDir}
}

type GetDocumentFunc func(
	ctx context.Context, request GetDocumentRequest,
) (*DocumentRevision, error)

func WrapGetDocumentFunc(fn GetDocumentFunc, cache DocumentCache) GetDocumentFunc {
	return func(
		ctx context.Context, request GetDocumentRequest,
	) (*DocumentRevision, error) {
		ref := VersionReference(request)

		cached, err := cache.FetchDocument(ref, "")
		if err == nil {
			var res DocumentRevision

			err := json.Unmarshal(cached, &res)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to unmarshal cached document: %w", err)
			}

			return &res, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf(
				"failed to fetch document from cache: %w", err)
		}

		res, err := fn(ctx, request)
		if err != nil {
			return nil, err
		}

		ref.Version = res.Version

		cacheData, err := json.MarshalIndent(res, "", "  ")
		if err != nil {
			return nil, fmt.Errorf(
				"failed to marshal result for cache: %w", err)
		}

		err = cache.StoreDocument(ref, "", cacheData)
		if err != nil {
			return nil, fmt.Errorf("failed to cache result: %w", err)
		}

		return res, nil
	}
}

func (fc FSCache) path(ref VersionReference, parameters string) string {
	if parameters == "" {
		return filepath.Join(fc.dir, ref.UUID, fmt.Sprintf(
			"%d.json", ref.Version,
		))
	}

	hash := sha1.Sum([]byte(parameters))

	return filepath.Join(fc.dir, ref.UUID, fmt.Sprintf(
		"%d-%x.json", ref.Version, hash,
	))
}

func (fc FSCache) StoreDocument(ref VersionReference, parameters string, doc []byte) error {
	path := fc.path(ref, parameters)

	err := os.MkdirAll(filepath.Dir(path), 0770)
	if err != nil {
		return fmt.Errorf("failed to ensure directory structure: %w", err)
	}

	err = os.WriteFile(path, doc, 0660)
	if err != nil {
		return fmt.Errorf("failed to write document to disk: %w", err)
	}

	return nil
}

func (fc FSCache) FetchDocument(ref VersionReference, parameters string) ([]byte, error) {
	if ref.Version == 0 {
		return nil, os.ErrNotExist
	}

	path := fc.path(ref, parameters)

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, err
	} else if err != nil {
		return nil, fmt.Errorf("failed to load data from disk: %w", err)
	}

	return data, nil
}

type PropertyCache struct {
	fc     *FSCache
	source PropertyGetter
}

func NewPropertyCache(cache *FSCache, source PropertyGetter) *PropertyCache {
	return &PropertyCache{
		fc:     cache,
		source: source,
	}
}

func (pc *PropertyCache) GetProperties(
	ctx context.Context, uuid string, version int, props []string,
) (map[string][]string, error) {
	ref := VersionReference{
		UUID:    uuid,
		Version: version,
	}

	parameters := strings.Join(props, ",")

	cached, err := pc.fc.FetchDocument(ref, parameters)
	if err == nil {
		var res map[string][]string

		err := json.Unmarshal(cached, &res)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to unmarshal cached properties: %w", err)
		}

		return res, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf(
			"failed to fetch properties from cache: %w", err)
	}

	res, err := pc.source.GetProperties(ctx, uuid, version, props)
	if err != nil {
		return nil, err
	}

	if version != 0 {
		cacheData, err := json.MarshalIndent(res, "", "  ")
		if err != nil {
			return nil, fmt.Errorf(
				"failed to marshal result for cache: %w", err)
		}

		err = pc.fc.StoreDocument(ref, parameters, cacheData)
		if err != nil {
			return nil, fmt.Errorf("failed to cache result: %w", err)
		}
	}

	return res, nil
}
