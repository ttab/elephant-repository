package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/exp/slog"
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

	hash := sha256.Sum256([]byte(parameters))

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

	err = os.WriteFile(path, doc, 0600)
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
	if err != nil {
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
		return nil, err //nolint:wrapcheck
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

func NewObjectCache(
	logger *slog.Logger, fc *FSCache, source *OCClient,
) *ObjectCache {
	return &ObjectCache{
		logger: logger,
		fc:     fc,
		source: source,
	}
}

type ObjectCache struct {
	logger *slog.Logger
	fc     *FSCache
	source *OCClient
}

type cachedObject struct {
	Header http.Header
	Data   json.RawMessage
}

func (oc ObjectCache) GetObject(
	ctx context.Context, uuid string, version int, o any,
) (http.Header, error) {
	ref := VersionReference{
		UUID:    uuid,
		Version: version,
	}

	cached, err := oc.fc.FetchDocument(ref, "")
	if err == nil {
		var co cachedObject

		err = json.Unmarshal(cached, &co)
		if err != nil {
			return nil, fmt.Errorf("failed to decode cache entry: %w", err)
		}

		err = json.Unmarshal(co.Data, o)
		if err != nil {
			return nil, fmt.Errorf("failed to decode cached object: %w", err)
		}

		return co.Header, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf(
			"failed to fetch properties from cache: %w", err)
	}

	header, err := oc.source.GetObject(ctx, uuid, version, o)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	co := cachedObject{
		Header: header,
	}

	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal object for caching: %w", err)
	}

	co.Data = data

	cacheEntry, err := json.MarshalIndent(co, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cache entry: %w", err)
	}

	err = oc.fc.StoreDocument(ref, "", cacheEntry)
	if err != nil {
		return nil, fmt.Errorf("failed to cache result: %w", err)
	}

	return header, nil
}
