package repository

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ttab/newsdoc"
	"github.com/viccon/sturdyc"
)

// NewDocCache creates a document cache that can be used for efficient bulk
// document loading.
func NewDocCache(store DocStore, capacity int) *DocCache {
	pdc := DocCache{
		store: store,
		cache: sturdyc.New[BulkGetItem](capacity, 1, 10*time.Minute, 10,
			sturdyc.WithEvictionInterval(10*time.Second),
		),
	}

	return &pdc
}

type DocCache struct {
	store DocStore
	cache *sturdyc.Client[BulkGetItem]
}

func (dc *DocCache) CacheDocument(
	docID uuid.UUID,
	version int64,
	doc newsdoc.Document,
) {
	dc.cache.Set(docCacheKey(docID.String(), version), BulkGetItem{
		Document: doc,
		Version:  version,
	})
}

func (dc *DocCache) GetDocuments(
	ctx context.Context,
	refs []BulkGetReference,
) (iter.Seq[BulkGetItem], error) {
	keys := bulkToKeys(refs)

	res, err := dc.cache.GetOrFetchBatch(
		ctx,
		keys,
		dc.cache.BatchKeyFn("doc"),
		dc.batchFetch)
	if err != nil {
		return nil, fmt.Errorf("get batch from cache: %w", err)
	}

	return maps.Values(res), nil
}

func (dc *DocCache) batchFetch(
	ctx context.Context, keys []string,
) (map[string]BulkGetItem, error) {
	refs := keysToBulk(keys)

	docs, err := dc.store.BulkGetDocuments(ctx, refs)
	if err != nil {
		return nil, fmt.Errorf("get documents from store: %w", err)
	}

	result := make(map[string]BulkGetItem, len(docs))

	for i := range docs {
		key := docCacheKey(docs[i].Document.UUID, docs[i].Version)

		result[key] = docs[i]
	}

	return result, nil
}

func bulkToKeys(refs []BulkGetReference) []string {
	keys := make([]string, len(refs))

	for i := range refs {
		keys[i] = docCacheKey(refs[i].UUID.String(), refs[i].Version)
	}

	return keys
}

func docCacheKey(docID string, version int64) string {
	return docID + ":" + strconv.FormatInt(version, 10)
}

func keysToBulk(keys []string) []BulkGetReference {
	refs := make([]BulkGetReference, len(keys))

	// We're panicing if the key is invalid, as it's not a matter of
	// external input being wrong, but the internal key generation. It's
	// essentially a guard against a "this shouldn't happen" scenario.

	for i := range keys {
		id, v, ok := strings.Cut(keys[i], ":")
		if !ok {
			panic(errors.New("missing : in cache key"))
		}

		docID, err := uuid.Parse(id)
		if err != nil {
			panic(fmt.Errorf("invalid document UUID in cache key: %w", err))
		}

		version, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			panic(fmt.Errorf("invalid documentversion in cache key: %w", err))
		}

		refs[i] = BulkGetReference{
			UUID:    docID,
			Version: version,
		}
	}

	return refs
}
