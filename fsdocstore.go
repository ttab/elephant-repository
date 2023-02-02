package docformat

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type FSDocStore struct {
	index *SearchIndex
	dir   string
}

func NewFSDocStore(dir string, index *SearchIndex) *FSDocStore {
	return &FSDocStore{
		index: index,
		dir:   dir,
	}
}

func (s *FSDocStore) GetDocumentMeta(
	ctx context.Context, uuid string,
) (*DocumentMeta, error) {
	meta := DocumentMeta{
		Statuses: make(map[string][]Status),
	}

	err := s.read(filepath.Join(uuid, "meta.json"), &meta)
	if errors.Is(err, os.ErrNotExist) {
		return nil, DocStoreErrorf(
			ErrCodeNotFound, "document doesn't exist")
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	return &meta, nil
}

func (s *FSDocStore) GetDocument(
	ctx context.Context, uuid string, version int64,
) (*Document, error) {
	var doc Document

	err := s.read(filepath.Join(
		uuid, fmt.Sprintf("%d.json", version)), &doc)
	if errors.Is(err, os.ErrNotExist) {
		return nil, DocStoreErrorf(
			ErrCodeNotFound, "document version doesn't exist")
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch document: %w", err)
	}

	return &doc, nil
}

func (s *FSDocStore) Delete(ctx context.Context, uuid string) error {
	meta, err := s.GetDocumentMeta(ctx, uuid)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to load current metadata: %w", err)
	}

	meta.Deleted = true

	err = s.write(filepath.Join(
		uuid, "meta.json",
	), meta)
	if err != nil {
		return fmt.Errorf("failed to write metadata to disk: %w", err)
	}

	return nil
}

func (s *FSDocStore) Update(
	ctx context.Context, update UpdateRequest,
) (*DocumentUpdate, error) {
	if update.UUID == "" {
		return nil, DocStoreErrorf(
			ErrCodeBadRequest, "update UUID cannot be empty")
	}

	for _, stat := range update.Status {
		if stat.Version == 0 && update.Document == nil {
			return nil, DocStoreErrorf(ErrCodeBadRequest,
				"status version must be set when not providing a document")
		}
	}

	up := DocumentUpdate{
		Created: update.Created,
		Updater: update.Updater,
		Meta:    update.Meta,
	}

	current, err := s.GetDocumentMeta(ctx, update.UUID)
	if err != nil && !IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, fmt.Errorf(
			"failed to load current metadata: %w", err)
	}

	meta := DocumentMeta{
		Statuses: make(map[string][]Status),
	}

	if current != nil {
		meta = *current
	}

	if update.Document == nil && (meta.CurrentVersion == 0 || meta.Deleted) {
		return nil, DocStoreErrorf(ErrCodeBadRequest,
			"a document must be included for creation")
	}

	meta.Deleted = false

	switch update.IfMatch {
	case 0:
	case -1:
		if meta.CurrentVersion != 0 {
			return nil, DocStoreErrorf(ErrCodeOptimisticLock,
				"document already exists")
		}
	default:
		if meta.CurrentVersion != update.IfMatch {
			return nil, DocStoreErrorf(ErrCodeOptimisticLock,
				"document version is %d, not %d as expected",
				meta.CurrentVersion, update.IfMatch,
			)
		}
	}

	if meta.Created.IsZero() {
		meta.Created = up.Created
	}

	meta.Modified = up.Created

	for _, ua := range update.ACL {
		meta.ACL = upsertACL(meta.ACL, ua)
	}

	if len(meta.ACL) == 0 {
		meta.ACL = append(meta.ACL, ACLEntry{
			URI:         up.Updater.URI,
			Name:        up.Updater.Name,
			Permissions: []string{"r", "w"},
		})
	}

	if update.Document != nil {
		meta.CurrentVersion++
		up.Version = meta.CurrentVersion

		meta.Updates = append(meta.Updates, up)
	}

	up.Version = meta.CurrentVersion

	for _, stat := range update.Status {
		status := Status{
			Updater: update.Updater,
			Version: stat.Version,
			Meta:    stat.Meta,
			Created: meta.Modified,
		}

		if status.Version == 0 {
			status.Version = up.Version
		}

		if status.Version > int64(len(meta.Updates)) {
			return nil, DocStoreErrorf(ErrCodeBadRequest,
				"status %q refers to a version %d that doesn't exist",
				stat.Name, status.Version)
		}

		meta.Statuses[stat.Name] = append(
			meta.Statuses[stat.Name], status,
		)
	}

	if update.Document != nil {
		err = s.write(filepath.Join(
			update.UUID, fmt.Sprintf(
				"%d.json", up.Version),
		), update.Document)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to write document to disk: %w", err)
		}
	}

	err = s.write(filepath.Join(
		update.UUID, "meta.json",
	), meta)
	if err != nil {
		return nil, fmt.Errorf("failed to write metadata to disk: %w", err)
	}

	docToIndex := update.Document
	if docToIndex == nil {
		d, err := s.GetDocument(ctx, update.UUID, meta.CurrentVersion)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to load document for indexing: %w", err)
		}

		docToIndex = d
	}

	err = s.index.IndexDocument(meta, *docToIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to index document: %w", err)
	}

	return &up, nil
}

func (s *FSDocStore) ReIndex(done chan string) error {
	dir, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("failed to open document directory: %w", err)
	}

	defer dir.Close()

	ctx := context.Background()

	for {
		names, err := dir.Readdirnames(100)
		if errors.Is(err, io.EOF) {
			break
		}

		for _, name := range names {
			meta, err := s.GetDocumentMeta(ctx, name)
			if IsDocStoreErrorCode(err, ErrCodeNotFound) {
				continue
			}

			if err != nil {
				return fmt.Errorf(
					"failed to load %s metadata: %w",
					name, err)
			}

			if len(meta.Updates) == 0 {
				continue
			}

			lastVersion := meta.Updates[len(meta.Updates)-1]

			doc, err := s.GetDocument(ctx, name, lastVersion.Version)
			if err != nil {
				return fmt.Errorf(
					"could not load document %s v%d: %w",
					name, lastVersion.Version, err)
			}

			err = s.index.IndexDocument(*meta, *doc)
			if err != nil {
				return fmt.Errorf(
					"failed to index %s v%d: %w",
					name, lastVersion.Version, err)
			}

			done <- name
		}

	}

	return nil
}

func upsertACL(acl []ACLEntry, entry ACLEntry) []ACLEntry {
	for i := range acl {
		if acl[i].URI != entry.URI {
			continue
		}

		if entry.Name != "" {
			acl[i].Name = entry.Name
		}

		acl[i].Permissions = entry.Permissions

		return acl
	}

	acl = append(acl, entry)

	return acl
}

func hash(obj any) (string, error) {
	hash := sha256.New()
	enc := json.NewEncoder(hash)

	err := enc.Encode(obj)
	if err != nil {
		return "", fmt.Errorf(
			"failed to hash object: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(hash.Sum(nil)), nil
}

func (s *FSDocStore) read(name string, o any) error {
	path := filepath.Join(s.dir, name)

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return err
	} else if err != nil {
		return fmt.Errorf("failed to load data from disk: %w", err)
	}

	err = json.Unmarshal(data, o)
	if err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}

	return nil
}

func (s *FSDocStore) write(name string, o any) error {
	path := filepath.Join(s.dir, name)

	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal value for storage: %w", err)
	}

	err = os.MkdirAll(filepath.Dir(path), 0770)
	if err != nil {
		return fmt.Errorf("failed to ensure directory structure: %w", err)
	}

	err = os.WriteFile(path, data, 0660)
	if err != nil {
		return fmt.Errorf("failed to write data to disk: %w", err)
	}

	return nil
}
