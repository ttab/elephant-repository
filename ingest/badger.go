package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v3"
)

func NewBadgerStore(dbPath string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(dbPath)

	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return &BadgerStore{
		db: db,
	}, nil
}

type BadgerStore struct {
	m  sync.RWMutex
	db *badger.DB
}

func (bs *BadgerStore) Close() error {
	return bs.db.Close() //nolint:wrapcheck
}

func (bs *BadgerStore) GetCurrentVersion(
	documentUUID string,
) (*DocumentVersionInformation, error) {
	bs.m.RLock()
	defer bs.m.RUnlock()

	var r DocumentVersionInformation

	err := bs.db.View(func(txn *badger.Txn) error {
		var docRefKey = fmt.Sprintf("/document/ref/%s", documentUUID)

		err := getAndDecodeIfFound(txn, []byte(docRefKey), &r)
		if err != nil {
			return fmt.Errorf(
				"failed to read current document reference: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("database transaction failed: %w", err)
	}

	return &r, nil
}

func (bs *BadgerStore) SetLogPosition(pos int) error {
	bs.m.Lock()
	defer bs.m.Unlock()

	err := bs.db.Update(func(txn *badger.Txn) error {
		return encodeAndStore(txn, []byte("/contentlog/position"), pos)
	})
	if err != nil {
		return fmt.Errorf("database transaction failed: %w", err)
	}

	return nil
}

func (bs *BadgerStore) GetLogPosition() (int, error) {
	bs.m.RLock()
	defer bs.m.RUnlock()

	var pos int

	err := bs.db.View(func(txn *badger.Txn) error {
		return getAndDecodeIfFound(txn, []byte("/contentlog/position"), &pos)
	})
	if err != nil {
		return 0, fmt.Errorf("database transaction failed: %w", err)
	}

	return pos, nil
}

func (bs *BadgerStore) RegisterReference(ref VersionReference) (VersionReference, error) {
	bs.m.Lock()
	defer bs.m.Unlock()

	norm := ref

	err := bs.db.Update(func(txn *badger.Txn) error {
		var (
			docRefKey = fmt.Sprintf("/document/ref/%s", ref.UUID)
			r         DocumentVersionInformation
		)

		err := getAndDecodeIfFound(txn, []byte(docRefKey), &r)
		if err != nil {
			return fmt.Errorf("failed to read current document reference: %w", err)
		}

		r.CurrentVersion = ref.Version

		if r.OriginalUUID != "" {
			norm.UUID = r.OriginalUUID
		}

		return encodeAndStore(txn, []byte(docRefKey), &r)
	})
	if err != nil {
		return VersionReference{}, fmt.Errorf("failed to update database: %w", err)
	}

	return norm, nil
}

func (bs *BadgerStore) RegisterContinuation(
	originalUUID string, newUUID string,
) error {
	bs.m.Lock()
	defer bs.m.Unlock()

	err := bs.db.Update(func(txn *badger.Txn) error {
		var (
			originalKey = fmt.Sprintf(
				"/document/ref/%s", originalUUID)
			newKey = fmt.Sprintf(
				"/document/ref/%s", newUUID)
			originalRef, newRef DocumentVersionInformation
		)

		err := getAndDecodeIfFound(txn, []byte(originalKey), &originalRef)
		if err != nil {
			return fmt.Errorf(
				"failed to read original document reference: %w", err)
		}

		err = getAndDecodeIfFound(txn, []byte(newKey), &newRef)
		if err != nil {
			return fmt.Errorf(
				"failed to read new document reference: %w", err)
		}

		newRef.OriginalUUID = originalUUID

		if originalRef.OriginalUUID != "" {
			newRef.OriginalUUID = originalRef.OriginalUUID
		}

		return encodeAndStore(txn, []byte(newKey), &newRef)
	})
	if err != nil {
		return fmt.Errorf("failed to update database: %w", err)
	}

	return nil
}

func encodeAndStore(txn *badger.Txn, key []byte, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	err = txn.Set(key, data)
	if err != nil {
		return fmt.Errorf("failed to set value for key: %w", err)
	}

	return nil
}

func getAndDecodeIfFound(txn *badger.Txn, key []byte, o interface{}) error {
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to get item: %w", err)
	}

	err = item.Value(func(val []byte) error {
		err = json.Unmarshal(val, o)
		if err != nil {
			return fmt.Errorf("failed to unmarshal value: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to get value: %w", err)
	}

	return nil
}
