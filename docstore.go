package docformat

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type DocStore interface {
	GetDocumentMeta(
		ctx context.Context, uuid string) (*DocumentMeta, error)
	GetDocument(
		ctx context.Context, uuid string, version int,
	) (*Document, error)
	Update(
		ctx context.Context, update UpdateRequest,
	) (*DocumentUpdate, error)
	Delete(ctx context.Context, uuid string) error
}

type UpdateRequest struct {
	UUID     string
	Created  time.Time
	Updater  IdentityReference
	Meta     DataMap
	ACL      []ACLEntry
	Status   []StatusUpdate
	Document *Document
	IfMatch  int
}

type DocumentMeta struct {
	Created        time.Time
	Modified       time.Time
	CurrentVersion int
	ACL            []ACLEntry
	Updates        []DocumentUpdate
	Statuses       map[string][]Status
	StatusLog      map[string][]string
	Deleted        bool
	Log            []string
}

type ACLEntry struct {
	URI         string
	Name        string
	Permissions []string
}

type DocumentUpdate struct {
	Version       int
	Hash          string
	Parent        string
	Updater       IdentityReference
	Created       time.Time
	Meta          DataMap
	SchemaVersion int
}

type IdentityReference struct {
	URI  string
	Name string
}

type Status struct {
	Version     int
	VersionHash string
	Parent      string
	Updater     IdentityReference
	Created     time.Time
	Meta        DataMap
}

type StatusUpdate struct {
	Name    string
	Version int
	Meta    DataMap
}

type DocStoreErrorCode string

const (
	NoErrCode             DocStoreErrorCode = ""
	ErrCodeNotFound       DocStoreErrorCode = "not-found"
	ErrCodeOptimisticLock DocStoreErrorCode = "optimistic-lock"
	ErrCodeBadRequest     DocStoreErrorCode = "bad-request"
)

type DocStoreError struct {
	cause error
	code  DocStoreErrorCode
	msg   string
}

func DocStoreErrorf(code DocStoreErrorCode, format string, a ...any) error {
	e := fmt.Errorf(format, a...)

	return DocStoreError{
		cause: errors.Unwrap(e),
		code:  code,
		msg:   e.Error(),
	}
}

func (e DocStoreError) Error() string {
	return e.msg
}

func (e DocStoreError) Unwrap() error {
	return e.cause
}

func IsDocStoreErrorCode(err error, code DocStoreErrorCode) bool {
	return GetDocStoreErrorCode(err) == code
}

func GetDocStoreErrorCode(err error) DocStoreErrorCode {
	if err == nil {
		return NoErrCode
	}

	var e DocStoreError

	if errors.As(err, &e) {
		return e.code
	}

	return ""
}
