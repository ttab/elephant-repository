package docformat

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type DocStore interface {
	GetDocumentMeta(
		ctx context.Context, uuid uuid.UUID) (*DocumentMeta, error)
	GetDocument(
		ctx context.Context, uuid uuid.UUID, version int64,
	) (*Document, error)
	GetVersion(
		ctx context.Context, uuid uuid.UUID, version int64,
	) (DocumentUpdate, error)
	GetVersionHistory(
		ctx context.Context, uuid uuid.UUID,
		before int64, count int,
	) ([]DocumentUpdate, error)
	Update(
		ctx context.Context, update UpdateRequest,
	) (*DocumentUpdate, error)
	Delete(ctx context.Context, req DeleteRequest) error
}

type UpdateRequest struct {
	UUID     uuid.UUID
	Updated  time.Time
	Updater  string
	Meta     DataMap
	ACL      []ACLEntry
	Status   []StatusUpdate
	Document *Document
	IfMatch  int64
}

type DeleteRequest struct {
	UUID    uuid.UUID
	Updated time.Time
	Updater string
	Meta    DataMap
	IfMatch int64
}

type DocumentMeta struct {
	Created        time.Time
	Modified       time.Time
	CurrentVersion int64
	ACL            []ACLEntry
	Statuses       map[string]Status
	Deleting       bool
}

type ACLEntry struct {
	URI         string
	Name        string
	Permissions []string
}

type DocumentUpdate struct {
	Version int64
	Creator string
	Created time.Time
	Meta    DataMap
}

type Status struct {
	ID      int64
	Version int64
	Creator string
	Created time.Time
	Meta    DataMap
}

type StatusUpdate struct {
	Name    string
	Version int64
	Meta    DataMap
}

type DocStoreErrorCode string

const (
	NoErrCode             DocStoreErrorCode = ""
	ErrCodeNotFound       DocStoreErrorCode = "not-found"
	ErrCodeOptimisticLock DocStoreErrorCode = "optimistic-lock"
	ErrCodeDeleteLock     DocStoreErrorCode = "delete-lock"
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
