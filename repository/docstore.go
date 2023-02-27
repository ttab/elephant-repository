package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/ttab/elephant/doc"
	"github.com/ttab/elephant/revisor"
)

type DocStore interface {
	GetDocumentMeta(
		ctx context.Context, uuid uuid.UUID) (*DocumentMeta, error)
	GetDocument(
		ctx context.Context, uuid uuid.UUID, version int64,
	) (*doc.Document, error)
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
	CheckPermission(
		ctx context.Context, req CheckPermissionRequest,
	) (CheckPermissionResult, error)
	RegisterSchema(
		ctx context.Context, req RegisterSchemaRequest,
	) error
	ActivateSchema(
		ctx context.Context, name, version string,
	) error
	DeactivateSchema(
		ctx context.Context, name string,
	) error
	GetSchema(
		ctx context.Context, name, version string,
	) (*Schema, error)
	GetActiveSchemas(ctx context.Context) ([]*Schema, error)
}

type Schema struct {
	Name          string
	Version       string
	Specification revisor.ConstraintSet
}

type RegisterSchemaRequest struct {
	Name          string
	Version       string
	Specification revisor.ConstraintSet
	Activate      bool
}

type Permission string

const (
	ReadPermission  Permission = "r"
	WritePermission Permission = "w"
)

func (p Permission) Name() string {
	switch p {
	case ReadPermission:
		return "read"
	case WritePermission:
		return "write"
	}

	return strconv.Quote(string(p))
}

type CheckPermissionRequest struct {
	UUID        uuid.UUID
	GranteeURIs []string
	Permission  Permission
}

type CheckPermissionResult int

const (
	PermissionCheckDenied = iota
	PermissionCheckAllowed
	PermissionCheckNoSuchDocument
)

type UpdateRequest struct {
	UUID       uuid.UUID
	Updated    time.Time
	Updater    string
	Meta       doc.DataMap
	ACL        []ACLEntry
	DefaultACL []ACLEntry
	Status     []StatusUpdate
	Document   *doc.Document
	IfMatch    int64
}

type DeleteRequest struct {
	UUID    uuid.UUID
	Updated time.Time
	Updater string
	Meta    doc.DataMap
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
	Permissions []string
}

type DocumentUpdate struct {
	Version int64
	Creator string
	Created time.Time
	Meta    doc.DataMap
}

type Status struct {
	ID      int64
	Version int64
	Creator string
	Created time.Time
	Meta    doc.DataMap
}

type StatusUpdate struct {
	Name    string
	Version int64
	Meta    doc.DataMap
}

type DocStoreErrorCode string

const (
	NoErrCode             DocStoreErrorCode = ""
	ErrCodeNotFound       DocStoreErrorCode = "not-found"
	ErrCodeOptimisticLock DocStoreErrorCode = "optimistic-lock"
	ErrCodeDeleteLock     DocStoreErrorCode = "delete-lock"
	ErrCodeBadRequest     DocStoreErrorCode = "bad-request"
	ErrCodeExists         DocStoreErrorCode = "exists"
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
