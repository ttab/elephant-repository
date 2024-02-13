package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ttab/newsdoc"
	"github.com/ttab/revisor"
)

type DocStore interface {
	GetDocumentMeta(
		ctx context.Context, uuid uuid.UUID) (*DocumentMeta, error)
	GetDocument(
		ctx context.Context, uuid uuid.UUID, version int64,
	) (*newsdoc.Document, int64, error)
	GetVersion(
		ctx context.Context, uuid uuid.UUID, version int64,
	) (DocumentUpdate, error)
	GetVersionHistory(
		ctx context.Context, uuid uuid.UUID,
		before int64, count int,
	) ([]DocumentUpdate, error)
	Update(
		ctx context.Context,
		workflows WorkflowProvider,
		update []*UpdateRequest,
	) ([]DocumentUpdate, error)
	Delete(ctx context.Context, req DeleteRequest) error
	CheckPermission(
		ctx context.Context, req CheckPermissionRequest,
	) (CheckPermissionResult, error)
	GetMetaTypeForDocument(
		ctx context.Context, uuid uuid.UUID,
	) (DocumentMetaType, error)
	RegisterMetaType(
		ctx context.Context, metaType string, exclusive bool,
	) error
	RegisterMetaTypeUse(
		ctx context.Context, mainType string, metaType string,
	) error
	GetEventlog(
		ctx context.Context, after int64, limit int32,
	) ([]Event, error)
	GetLastEvent(
		ctx context.Context,
	) (*Event, error)
	GetLastEventID(
		ctx context.Context,
	) (int64, error)
	GetCompactedEventlog(
		ctx context.Context, req GetCompactedEventlogRequest,
	) ([]Event, error)
	OnEventlog(
		ctx context.Context, ch chan int64,
	)
	GetStatusHistory(
		ctx context.Context, uuid uuid.UUID,
		name string, before int64, count int,
	) ([]Status, error)
	GetDocumentACL(
		ctx context.Context, uuid uuid.UUID,
	) ([]ACLEntry, error)
	Lock(
		ctx context.Context, req LockRequest,
	) (LockResult, error)
	UpdateLock(
		ctx context.Context, req UpdateLockRequest,
	) (LockResult, error)
	Unlock(
		ctx context.Context, uuid uuid.UUID, token string,
	) error
}

type SchemaStore interface {
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
	GetSchemaVersions(ctx context.Context) (map[string]string, error)
	OnSchemaUpdate(ctx context.Context, ch chan SchemaEvent)
	RegisterMetaType(
		ctx context.Context, metaType string, exclusive bool,
	) error
	RegisterMetaTypeUse(
		ctx context.Context, mainType string, metaType string,
	) error
}

type WorkflowStore interface {
	UpdateStatus(
		ctx context.Context, req UpdateStatusRequest,
	) error
	GetStatuses(ctx context.Context) ([]DocumentStatus, error)
	UpdateStatusRule(
		ctx context.Context, rule StatusRule,
	) error
	DeleteStatusRule(
		ctx context.Context, name string,
	) error
	GetStatusRules(ctx context.Context) ([]StatusRule, error)
}

type ReportStore interface {
	ListReports(
		ctx context.Context,
	) ([]ReportListItem, error)
	UpdateReport(
		ctx context.Context, report Report, enabled bool,
	) (time.Time, error)
	GetReport(
		ctx context.Context, name string,
	) (*StoredReport, error)
	DeleteReport(
		ctx context.Context, name string,
	) error
}

type MetricStore interface {
	RegisterMetricKind(
		ctx context.Context, name string, aggregation Aggregation,
	) error
	DeleteMetricKind(
		ctx context.Context, name string,
	) error
	GetMetricKind(
		ctx context.Context, name string,
	) (*MetricKind, error)
	GetMetricKinds(
		ctx context.Context,
	) ([]*MetricKind, error)
	RegisterOrReplaceMetric(
		ctx context.Context, metric Metric,
	) error
	RegisterOrIncrementMetric(
		ctx context.Context, metric Metric,
	) error
}

type DocumentStatus struct {
	Name     string
	Disabled bool
}

type StatusRule struct {
	Name        string
	Description string
	AccessRule  bool
	AppliesTo   []string
	ForTypes    []string
	Expression  string
}

type UpdateStatusRequest struct {
	Name     string
	Disabled bool
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
	UUID         uuid.UUID
	Updated      time.Time
	Updater      string
	Meta         newsdoc.DataMap
	ACL          []ACLEntry
	DefaultACL   []ACLEntry
	Status       []StatusUpdate
	Document     *newsdoc.Document
	MainDocument *uuid.UUID
	IfMatch      int64
	LockToken    string
}

type DeleteRequest struct {
	UUID      uuid.UUID
	Updated   time.Time
	Updater   string
	Meta      newsdoc.DataMap
	IfMatch   int64
	LockToken string
}

type DocumentMeta struct {
	Created        time.Time
	Modified       time.Time
	CurrentVersion int64
	ACL            []ACLEntry
	Statuses       map[string]Status
	Deleting       bool
	Lock           Lock
	MainDocument   string
}

type ACLEntry struct {
	URI         string   `json:"uri"`
	Permissions []string `json:"permissions"`
}

type Lock struct {
	Token   string
	URI     string
	Created time.Time
	Expires time.Time
	App     string
	Comment string
}

type LockRequest struct {
	UUID    uuid.UUID
	URI     string
	TTL     int32
	App     string
	Comment string
}

type LockResult struct {
	Token   string
	Created time.Time
	Expires time.Time
}

type UpdateLockRequest struct {
	UUID  uuid.UUID
	TTL   int32
	Token string
}

type DocumentUpdate struct {
	UUID    uuid.UUID
	Version int64
	Creator string
	Created time.Time
	Meta    newsdoc.DataMap
}

type Status struct {
	ID             int64
	Version        int64
	Creator        string
	Created        time.Time
	Meta           newsdoc.DataMap
	MetaDocVersion int64
}

type StatusUpdate struct {
	Name    string
	Version int64
	Meta    newsdoc.DataMap
}

type Aggregation int16

const (
	AggregationNone      Aggregation = 0
	AggregationReplace   Aggregation = 1
	AggregationIncrement Aggregation = 2
)

type MetricKind struct {
	Name        string
	Aggregation Aggregation
}

type Metric struct {
	UUID  uuid.UUID
	Kind  string
	Label string
	Value int64
}

type GetCompactedEventlogRequest struct {
	After  int64
	Until  int64
	Type   string
	Limit  *int32
	Offset int32
}

// DocStoreErrorCode TODO: Rename to StoreErrorCode and consistently rename all
// dependent types and methods.
type DocStoreErrorCode string

const (
	NoErrCode                 DocStoreErrorCode = ""
	ErrCodeNotFound           DocStoreErrorCode = "not-found"
	ErrCodeNoSuchLock         DocStoreErrorCode = "no-such-lock"
	ErrCodeOptimisticLock     DocStoreErrorCode = "optimistic-lock"
	ErrCodeDeleteLock         DocStoreErrorCode = "delete-lock"
	ErrCodeBadRequest         DocStoreErrorCode = "bad-request"
	ErrCodeExists             DocStoreErrorCode = "exists"
	ErrCodePermissionDenied   DocStoreErrorCode = "permission-denied"
	ErrCodeFailedPrecondition DocStoreErrorCode = "failed-precondition"
	ErrCodeDocumentLock       DocStoreErrorCode = "document-lock"
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
