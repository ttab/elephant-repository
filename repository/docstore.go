package repository

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/ttab/newsdoc"
	"github.com/ttab/revisor"
)

const (
	VersionHistoryMaxCount = 50
	StatusHistoryMaxCount  = 50
)

type DocStore interface {
	GetDocumentMeta(
		ctx context.Context, uuid uuid.UUID,
	) (*DocumentMeta, error)
	BulkGetDocumentMeta(
		ctx context.Context, documents []uuid.UUID,
	) (map[uuid.UUID]*DocumentMeta, error)
	GetDocument(
		ctx context.Context, uuid uuid.UUID, version int64,
	) (*newsdoc.Document, int64, error)
	BulkGetDocuments(
		ctx context.Context, documents []BulkGetReference,
	) ([]BulkGetItem, error)
	GetVersion(
		ctx context.Context, uuid uuid.UUID, version int64,
	) (DocumentUpdate, error)
	// GetVersionHistory of a document. Count cannot be greater than
	// VersionHistoryMaxCount.
	GetVersionHistory(
		ctx context.Context, uuid uuid.UUID,
		before int64, count int64, loadStatuses bool,
	) ([]DocumentHistoryItem, error)
	Update(
		ctx context.Context,
		workflows WorkflowProvider,
		update []*UpdateRequest,
	) ([]DocumentUpdate, error)
	Delete(ctx context.Context, req DeleteRequest) error
	ListDeleteRecords(
		ctx context.Context, docUUID *uuid.UUID,
		beforeID int64, startDate *time.Time,
	) ([]DeleteRecord, error)
	RestoreDocument(
		ctx context.Context, docUUID uuid.UUID, deleteRecordID int64,
		creator string, acl []ACLEntry,
	) error
	PurgeDocument(
		ctx context.Context, docUUID uuid.UUID, deleteRecordID int64,
		creator string,
	) error
	CheckPermissions(
		ctx context.Context, req CheckPermissionRequest,
	) (CheckPermissionResult, error)
	BulkCheckPermissions(
		ctx context.Context, req BulkCheckPermissionRequest,
	) ([]uuid.UUID, error)
	GetTypeOfDocument(
		ctx context.Context, uuid uuid.UUID,
	) (string, error)
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
	OnArchivedUpdate(
		ctx context.Context, ch chan ArchivedEvent,
	)
	GetStatus(
		ctx context.Context, uuid uuid.UUID,
		name string, id int64,
	) (Status, error)
	// GetStatusHistory of a document. Count cannot be greater than
	// StatusHistoryMaxCount.
	GetStatusHistory(
		ctx context.Context, uuid uuid.UUID,
		name string, before int64, count int,
	) ([]Status, error)
	GetNilStatuses(
		ctx context.Context, uuid uuid.UUID,
		names []string,
	) (map[string][]Status, error)
	GetStatusOverview(
		ctx context.Context,
		uuids []uuid.UUID, statuses []string,
		getMeta bool,
	) ([]StatusOverviewItem, error)
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
	GetDeliverableInfo(
		ctx context.Context, uuid uuid.UUID,
	) (DeliverableInfo, error)
	BulkGetDeliverableInfo(
		ctx context.Context, uuids []uuid.UUID,
	) ([]DeliverableInfo, error)
	CreateUpload(ctx context.Context, upload Upload) error
	GetAttachments(
		ctx context.Context,
		documents []uuid.UUID,
		attachment string,
		getDownloadLink bool,
	) ([]AttachmentDetails, error)
	ListDocumentsInTimeRange(
		ctx context.Context,
		docType string,
		span Timespan,
		labels []string,
	) ([]DocumentItem, error)
	ListDocumentsOfType(
		ctx context.Context,
		docType string,
		language *string,
		labels []string,
	) ([]DocumentItem, error)
	EnsureSocketKey(ctx context.Context) (*ecdsa.PrivateKey, error)
}

type DocumentItem struct {
	UUID           uuid.UUID
	CurrentVersion int64
	Language       string
}

type TypeConfiguration struct {
	BoundedCollection bool
	TimeExpressions   []TimespanConfiguration
	LabelExpressions  []LabelConfiguration
	Variants          []string
}

type DeliverableInfo struct {
	UUID            uuid.UUID
	HasPlanningInfo bool
	PlanningUUID    *uuid.UUID
	AssignmentUUID  *uuid.UUID
	EventUUID       *uuid.UUID
}

type Upload struct {
	ID        uuid.UUID
	CreatedBy string
	CreatedAt time.Time
	Meta      AssetMetadata
}

type AttachmentDetails struct {
	Document     uuid.UUID
	Name         string
	Version      int64
	DownloadLink string
	Filename     string
	ContentType  string
}

type AssetMetadata struct {
	Filename string            `json:"filename"`
	Mimetype string            `json:"mimetype"`
	Props    map[string]string `json:"props"`
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
	// ListActiveSchemas without populating the schema spec.
	ListActiveSchemas(ctx context.Context) ([]*Schema, error)
	GetActiveSchemas(ctx context.Context) ([]*Schema, error)
	GetSchemaVersions(ctx context.Context) (map[string]string, error)
	OnSchemaUpdate(ctx context.Context, ch chan SchemaEvent)
	RegisterMetaType(
		ctx context.Context, metaType string, exclusive bool,
	) error
	RegisterMetaTypeUse(
		ctx context.Context, mainType string, metaType string,
	) error
	GetMetaTypes(
		ctx context.Context,
	) ([]MetaTypeInfo, error)
	GetDeprecations(
		ctx context.Context,
	) ([]*Deprecation, error)
	UpdateDeprecation(
		ctx context.Context, deprecation Deprecation,
	) error
	ConfigureType(
		ctx context.Context,
		docType string,
		configuration TypeConfiguration,
	) error
	GetTypeConfiguration(
		ctx context.Context,
		docType string,
	) (*TypeConfiguration, error)
	GetTypeConfigurations(
		ctx context.Context,
	) (map[string]TypeConfiguration, error)

	// Generation management.
	RegisterGeneration(
		ctx context.Context, req RegisterGenerationStoreRequest,
	) (int64, error)
	SetGenerationStatus(
		ctx context.Context, id int64, activation SchemaGenerationStatus,
	) error
	ListGenerations(
		ctx context.Context, before int64,
	) ([]SchemaGeneration, error)
	GetExemplars(
		ctx context.Context, generationID int64, known map[string]string,
	) ([]ExemplarRecord, error)
	GetActiveGeneration(ctx context.Context) (*SchemaGeneration, error)
	GetActiveGenerationID(ctx context.Context) (int64, error)
	GetPendingGeneration(ctx context.Context) (*SchemaGeneration, error)
	GetActiveGenerationSchemas(ctx context.Context) ([]*Schema, error)
	GetPendingGenerationSchemas(ctx context.Context) ([]*Schema, error)
}

type WorkflowStore interface {
	UpdateStatus(
		ctx context.Context, req UpdateStatusRequest,
	) error
	GetStatuses(ctx context.Context, docType string) ([]DocumentStatus, error)
	UpdateStatusRule(
		ctx context.Context, rule StatusRule,
	) error
	DeleteStatusRule(
		ctx context.Context, docType string, name string,
	) error
	GetStatusRules(ctx context.Context) ([]StatusRule, error)
	SetDocumentWorkflow(ctx context.Context, workflow DocumentWorkflow) error
	GetDocumentWorkflows(ctx context.Context) ([]DocumentWorkflow, error)
	GetDocumentWorkflow(ctx context.Context, docType string) (DocumentWorkflow, error)
	DeleteDocumentWorkflow(ctx context.Context, docType string) error
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
	GetMetrics(
		ctx context.Context, uuids []uuid.UUID, kinds []string,
	) ([]Metric, error)
}

type DocumentStatus struct {
	Type     string
	Name     string
	Disabled bool
}

type DocumentWorkflow struct {
	Type          string
	Updated       time.Time
	UpdaterURI    string
	Configuration DocumentWorkflowConfiguration
}

type WorkflowState struct {
	Step           string
	LastCheckpoint string
}

func (ws WorkflowState) Equal(b WorkflowState) bool {
	return ws.Step == b.Step && ws.LastCheckpoint == b.LastCheckpoint
}

type WorkflowStep struct {
	// Version should be set if this is a document version bump.
	Version int64
	// Status should be set if this is a status update.
	Status *StatusUpdate
}

func (wf DocumentWorkflow) Start() WorkflowState {
	return WorkflowState{
		Step: wf.Configuration.StepZero,
	}
}

func (wf DocumentWorkflow) Step(state WorkflowState, step WorkflowStep) WorkflowState {
	if wf.Type == "" {
		return state
	}

	checkpoint := wf.Configuration.Checkpoint
	negCheckpoint := wf.Configuration.NegativeCheckpoint

	isCheckpoint := checkpoint != "" && step.Status != nil && step.Status.Name == checkpoint
	isStep := step.Status != nil && slices.Contains(wf.Configuration.Steps, step.Status.Name)
	atCheckpoint := (checkpoint != "" && state.Step == checkpoint) ||
		(negCheckpoint != "" && state.Step == negCheckpoint)

	// Creating a new version when at a checkpoint resets the step to zero.
	if step.Version > 0 && atCheckpoint {
		state.Step = wf.Configuration.StepZero
	}

	switch {
	case isCheckpoint && step.Status.Version > 0:
		state.Step = checkpoint
		state.LastCheckpoint = checkpoint
	case isCheckpoint && step.Status.Version == -1:
		state.Step = negCheckpoint
		state.LastCheckpoint = negCheckpoint
	case isStep && step.Status.Version > 0:
		state.Step = step.Status.Name
	}

	return state
}

type DocumentWorkflowConfiguration struct {
	StepZero           string   `json:"step_zero"`
	Checkpoint         string   `json:"checkpoint"`
	NegativeCheckpoint string   `json:"negative_checkpoint"`
	Steps              []string `json:"steps"`
}

type StatusRule struct {
	Type        string
	Name        string
	Description string
	AccessRule  bool
	AppliesTo   []string
	Expression  string
}

type UpdateStatusRequest struct {
	Type     string
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

// SchemaGenerationStatus represents the activation state of a generation.
type SchemaGenerationStatus string

const (
	GenerationStatusActive      SchemaGenerationStatus = "active"
	GenerationStatusPending     SchemaGenerationStatus = "pending"
	GenerationStatusDeactivated SchemaGenerationStatus = "deactivated"
)

// SchemaGeneration is a numbered, immutable set of schema versions.
type SchemaGeneration struct {
	ID          int64
	Status      SchemaGenerationStatus
	Created     time.Time
	Activated   *time.Time
	Deactivated *time.Time
	Schemas     []SchemaReference
}

// SchemaReference identifies a schema by name and version.
type SchemaReference struct {
	Name    string
	Version string
}

// ExemplarRecord is a stored exemplar document for a generation.
type ExemplarRecord struct {
	Name        string
	VersionHash string
	DocType     string
	Document    json.RawMessage
}

// RegisterGenerationSchema is a schema to include in a generation.
type RegisterGenerationSchema struct {
	Name          string
	Version       string
	Specification revisor.ConstraintSet
}

// ExemplarInput is an exemplar document to register with a generation.
type ExemplarInput struct {
	Name     string
	DocType  string
	Document json.RawMessage
	Version  string
}

// RegisterGenerationStoreRequest is the store-layer request for registering
// a schema generation.
type RegisterGenerationStoreRequest struct {
	Schemas    []RegisterGenerationSchema
	Activation SchemaGenerationStatus
	Exemplars  []ExemplarInput
}

type MetaTypeInfo struct {
	Name   string
	UsedBy []string
}

type Deprecation struct {
	Label    string
	Enforced bool
}

type CheckPermissionRequest struct {
	UUID        uuid.UUID
	GranteeURIs []string
	Permissions []Permission
}

type BulkCheckPermissionRequest struct {
	UUIDs       []uuid.UUID
	GranteeURIs []string
	Permissions []Permission
}

type CheckPermissionResult int

const (
	PermissionCheckDenied = iota
	PermissionCheckAllowed
	PermissionCheckNoSuchDocument
	PermissionCheckSystemLock
)

type UpdateRequest struct {
	UUID             uuid.UUID
	Updated          time.Time
	Updater          string
	Meta             newsdoc.DataMap
	ACL              []ACLEntry
	DefaultACL       []ACLEntry
	Status           []StatusUpdate
	Document         *newsdoc.Document
	MainDocument     *uuid.UUID
	IfMatch          int64
	LockToken        string
	IfWorkflowState  string
	IfStatusHeads    map[string]int64
	AttachObjects    map[string]Upload
	DetachObjects    []string
	SchemaGeneration int64
}

type DeleteRequest struct {
	UUID      uuid.UUID
	Updated   time.Time
	Updater   string
	Meta      newsdoc.DataMap
	IfMatch   int64
	LockToken string
}

type SystemState string

const (
	SystemStateDeleting  = "deleting"
	SystemStateRestoring = "restoring"
)

type DeleteRecord struct {
	ID           int64
	UUID         uuid.UUID
	URI          string
	Type         string
	Language     string
	Version      int64
	Created      time.Time
	Creator      string
	Meta         newsdoc.DataMap
	MainDocument *uuid.UUID
	Finalised    *time.Time
	Purged       *time.Time
}

type DocumentMeta struct {
	Type               string
	Nonce              uuid.UUID
	Created            time.Time
	CreatorURI         string
	Modified           time.Time
	UpdaterURI         string
	CurrentVersion     int64
	ACL                []ACLEntry
	Statuses           map[string]StatusHead
	SystemLock         SystemState
	Lock               Lock
	MainDocument       string
	WorkflowState      string
	WorkflowCheckpoint string
	Attachments        []AttachmentRef
}

type AttachmentRef struct {
	Name    string
	Version int64
}

type ACLEntry struct {
	URI         string   `json:"uri"`
	Permissions []string `json:"permissions"`
}

type Lock struct {
	Token       string
	URI         string
	Created     time.Time
	Expires     time.Time
	App         string
	Comment     string
	Exclusivity LockExclusivity
}

// LockExclusivity controls which operations a document lock blocks for
// callers that don't hold the lock token. Document updates are always
// blocked by a lock; the exclusivity level can extend the lock to also
// cover status and ACL updates.
type LockExclusivity string

const (
	// LockExclusivityDocument blocks document updates only.
	LockExclusivityDocument LockExclusivity = "document"
	// LockExclusivityStatus blocks document and status updates.
	LockExclusivityStatus LockExclusivity = "status"
	// LockExclusivityACL blocks document and ACL updates.
	LockExclusivityACL LockExclusivity = "acl"
	// LockExclusivityExclusive blocks document, status, and ACL updates.
	LockExclusivityExclusive LockExclusivity = "exclusive"
)

type LockRequest struct {
	UUID        uuid.UUID
	URI         string
	TTL         int32
	App         string
	Comment     string
	Exclusivity LockExclusivity
}

type LockResult struct {
	Token   string
	Created time.Time
	Expires time.Time
}

// LockConflictError is returned by Lock when the document is already
// locked. It carries the existing lock's holder info so handlers can
// surface useful diagnostics — typically as twirp error metadata.
//
// Embeds DocStoreError so IsDocStoreErrorCode(err, ErrCodeDocumentLock)
// continues to work.
type LockConflictError struct {
	DocStoreError
	Holder Lock
}

// Unwrap exposes the embedded DocStoreError so errors.As walks down
// to it when callers ask for a DocStoreError specifically.
func (e *LockConflictError) Unwrap() error {
	return e.DocStoreError
}

type UpdateLockRequest struct {
	UUID  uuid.UUID
	TTL   int32
	Token string
}

type DocumentUpdate struct {
	UUID           uuid.UUID
	Version        int64
	CurrentVersion int64
	Creator        string
	Created        time.Time
	Meta           newsdoc.DataMap
}

type DocumentHistoryItem struct {
	UUID     uuid.UUID
	Version  int64
	Creator  string
	Created  time.Time
	Meta     newsdoc.DataMap
	Statuses map[string][]Status
}

type Status struct {
	ID             int64
	Version        int64
	Creator        string
	Created        time.Time
	Meta           newsdoc.DataMap
	MetaDocVersion int64
}

type StatusHead struct {
	ID             int64
	Version        int64
	Creator        string
	Created        time.Time
	Meta           newsdoc.DataMap
	MetaDocVersion int64
	Language       string
}

type StatusOverviewItem struct {
	UUID               uuid.UUID
	CurrentVersion     int64
	Updated            time.Time
	Heads              map[string]Status
	WorkflowStep       string
	WorkflowCheckpoint string
	CreatorURI         string
	UpdaterURI         string
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
	ErrCodeSystemLock         DocStoreErrorCode = "system-lock"
	ErrCodeBadRequest         DocStoreErrorCode = "bad-request"
	ErrCodeExists             DocStoreErrorCode = "exists"
	ErrCodePermissionDenied   DocStoreErrorCode = "permission-denied"
	ErrCodeFailedPrecondition DocStoreErrorCode = "failed-precondition"
	ErrCodeDocumentLock       DocStoreErrorCode = "document-lock"
	ErrCodeDuplicateURI       DocStoreErrorCode = "duplicate-uri"
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
