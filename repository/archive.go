package repository

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	mrand "math/rand"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rakutentech/jwk-go/jwk"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/newsdoc"
	"golang.org/x/sync/errgroup"
)

type RestoreSpec struct {
	ACL []ACLEntry
}

type ArchiverOptions struct {
	Logger             *slog.Logger
	S3                 *s3.Client
	Bucket             string
	AssetBucket        string
	DB                 *pgxpool.Pool
	MetricsRegisterer  prometheus.Registerer
	Store              DocStore
	TypeConfigurations *TypeConfigurations
	TolerateGaps       bool
}

// Archiver reads unarchived document versions, and statuses and writes a copy
// to S3. It does this using SELECT ... FOR UPDATE SKIP LOCKED.
type Archiver struct {
	logger             *slog.Logger
	s3                 *s3.Client
	bucket             string
	assetBucket        string
	pool               *pgxpool.Pool
	signingKeys        SigningKeySet
	signingKeysChecked time.Time
	reader             *ArchiveReader
	store              DocStore
	types              *TypeConfigurations
	tolerateGaps       bool

	eventsArchiver    prometheus.Gauge
	eventArchived     *prometheus.CounterVec
	deletesProcessed  *prometheus.CounterVec
	deleteMoves       *prometheus.CounterVec
	restoresProcessed *prometheus.CounterVec
	purgesProcessed   *prometheus.CounterVec
	purgeDeletes      *prometheus.CounterVec

	cancel  func()
	stopped chan struct{}
}

func NewArchiver(opts ArchiverOptions) (*Archiver, error) {
	if opts.MetricsRegisterer == nil {
		opts.MetricsRegisterer = prometheus.DefaultRegisterer
	}

	if opts.TypeConfigurations == nil {
		return nil, errors.New("missing required type configurations")
	}

	if opts.AssetBucket == "" {
		return nil, errors.New("missing required asset bucket option")
	}

	a := Archiver{
		logger:       opts.Logger,
		s3:           opts.S3,
		pool:         opts.DB,
		store:        opts.Store,
		types:        opts.TypeConfigurations,
		bucket:       opts.Bucket,
		assetBucket:  opts.AssetBucket,
		tolerateGaps: opts.TolerateGaps,
	}

	m := elephantine.NewMetricsHelper(opts.MetricsRegisterer)

	m.Gauge(&a.eventsArchiver, prometheus.GaugeOpts{
		Name: "elephant_archiver_event_archiver_position",
		Help: "Eventlog archiver position.",
	})

	m.CounterVec(&a.eventArchived, prometheus.CounterOpts{
		Name: "elephant_event_archived_total",
		Help: "Number of document events archived.",
	}, []string{"event_type", "status"})

	m.CounterVec(&a.deletesProcessed, prometheus.CounterOpts{
		Name: "elephant_archiver_deletes_total",
		Help: "Number of document deletes processed.",
	}, []string{"status"})

	m.CounterVec(&a.deleteMoves, prometheus.CounterOpts{
		Name: "elephant_archiver_delete_moves_total",
		Help: "Number of objects moves as part of delete processing.",
	}, []string{"status"})

	m.CounterVec(&a.restoresProcessed, prometheus.CounterOpts{
		Name: "elephant_archiver_restores_total",
		Help: "Number of document restores processed.",
	}, []string{"status"})

	m.CounterVec(&a.purgesProcessed, prometheus.CounterOpts{
		Name: "elephant_archiver_purges_total",
		Help: "Number of document purges processed.",
	}, []string{"status"})

	m.CounterVec(&a.purgeDeletes, prometheus.CounterOpts{
		Name: "elephant_archiver_purge_deletes_total",
		Help: "Number of objects deleted as part of purge processing.",
	}, []string{"status"})

	if err := m.Err(); err != nil {
		return nil, fmt.Errorf("register metrics: %w", err)
	}

	a.reader = NewArchiveReader(ArchiveReaderOptions{
		S3:          opts.S3,
		Bucket:      opts.Bucket,
		SigningKeys: &a.signingKeys,
	})

	return &a, nil
}

func (a *Archiver) Run(ctx context.Context) error {
	a.logger.Info("starting archiver")

	a.stopped = make(chan struct{})
	defer close(a.stopped)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	a.cancel = cancel

	err := a.ensureSigningKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to ensure signing keys: %w", err)
	}

	grp := elephantine.NewErrGroup(ctx, a.logger)

	grp.GoWithRetries("run poll loop",
		// Try 30 times, bail after approximately 5 minutes of retries.
		30, elephantine.StaticBackoff(10*time.Second),
		1*time.Hour,
		a.runPollLoop)

	grp.GoWithRetries("run eventlog archiver",
		30, elephantine.StaticBackoff(10*time.Second),
		1*time.Hour,
		a.runEventlogArchiver)

	return grp.Wait() //nolint: wrapcheck
}

func (a *Archiver) Stop(ctx context.Context) error {
	a.cancel()

	select {
	case <-a.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *Archiver) runEventlogArchiver(ctx context.Context) error {
	lock, err := pg.NewJobLock(a.pool, a.logger, "eventlog-archiver",
		pg.JobLockOptions{})
	if err != nil {
		return fmt.Errorf("acquire job lock: %w", err)
	}

	return lock.RunWithContext(ctx, a.archiveEventlog)
}

type ArchivedEventlogItem struct {
	Event           Event     `json:"event"`
	ParentID        int64     `json:"parent_id,omitempty"`
	ParentSignature string    `json:"parent_signature,omitempty"`
	ObjectSignature string    `json:"object_signature,omitempty"`
	Archived        time.Time `json:"archived"`
}

func (av *ArchivedEventlogItem) GetArchivedTime() time.Time {
	return av.Archived
}

func (av *ArchivedEventlogItem) GetParentSignature() string {
	return av.ParentSignature
}

type ArchivedEventlogBatch struct {
	Events          []Event   `json:"events"`
	ParentSignature string    `json:"parent_signature,omitempty"`
	Archived        time.Time `json:"archived"`
}

func (av *ArchivedEventlogBatch) GetArchivedTime() time.Time {
	return av.Archived
}

func (av *ArchivedEventlogBatch) GetParentSignature() string {
	return av.ParentSignature
}

func (a *Archiver) archiveEventlog(ctx context.Context) error {
	a.logger.Info("starting eventlog archiver")

	q := postgres.New(a.pool)

	newEvents := make(chan int64, 5)
	a.store.OnEventlog(ctx, newEvents)

	pollDelay := 100 * time.Millisecond

	state, err := q.GetEventlogArchiver(ctx, 1)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("get current position: %w", err)
	}

	for {
		items, err := q.GetEventlog(ctx, postgres.GetEventlogParams{
			After:    state.Position,
			RowLimit: 100,
		})
		if err != nil {
			return fmt.Errorf("read eventlog: %w", err)
		}

		for _, item := range items {
			newState, err := a.archiveEventlogItem(ctx, item, state)
			if err != nil {
				a.eventArchived.WithLabelValues(item.Event, "error")

				return err
			}

			// Bump metrics on success.
			a.eventsArchiver.Set(float64(item.ID))
			a.eventArchived.WithLabelValues(item.Event, "ok")

			state = newState
		}

		select {
		case <-time.After(pollDelay):
		case <-newEvents:
			pollDelay = 5 * time.Second
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *Archiver) archiveEventlogItem(
	ctx context.Context,
	item postgres.GetEventlogRow,
	state postgres.GetEventlogArchiverRow,
) (_ postgres.GetEventlogArchiverRow, outErr error) {
	if item.ID != state.Position+1 && !a.tolerateGaps {
		return postgres.GetEventlogArchiverRow{},
			fmt.Errorf("inconsistent eventlog, expected event %d, got %d",
				state.Position+1, item.ID)
	}

	event, err := eventlogRowToEvent(item)
	if err != nil {
		return postgres.GetEventlogArchiverRow{},
			fmt.Errorf(
				"invalid event log item %d: %w", item.ID, err)
	}

	archiveItem := ArchivedEventlogItem{
		Event:           event,
		ParentID:        state.Position,
		ParentSignature: state.LastSignature,
		Archived:        time.Now(),
	}

	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return postgres.GetEventlogArchiverRow{},
			fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	var (
		objRef *archiveObjectRef
		objErr error
		// deletedDoc signals that a document was deleted *before* it
		// could be archived. This only happens in a repo migrated from
		// a version before v1.2.0.
		deletedDoc bool
	)

	// Create a new cleanup context that's detached from parent cancel, but
	// with a timeout so that we don't hang.
	cleanupCtx, cancelCleanup := context.WithTimeout(
		context.WithoutCancel(ctx), 10*time.Second)
	defer cancelCleanup()

	switch event.Event {
	case TypeACLUpdate, TypeDeleteDocument, TypeRestoreFinished, TypeWorkflow:
		info, err := q.GetDocumentRow(ctx, event.UUID)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			objErr = errDocumentDeleted
		case err != nil:
			return postgres.GetEventlogArchiverRow{},
				fmt.Errorf("get document info: %w", err)
		case info.Nonce != event.Nonce:
			objErr = errDocumentDeleted
		}
	case TypeDocumentVersion:
		objRef, objErr = a.archiveDocumentVersion(ctx, cleanupCtx, event, tx)
	case TypeNewStatus:
		objRef, objErr = a.archiveDocumentStatus(ctx, cleanupCtx, event, tx)
	case TypeEventIgnored:
		panic(fmt.Sprintf("unexpected repository.EventType: %#v", event.Event))
	default:
		panic(fmt.Sprintf("unexpected repository.EventType: %#v", event.Event))
	}

	switch {
	case errors.Is(objErr, errDocumentDeleted):
		// We are ignoring documents that have been deleted
		// before they're archived. This is something that only
		// will happen in an elephant installation that started
		// before v1.2.0.
		deletedDoc = true
	case objErr != nil:
		return postgres.GetEventlogArchiverRow{},
			fmt.Errorf("archive event object: %w", objErr)
	case objRef != nil:
		archiveItem.ObjectSignature = objRef.Signature

		// We try to clean up the S3 object for the related object if the
		// operation fails.
		defer func() {
			if outErr == nil {
				return
			}

			objRef.Remove(cleanupCtx)
		}()
	}

	key := fmt.Sprintf("events/%020d.json", item.ID)

	ref, err := a.storeArchiveObject(ctx, key, &archiveItem)
	if err != nil {
		return postgres.GetEventlogArchiverRow{},
			fmt.Errorf("archive log item %d: %w",
				item.ID, err)
	}

	// We try to clean up the eventlog item S3 object if the operation
	// fails.
	defer func() {
		if outErr == nil {
			return
		}

		ref.Remove(cleanupCtx)
	}()

	err = q.SetEventlogArchiver(ctx,
		postgres.SetEventlogArchiverParams{
			Size:      1,
			Position:  item.ID,
			Signature: ref.Signature,
		})
	if err != nil {
		return postgres.GetEventlogArchiverRow{},
			fmt.Errorf("update archiver state: %w", err)
	}

	if !deletedDoc {
		_, err := q.UpdateDocumentUnarchivedCount(ctx,
			postgres.UpdateDocumentUnarchivedCountParams{
				UUID:  item.UUID,
				Delta: -1,
			})
		if err != nil {
			return postgres.GetEventlogArchiverRow{},
				fmt.Errorf("update document archive counter: %w", err)
		}
	}

	err = q.SetEventlogItemAsArchived(ctx,
		postgres.SetEventlogItemAsArchivedParams{
			ID:        item.ID,
			Signature: pg.Text(ref.Signature),
		})
	if err != nil {
		return postgres.GetEventlogArchiverRow{},
			fmt.Errorf("set item as archived: %w", err)
	}

	err = pg.Publish(ctx, tx, NotifyArchived, ArchivedEvent{
		Type:    ArchiveEventTypeLogItem,
		EventID: item.ID,
		UUID:    item.UUID,
	})
	if err != nil {
		return postgres.GetEventlogArchiverRow{},
			fmt.Errorf("send archived notification: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return postgres.GetEventlogArchiverRow{},
			fmt.Errorf("commit changes: %w", err)
	}

	state.LastSignature = ref.Signature
	state.Position = item.ID

	return postgres.GetEventlogArchiverRow{
		Position:      item.ID,
		LastSignature: ref.Signature,
	}, nil
}

func (a *Archiver) runPollLoop(ctx context.Context) error {
	wait := make(map[string]time.Time)

	// This is fine, pseudorandom is enough, we're not using this for crypto
	// work.
	//
	//nolint:gosec
	r := mrand.New(mrand.NewSource(time.Now().Unix()))

	runWithDelay := func(
		name string, now time.Time,
		metric *prometheus.CounterVec,
		fn func(ctx context.Context) (bool, error),
	) error {
		if now.Before(wait[name]) {
			return nil
		}

		ok, err := fn(ctx)
		if err != nil {
			metric.WithLabelValues("error").Inc()

			return fmt.Errorf(
				"failed to %s: %w", name, err)
		}

		if !ok {
			d := 500 * time.Millisecond

			n := d / 2
			d = n + (time.Duration(r.Int()) % n)

			wait[name] = time.Now().Add(d)

			return nil
		}

		metric.WithLabelValues("ok").Inc()

		return nil
	}

	for {
		if time.Since(a.signingKeysChecked) > 24*time.Hour {
			err := a.ensureSigningKeys(ctx)
			if err != nil {
				return fmt.Errorf(
					"failed to ensure signing keys: %w", err)
			}
		}

		now := time.Now()

		err := runWithDelay(
			"process document deletes", now,
			a.deletesProcessed, a.processDeletes)
		if err != nil {
			return err
		}

		err = runWithDelay(
			"process document restores", now,
			a.restoresProcessed, a.processRestores)
		if err != nil {
			return err
		}

		err = runWithDelay(
			"process document purges", now,
			a.purgesProcessed, a.processPurges)
		if err != nil {
			return err
		}

		next := now.Add(1 * time.Second)

		for _, t := range wait {
			if t.Before(next) {
				next = t
			}
		}

		select {
		case <-time.After(time.Until(next)):
		case <-ctx.Done():
			return nil
		}
	}
}

type DeleteManifest struct {
	Nonce       uuid.UUID        `json:"nonce"`
	LastVersion int64            `json:"last_version"`
	Heads       map[string]int64 `json:"heads"`
	ACL         []ACLEntry       `json:"acl"`
	Archived    time.Time        `json:"archived"`
	Attached    []AttachedObject `json:"attached"`
}

type AttachedObject struct {
	Document      uuid.UUID         `json:"document"`
	Name          string            `json:"name"`
	Version       int64             `json:"version"`
	ObjectVersion string            `json:"object_version"`
	AttachedAt    int64             `json:"attached_at"`
	CreatedBy     string            `json:"created_by"`
	CreatedAt     time.Time         `json:"created_at"`
	Filename      string            `json:"filename"`
	Mimetype      string            `json:"mimetype"`
	Props         map[string]string `json:"props"`
}

func (m *DeleteManifest) GetArchivedTime() time.Time {
	return m.Archived
}

func (m *DeleteManifest) GetParentSignature() string {
	return ""
}

func (a *Archiver) processDeletes(
	ctx context.Context,
) (_ bool, outErr error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	deleteOrder, err := q.GetDocumentForDeletion(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to get pending deletions: %w", err)
	}

	prefix := fmt.Sprintf("documents/%s/", deleteOrder.UUID)
	dstPrefix := fmt.Sprintf("deleted/%s/%019d/",
		deleteOrder.UUID, deleteOrder.ID)

	paginator := s3.NewListObjectsV2Paginator(a.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(a.bucket),
		Prefix: aws.String(prefix),
	})

	const workerCount = 8

	group, gCtx := errgroup.WithContext(ctx)
	moves := make(chan string, workerCount)

	for range workerCount {
		group.Go(func() error {
			for key := range moves {
				baseKey := strings.TrimPrefix(key, prefix)
				dstKey := dstPrefix + baseKey

				err := a.moveDeletedObject(gCtx, a.bucket, key, dstKey)
				if err != nil {
					return err
				}
			}

			return nil
		})
	}

	group.Go(func() error {
		defer close(moves)

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("failed to list objects: %w", err)
			}

			for _, o := range page.Contents {
				moves <- *o.Key
			}
		}

		return nil
	})

	group.Go(func() error {
		for _, o := range deleteOrder.Attachments {
			key := fmt.Sprintf("objects/%s/%s", o.Name, o.Document)
			dstKey := fmt.Sprintf("%sattached/%s", dstPrefix, o.Name)

			err := a.moveDeletedObject(gCtx, a.assetBucket, key, dstKey)
			if err != nil {
				return fmt.Errorf("copy attachment: %w", err)
			}
		}

		return nil
	})

	err = group.Wait()
	if err != nil {
		return false, fmt.Errorf("failed to move all document objects: %w", err)
	}

	manifest := DeleteManifest{
		Nonce:       deleteOrder.Nonce,
		Archived:    time.Now(),
		LastVersion: deleteOrder.Version,
		Heads:       deleteOrder.Heads,
		ACL:         make([]ACLEntry, len(deleteOrder.Acl)),
		Attached:    make([]AttachedObject, len(deleteOrder.Attachments)),
	}

	for i := range deleteOrder.Acl {
		manifest.ACL[i] = ACLEntry{
			URI:         deleteOrder.Acl[i].URI,
			Permissions: deleteOrder.Acl[i].Permissions,
		}
	}

	for i, a := range deleteOrder.Attachments {
		manifest.Attached[i] = AttachedObject{
			Document:      a.Document,
			Name:          a.Name,
			Version:       a.Version,
			ObjectVersion: a.ObjectVersion,
			AttachedAt:    a.AttachedAt,
			CreatedBy:     a.CreatedBy,
			CreatedAt:     a.CreatedAt.Time,
			Filename:      a.Meta.Filename,
			Mimetype:      a.Meta.Mimetype,
			Props:         a.Meta.Props,
		}
	}

	ref, err := a.storeArchiveObject(
		ctx, dstPrefix+"manifest.json", &manifest,
	)
	if err != nil {
		return false, fmt.Errorf("store delete manifest")
	}

	// We try to clean up the S3 object if the operation fails.
	defer func() {
		if outErr == nil {
			return
		}

		ref.Remove(ctx)
	}()

	count, err := q.FinaliseDocumentDelete(ctx, deleteOrder.UUID)
	if err != nil {
		return false, fmt.Errorf("delete document row: %w", err)
	}

	if count != 1 {
		return false, errors.New(
			"no match for document delete, database might be inconsistent")
	}

	err = q.FinaliseDeleteRecord(ctx, postgres.FinaliseDeleteRecordParams{
		UUID:      deleteOrder.UUID,
		ID:        deleteOrder.ID,
		Finalised: pg.Time(time.Now()),
	})
	if err != nil {
		return false, fmt.Errorf("set delete record as finished: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return false, fmt.Errorf("commit transaction: %w", err)
	}

	return true, nil
}

type archiveObjectRef struct {
	Signature string
	Remove    func(ctx context.Context)
}

func (a *Archiver) storeArchiveObject(
	ctx context.Context, key string, v ArchivedObject,
) (*archiveObjectRef, error) {
	objectBody, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf(
			"delete manifest to JSON: %w", err)
	}

	checksum := sha256.Sum256(objectBody)

	signingKey := a.signingKeys.CurrentKey(v.GetArchivedTime())
	if signingKey == nil {
		return nil, fmt.Errorf(
			"no signing keys have been configured: %w", err)
	}

	signature, err := NewArchiveSignature(signingKey, checksum)
	if err != nil {
		return nil, fmt.Errorf("failed to sign archive data: %w", err)
	}

	putRes, err := a.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(a.bucket),
		Key:               aws.String(key),
		Body:              bytes.NewReader(objectBody),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumSHA256: aws.String(
			base64.StdEncoding.EncodeToString(checksum[:]),
		),
		ContentType:   aws.String("application/json"),
		ContentLength: aws.Int64(int64(len(objectBody))),
		Metadata: map[string]string{
			"elephant-signature": signature.String(),
		},
	})
	if err != nil {
		return nil, fmt.Errorf(
			"failed to store archive object: %w", err)
	}

	ref := archiveObjectRef{
		Signature: signature.String(),
		Remove: func(ctx context.Context) {
			_, cErr := a.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket:    aws.String(a.bucket),
				Key:       aws.String(key),
				VersionId: putRes.VersionId,
			})
			if cErr != nil {
				a.logger.ErrorContext(ctx,
					"failed to clean up archive object after failure",
					elephantine.LogKeyError, cErr,
					elephantine.LogKeyBucket, a.bucket,
					elephantine.LogKeyObjectKey, key)
			}
		},
	}

	return &ref, nil
}

func (a *Archiver) processRestores(
	ctx context.Context,
) (_ bool, outErr error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin transaction: %w", err)
	}

	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	// Drop restore requests whose delete record has been purged.
	err = q.DropInvalidRestoreRequests(ctx)
	if err != nil {
		return false, fmt.Errorf("drop invalid restores: %w", err)
	}

	req, err := q.GetNextRestoreRequest(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("get next restore request: %w", err)
	}

	var spec RestoreSpec

	err = json.Unmarshal(req.Spec, &spec)
	if err != nil {
		return false, fmt.Errorf("unmarshal restore spec: %w", err)
	}

	nonce := req.Nonce
	requestPrefix := fmt.Sprintf("deleted/%s/%019d",
		req.UUID, req.DeleteRecordID)
	manifestKey := requestPrefix + "/manifest.json"
	versionsPrefix := requestPrefix + "/versions"
	statusesPrefix := requestPrefix + "/statuses"

	manifest, _, err := a.reader.ReadDeleteManifest(ctx, manifestKey)
	if err != nil {
		return false, fmt.Errorf("failed to read manifest: %w", err)
	}

	// Default to using the previous ACL.
	if len(spec.ACL) == 0 {
		spec.ACL = manifest.ACL
	}

	var (
		parentSig    string
		lastAttached = map[string]int64{}
	)

	// Iterate through the versions we want to restore.
	for version := int64(1); version <= manifest.LastVersion; version++ {
		key := fmt.Sprintf("%s/%019d.json", versionsPrefix, version)

		attached, sig, err := a.restoreDocumentVersion(
			ctx, tx, parentSig, version, nonce, req, key)
		if err != nil {
			return false, fmt.Errorf(
				"restore version %d: %w", version, err)
		}

		for _, name := range attached {
			lastAttached[name] = version
		}

		parentSig = sig
	}

	var statuses []*ArchivedDocumentStatus

	for name, lastID := range manifest.Heads {
		parentSig = ""

		for id := int64(1); id <= lastID; id++ {
			key := fmt.Sprintf("%s/%s/%019d.json", statusesPrefix, name, id)

			stat, sig, err := a.reader.ReadDocumentStatus(ctx, key, &parentSig)
			if err != nil {
				return false, fmt.Errorf("read archived status: %w", err)
			}

			statuses = append(statuses, stat)

			parentSig = sig
		}
	}

	slices.SortFunc(statuses, func(a, b *ArchivedDocumentStatus) int {
		return int(a.EventID - b.EventID)
	})

	for _, stat := range statuses {
		err = a.restoreDocumentStatus(
			ctx, tx, stat.ID, stat, nonce, req)
		if err != nil {
			return false, fmt.Errorf(
				"restore status %q %d: %w", stat.Name, stat.ID, err)
		}
	}

	attachedNames := make([]string, len(manifest.Attached))

	// Adding the attached object at the end of the restore, but that's fine
	// as it will become visible at the same time of commit.
	for i, o := range manifest.Attached {
		srcKey := fmt.Sprintf("%s/attached/%s", requestPrefix, o.Name)
		key := fmt.Sprintf("objects/%s/%s", o.Name, o.Document)

		res, err := a.s3.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(a.assetBucket),
			Key:        aws.String(key),
			CopySource: aws.String(a.bucket + "/" + srcKey),
		})
		if err != nil {
			return false, fmt.Errorf("restore attachment %q: %w",
				o.Name, err)
		}

		if res.VersionId == nil {
			return false, errors.New("attachment bucket is not versioned")
		}

		attachedAt, ok := lastAttached[o.Name]
		if !ok {
			return false, errors.New("no known attach document version")
		}

		err = q.AddAttachedObject(ctx, postgres.AddAttachedObjectParams{
			Document:      o.Document,
			Name:          o.Name,
			Version:       1,
			ObjectVersion: *res.VersionId,
			AttachedAt:    attachedAt,
			CreatedBy:     o.CreatedBy,
			CreatedAt:     pg.Time(o.CreatedAt),
			Meta: postgres.AssetMetadata{
				Filename: o.Filename,
				Mimetype: o.Mimetype,
				Props:    o.Props,
			},
		})
		if err != nil {
			return false, fmt.Errorf("store attachment data: %w", err)
		}

		err = q.SetCurrentAttachedObject(ctx,
			postgres.SetCurrentAttachedObjectParams{
				Document: o.Document,
				Name:     o.Name,
				Version:  1,
				Deleted:  false,
			})
		if err != nil {
			return false, fmt.Errorf("set current attachment: %w", err)
		}

		attachedNames[i] = o.Name
	}

	var (
		acls      = make([]postgres.ACLUpdateParams, len(spec.ACL))
		eventACLs = make([]postgres.ACLEntry, len(spec.ACL))
	)

	for i, acl := range spec.ACL {
		acls[i] = postgres.ACLUpdateParams{
			UUID:        req.UUID,
			URI:         acl.URI,
			Permissions: acl.Permissions,
		}

		eventACLs[i] = postgres.ACLEntry{
			URI:         acl.URI,
			Permissions: acl.Permissions,
		}
	}

	docInfo, err := q.GetDocumentRow(ctx, req.UUID)
	if err != nil {
		return false, fmt.Errorf(
			"read document information after restore: %w", err)
	}

	timespans := TimespansAsTuples(TimestampRangesToTimespans(docInfo.Time))

	var aclErrors []error

	q.ACLUpdate(ctx, acls).Exec(func(_ int, err error) {
		if err != nil {
			aclErrors = append(aclErrors, err)
		}
	})

	if len(aclErrors) > 0 {
		return false, fmt.Errorf("failed to update ACLs: %w",
			errors.Join(aclErrors...))
	}

	err = q.InsertACLAuditEntry(ctx, postgres.InsertACLAuditEntryParams{
		UUID:        req.UUID,
		Type:        pg.TextOrNull(docInfo.Type),
		Updated:     pg.Time(time.Now()),
		UpdaterUri:  docInfo.UpdaterUri,
		Language:    docInfo.Language.String,
		SystemState: pg.Text(SystemStateRestoring),
	})
	if err != nil {
		return false, fmt.Errorf("failed to record audit trail: %w", err)
	}

	err = q.ClearSystemState(ctx, req.UUID)
	if err != nil {
		return false, fmt.Errorf("clear system state: %w", err)
	}

	err = q.FinishRestoreRequest(ctx, postgres.FinishRestoreRequestParams{
		ID:       req.ID,
		Finished: pg.Time(time.Now()),
	})
	if err != nil {
		return false, fmt.Errorf("set restore as finished: %w", err)
	}

	err = addEventToOutbox(ctx, tx, postgres.OutboxEvent{
		Event:       string(TypeACLUpdate),
		UUID:        req.UUID,
		Version:     manifest.LastVersion,
		Nonce:       nonce,
		Timestamp:   time.Now(),
		Updater:     req.Creator,
		ACL:         eventACLs,
		Type:        docInfo.Type,
		Language:    docInfo.Language.String,
		SystemState: SystemStateRestoring,
		Labels:      docInfo.Labels,
		Timespans:   timespans,
	})
	if err != nil {
		return false, fmt.Errorf("add restore finished event to outbox: %w", err)
	}

	err = addEventToOutbox(ctx, tx, postgres.OutboxEvent{
		Event:     string(TypeRestoreFinished),
		UUID:      req.UUID,
		Nonce:     nonce,
		Timestamp: time.Now(),
		Version:   manifest.LastVersion,
		Updater:   req.Creator,
		Type:      docInfo.Type,
		Language:  docInfo.Language.String,
		Labels:    docInfo.Labels,
		Timespans: timespans,
	})
	if err != nil {
		return false, fmt.Errorf("add restore finished event to outbox: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return false, fmt.Errorf("commit restore: %w", err)
	}

	return false, nil
}

func (a *Archiver) restoreDocumentVersion(
	ctx context.Context, tx postgres.DBTX, parentSig string,
	id int64, nonce uuid.UUID, req postgres.GetNextRestoreRequestRow, key string,
) ([]string, string, error) {
	q := postgres.New(tx)

	dv, sig, err := a.reader.ReadDocumentVersion(ctx, key, &parentSig)
	if err != nil {
		return nil, "", fmt.Errorf("read archived version: %w", err)
	}

	if dv.Version != id {
		return nil, "", fmt.Errorf("expected document version %d, got %d",
			id, dv.Version)
	}

	var doc newsdoc.Document

	err = json.Unmarshal(dv.DocumentData, &doc)
	if err != nil {
		return nil, "", fmt.Errorf("invalid document data: %w", err)
	}

	// Run type extractors, we don't fail the restore on extract failure.
	var (
		timespans []Timespan
		labels    []string
	)

	tConf, configured, err := a.types.GetConfiguration(ctx, doc.Type)
	if err != nil {
		return nil, "", fmt.Errorf("get type configuration: %w", err)
	}

	if configured && len(tConf.TimeExpressions) > 0 {
		ts, err := a.types.TimespansForDocument(ctx, doc)
		if err != nil {
			a.logger.Error("failed to extract timespans on document restore",
				elephantine.LogKeyError, err)
		}

		timespans = ts
	}

	if configured && len(tConf.LabelExpressions) > 0 {
		lbl, err := a.types.LabelsForDocument(ctx, doc)
		if err != nil {
			a.logger.Error("failed to extract labels on document restore",
				elephantine.LogKeyError, err)
		}

		labels = lbl
	}

	evt, _, err := a.reader.ReadEvent(ctx, dv.EventID, nil)
	if err != nil {
		return nil, "", fmt.Errorf("read event information for version: %w", err)
	}

	if evt.ObjectSignature != sig {
		return nil, "", fmt.Errorf(
			"document signature didn't match the stored event")
	}

	updatedMetaData, err := annotateMeta(dv.Meta, newsdoc.DataMap{
		"restored_at": time.Now().Format(time.RFC3339),
		"restored_by": req.Creator,
	})
	if err != nil {
		return nil, "", fmt.Errorf(
			"annotate metadata: %w", err)
	}

	err = q.CreateDocumentVersion(ctx, postgres.CreateDocumentVersionParams{
		UUID:         dv.UUID,
		Version:      id,
		Created:      pg.Time(dv.Created),
		CreatorUri:   dv.CreatorURI,
		Meta:         updatedMetaData,
		DocumentData: dv.DocumentData,
		Time:         TimespansToTimestampMultiranges(timespans),
		Labels:       labels,
	})
	if err != nil {
		return nil, "", fmt.Errorf("create document version: %w", err)
	}

	err = q.UpsertDocument(ctx, postgres.UpsertDocumentParams{
		UUID:       dv.UUID,
		Nonce:      nonce,
		URI:        dv.URI,
		Type:       dv.Type,
		Created:    pg.Time(dv.Created),
		CreatorUri: dv.CreatorURI,
		Version:    id,
		MainDoc:    pg.PUUID(dv.MainDocument),
		Language:   pg.Text(dv.Language),
		Time:       TimespansToTimestampMultiranges(timespans),
		Labels:     labels,
	})
	if err != nil {
		return nil, "", fmt.Errorf("update document row: %w", err)
	}

	err = addEventToOutbox(ctx, tx, postgres.OutboxEvent{
		Event:           string(TypeDocumentVersion),
		UUID:            req.UUID,
		Nonce:           nonce,
		Timestamp:       time.Now(),
		Version:         id,
		Updater:         dv.CreatorURI,
		Type:            dv.Type,
		Language:        dv.Language,
		SystemState:     SystemStateRestoring,
		AttachedObjects: evt.Event.AttachedObjects,
		DetachedObjects: evt.Event.DetachedObjects,
		Timespans:       TimespansAsTuples(timespans),
		Labels:          labels,
	})
	if err != nil {
		return nil, "", fmt.Errorf("add last version event to outbox: %w", err)
	}

	return evt.Event.AttachedObjects, sig, nil
}

func annotateMeta(originalMeta []byte, add newsdoc.DataMap) ([]byte, error) {
	var meta newsdoc.DataMap

	if originalMeta != nil {
		err := json.Unmarshal(originalMeta, &meta)
		if err != nil {
			return nil, fmt.Errorf(
				"invalid original metadata: %w", err)
		}
	} else {
		meta = make(newsdoc.DataMap, len(add))
	}

	maps.Copy(meta, add)

	updatedMetaData, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal updated metadata: %w", err)
	}

	return updatedMetaData, nil
}

func (a *Archiver) restoreDocumentStatus(
	ctx context.Context, tx postgres.DBTX,
	id int64, stat *ArchivedDocumentStatus, nonce uuid.UUID,
	req postgres.GetNextRestoreRequestRow,
) error {
	q := postgres.New(tx)

	err := q.CreateStatusHead(ctx, postgres.CreateStatusHeadParams{
		UUID:        stat.UUID,
		Name:        stat.Name,
		Type:        stat.Type,
		Version:     stat.Version,
		ID:          id,
		Created:     pg.Time(stat.Created),
		CreatorUri:  stat.CreatorURI,
		Language:    stat.Language,
		SystemState: pg.TextOrNull(SystemStateRestoring),
	})
	if err != nil {
		return fmt.Errorf("create status head: %w", err)
	}

	meta := map[string]string{}

	if len(stat.Meta) > 0 {
		err := json.Unmarshal(stat.Meta, &meta)
		if err != nil {
			return fmt.Errorf("unmarshal status meta: %w", err)
		}
	}

	meta["restored_at"] = time.Now().Format(time.RFC3339)
	meta["restored_by"] = req.Creator

	err = q.InsertDocumentStatus(ctx, postgres.InsertDocumentStatusParams{
		UUID:           stat.UUID,
		Name:           stat.Name,
		ID:             id,
		Version:        stat.Version,
		Created:        pg.Time(stat.Created),
		CreatorUri:     stat.CreatorURI,
		Meta:           meta,
		MetaDocVersion: stat.MetaDocVersion,
	})
	if err != nil {
		return fmt.Errorf("insert document status: %w", err)
	}

	err = addEventToOutbox(ctx, tx, postgres.OutboxEvent{
		Event:       string(TypeNewStatus),
		UUID:        stat.UUID,
		Nonce:       nonce,
		Timestamp:   time.Now(),
		Status:      stat.Name,
		StatusID:    id,
		Version:     stat.Version,
		Updater:     stat.CreatorURI,
		Type:        stat.Type,
		Language:    stat.Language,
		SystemState: SystemStateRestoring,
	})
	if err != nil {
		return fmt.Errorf(
			"status event to outbox: %w", err)
	}

	return nil
}

func (a *Archiver) processPurges(
	ctx context.Context,
) (_ bool, outErr error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin transaction: %w", err)
	}

	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	// Drop purge requests whose delete record has already been purged.
	err = q.DropRedundantPurgeRequests(ctx)
	if err != nil {
		return false, fmt.Errorf("drop redundant purge requests: %w", err)
	}

	req, err := q.GetNextPurgeRequest(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("get next purge request: %w", err)
	}

	prefix := fmt.Sprintf("deleted/%s/%019d/",
		req.UUID, req.DeleteRecordID)

	paginator := s3.NewListObjectsV2Paginator(a.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(a.bucket),
		Prefix: aws.String(prefix),
		// 1000 is the default, but I want this to be explicitly
		// enforced as the limit for DeleteObjects is 1000. If the
		// object pagination default changed that could break the delete
		// calls.
		MaxKeys: aws.Int32(1000),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return false, fmt.Errorf("list archived objects: %w", err)
		}

		var objects []types.ObjectIdentifier

		for _, obj := range page.Contents {
			objects = append(objects, types.ObjectIdentifier{
				Key: aws.String(*obj.Key),
			})
		}

		if len(objects) == 0 {
			break
		}

		res, err := a.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &a.bucket,
			Delete: &types.Delete{Objects: objects},
		})
		if err != nil {
			a.purgeDeletes.WithLabelValues("error").Inc()

			return false, fmt.Errorf("delete objects: %w", err)
		}

		a.purgeDeletes.WithLabelValues("ok").Inc()

		if len(res.Errors) > 0 {
			return false, fmt.Errorf(
				"%d objects failed to delete", len(res.Errors))
		}
	}

	purgedTime := time.Now()

	err = q.PurgeDeleteRecordDetails(ctx,
		postgres.PurgeDeleteRecordDetailsParams{
			ID:         req.DeleteRecordID,
			PurgedTime: pg.Time(purgedTime),
		})
	if err != nil {
		return false, fmt.Errorf("purge delete record details: %w", err)
	}

	err = q.FinishPurgeRequest(ctx, postgres.FinishPurgeRequestParams{
		ID:       req.ID,
		Finished: pg.Time(purgedTime),
	})
	if err != nil {
		return false, fmt.Errorf("mark purge request as finished: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return false, fmt.Errorf("commit purge: %w", err)
	}

	return true, nil
}

func (a *Archiver) moveDeletedObject(
	ctx context.Context, sourceBucket string, key string, dstKey string,
) (outErr error) {
	defer func() {
		status := "ok"

		if outErr != nil {
			status = "error"
		}

		a.deleteMoves.WithLabelValues(status).Inc()
	}()

	_, err := a.s3.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(dstKey),
		CopySource: aws.String(
			sourceBucket + "/" + key,
		),
	})
	if err != nil {
		return fmt.Errorf(
			"failed to copy object: %w", err)
	}

	_, err = a.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(sourceBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf(
			"failed to delete source object after copy: %w", err)
	}

	return nil
}

type ArchivedObject interface {
	GetArchivedTime() time.Time
	GetParentSignature() string
}

type ArchivedDocumentVersion struct {
	UUID            uuid.UUID       `json:"uuid"`
	URI             string          `json:"uri"`
	Type            string          `json:"type"`
	Language        string          `json:"language"`
	Version         int64           `json:"version"`
	Created         time.Time       `json:"created"`
	CreatorURI      string          `json:"creator_uri"`
	Meta            json.RawMessage `json:"meta,omitempty"`
	DocumentData    json.RawMessage `json:"document_data"`
	MainDocument    *uuid.UUID      `json:"main_document,omitempty"`
	ParentSignature string          `json:"parent_signature,omitempty"`
	Archived        time.Time       `json:"archived"`
	Attached        []string        `json:"attached"`
	Detached        []string        `json:"detached"`
	EventID         int64           `json:"event_id"`
}

func (av *ArchivedDocumentVersion) GetArchivedTime() time.Time {
	return av.Archived
}

func (av *ArchivedDocumentVersion) GetParentSignature() string {
	return av.ParentSignature
}

var errDocumentDeleted = errors.New("document deleted")

func (a *Archiver) archiveDocumentVersion(
	ctx context.Context,
	cleanupCtx context.Context,
	event Event,
	tx postgres.DBTX,
) (_ *archiveObjectRef, outErr error) {
	q := postgres.New(tx)

	unarchived, err := q.GetDocumentVersionForArchiving(ctx,
		postgres.GetDocumentVersionForArchivingParams{
			UUID:    event.UUID,
			Version: event.Version,
		})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errDocumentDeleted
	} else if err != nil {
		return nil, fmt.Errorf(
			"failed to get unarchived version from the database: %w",
			err)
	}

	if unarchived.Nonce != event.Nonce {
		return nil, errDocumentDeleted
	}

	dv := ArchivedDocumentVersion{
		UUID:            unarchived.UUID,
		URI:             unarchived.URI,
		Type:            unarchived.Type,
		Language:        unarchived.Language.String,
		Version:         unarchived.Version,
		Created:         unarchived.Created.Time,
		CreatorURI:      unarchived.CreatorUri,
		Meta:            unarchived.Meta,
		DocumentData:    unarchived.DocumentData,
		MainDocument:    pg.ToUUIDPointer(unarchived.MainDoc),
		ParentSignature: unarchived.ParentSignature.String,
		Archived:        time.Now(),
		EventID:         event.ID,
		Attached:        event.AttachedObjects,
		Detached:        event.DetachedObjects,
	}

	key := fmt.Sprintf("documents/%s/versions/%019d.json", dv.UUID, dv.Version)

	ref, err := a.storeArchiveObject(ctx, key, &dv)
	if err != nil {
		return nil, err
	}

	// We try to clean up the S3 object if the operation fails.
	defer func() {
		if outErr == nil {
			return
		}

		ref.Remove(cleanupCtx)
	}()

	err = q.SetDocumentVersionAsArchived(ctx,
		postgres.SetDocumentVersionAsArchivedParams{
			UUID:      dv.UUID,
			Version:   dv.Version,
			Signature: ref.Signature,
		})
	if err != nil {
		return nil, fmt.Errorf("failed to update archived status in db: %w",
			err)
	}

	err = pg.Publish(ctx, tx, NotifyArchived, ArchivedEvent{
		Type:    ArchiveEventTypeVersion,
		UUID:    dv.UUID,
		Version: dv.Version,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send archive notification: %w", err)
	}

	return ref, nil
}

type ArchivedDocumentStatus struct {
	UUID            uuid.UUID       `json:"uuid"`
	Name            string          `json:"name"`
	ID              int64           `json:"id"`
	Type            string          `json:"type"`
	Version         int64           `json:"version"`
	Created         time.Time       `json:"created"`
	CreatorURI      string          `json:"creator_uri"`
	Meta            json.RawMessage `json:"meta,omitempty"`
	ParentSignature string          `json:"parent_signature,omitempty"`
	Archived        time.Time       `json:"archived"`
	Language        string          `json:"language"`
	MetaDocVersion  int64           `json:"meta_doc_version,omitempty"`
	EventID         int64           `json:"event_id"`
}

func (as *ArchivedDocumentStatus) GetArchivedTime() time.Time {
	return as.Archived
}

func (as *ArchivedDocumentStatus) GetParentSignature() string {
	return as.ParentSignature
}

func (a *Archiver) archiveDocumentStatus(
	ctx context.Context,
	cleanupCtx context.Context,
	event Event,
	tx postgres.DBTX,
) (_ *archiveObjectRef, outErr error) {
	q := postgres.New(tx)

	unarchived, err := q.GetDocumentStatusForArchiving(ctx,
		postgres.GetDocumentStatusForArchivingParams{
			UUID: event.UUID,
			Name: event.Status,
			ID:   event.StatusID,
		})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errDocumentDeleted
	} else if err != nil {
		return nil, fmt.Errorf(
			"failed to get status from the database: %w", err)
	}

	if unarchived.Nonce != event.Nonce {
		return nil, errDocumentDeleted
	}

	var metaData []byte

	if unarchived.Meta != nil {
		d, err := json.Marshal(unarchived.Meta)
		if err != nil {
			return nil, fmt.Errorf("marshal metadata: %w", err)
		}

		metaData = d
	}

	ds := ArchivedDocumentStatus{
		EventID:         event.ID,
		UUID:            unarchived.UUID,
		Name:            unarchived.Name,
		ID:              unarchived.ID,
		Type:            event.Type,
		Version:         unarchived.Version,
		Created:         unarchived.Created.Time,
		CreatorURI:      unarchived.CreatorUri,
		Meta:            metaData,
		ParentSignature: unarchived.ParentSignature.String,
		Archived:        time.Now(),
		Language:        event.Language,
		MetaDocVersion:  unarchived.MetaDocVersion.Int64,
	}

	key := fmt.Sprintf("documents/%s/statuses/%s/%019d.json",
		ds.UUID, ds.Name, ds.ID)

	ref, err := a.storeArchiveObject(ctx, key, &ds)
	if err != nil {
		return nil, err
	}

	// We try to clean up the S3 object if the operation fails.
	defer func() {
		if outErr == nil {
			return
		}

		ref.Remove(cleanupCtx)
	}()

	err = q.SetDocumentStatusAsArchived(ctx,
		postgres.SetDocumentStatusAsArchivedParams{
			Signature: ref.Signature,
			UUID:      ds.UUID,
			Name:      ds.Name,
			ID:        ds.ID,
		})
	if err != nil {
		return nil, fmt.Errorf("set status as archived: %w", err)
	}

	err = pg.Publish(ctx, tx, NotifyArchived, ArchivedEvent{
		Type:    ArchiveEventTypeStatus,
		UUID:    ds.UUID,
		Name:    ds.Name,
		Version: ds.ID,
	})
	if err != nil {
		return nil, fmt.Errorf("send archived notification: %w", err)
	}

	return ref, nil
}

func (a *Archiver) ensureSigningKeys(ctx context.Context) (outErr error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	err = q.AcquireTXLock(ctx, LockSigningKeys)
	if err != nil {
		return fmt.Errorf("failed to acquire signing keys lock: %w", err)
	}

	var set SigningKeySet

	const (
		days              = 24 * time.Hour
		validFor          = 180 * days
		headsUpPeriod     = 2 * days
		generateNewMargin = 7 * days
	)

	keys, err := q.GetSigningKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch signing keys: %w", err)
	}

	for i := range keys {
		var spec SigningKey

		err := json.Unmarshal(keys[i].Spec, &spec)
		if err != nil {
			return fmt.Errorf("failed to unmarshal key %q: %w",
				keys[i].Kid, err)
		}

		if spec.NotAfter.IsZero() {
			spec.NotAfter = spec.NotBefore.Add(validFor)
		}

		set.Keys = append(set.Keys, spec)
	}

	var keyID int64

	latestKey := set.LatestKey()
	if latestKey != nil {
		cid, err := strconv.ParseInt(latestKey.Spec.KeyID, 36, 64)
		if err != nil {
			return fmt.Errorf("invalid key ID in keyset: %w", err)
		}

		keyID = cid
	}

	if latestKey == nil || time.Until(latestKey.NotAfter) < generateNewMargin {
		key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		if err != nil {
			return fmt.Errorf("failed to generate key: %w", err)
		}

		notBefore := time.Now()

		// Only add a heads up period if we have keys.
		if latestKey != nil {
			notBefore = notBefore.Add(headsUpPeriod)
		}

		signingKey := SigningKey{
			Spec: jwk.NewSpecWithID(
				strconv.FormatInt(keyID+1, 36), key,
			),
			IssuedAt:  time.Now(),
			NotBefore: notBefore,
			NotAfter:  notBefore.Add(validFor),
		}

		specData, err := json.Marshal(&signingKey)
		if err != nil {
			return fmt.Errorf("failed to marshal key: %w", err)
		}

		err = q.InsertSigningKey(ctx, postgres.InsertSigningKeyParams{
			Kid:  signingKey.Spec.KeyID,
			Spec: specData,
		})
		if err != nil {
			return fmt.Errorf("failed to store signing key: %w", err)
		}

		set.Keys = append(set.Keys, signingKey)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit signing keys: %w", err)
	}

	a.signingKeysChecked = time.Now()
	a.signingKeys.Replace(set.Keys)

	return nil
}
