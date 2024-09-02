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
	mrand "math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
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

type S3Options struct {
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
	DisableHTTPS    bool
	HTTPClient      *http.Client
}

func S3Client(
	ctx context.Context, opts S3Options,
) (*s3.Client, error) {
	var (
		options   []func(*config.LoadOptions) error
		s3Options []func(*s3.Options)
	)

	if opts.Endpoint != "" {
		//nolint: staticcheck
		customResolver := aws.EndpointResolverWithOptionsFunc(func(
			service, region string, _ ...interface{},
		) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           opts.Endpoint,
					SigningRegion: region,
				}, nil
			}

			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})

		//nolint: staticcheck
		options = append(options,
			config.WithEndpointResolverWithOptions(customResolver),
			config.WithRegion("auto"),
		)

		s3Options = append(s3Options, func(o *s3.Options) {
			o.EndpointOptions.DisableHTTPS = opts.DisableHTTPS
			o.UsePathStyle = true
		})
	}

	if opts.AccessKeyID != "" {
		creds := credentials.NewStaticCredentialsProvider(
			opts.AccessKeyID, opts.AccessKeySecret, "")

		options = append(options,
			config.WithCredentialsProvider(creds))
	}

	if opts.HTTPClient != nil {
		options = append(options, config.WithHTTPClient(opts.HTTPClient))
	}

	cfg, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS SDK config: %w", err)
	}

	client := s3.NewFromConfig(cfg, s3Options...)

	return client, nil
}

type RestoreSpec struct {
	ACL []ACLEntry
}

type ArchiverOptions struct {
	Logger            *slog.Logger
	S3                *s3.Client
	Bucket            string
	DB                *pgxpool.Pool
	MetricsRegisterer prometheus.Registerer
}

// Archiver reads unarchived document versions, and statuses and writes a copy
// to S3. It does this using SELECT ... FOR UPDATE SKIP LOCKED.
type Archiver struct {
	logger             *slog.Logger
	s3                 *s3.Client
	bucket             string
	pool               *pgxpool.Pool
	signingKeys        SigningKeySet
	signingKeysChecked time.Time
	reader             *ArchiveReader

	versionsArchived  *prometheus.CounterVec
	statusesArchived  *prometheus.CounterVec
	deletesProcessed  *prometheus.CounterVec
	restoresProcessed *prometheus.CounterVec
	purgesProcessed   *prometheus.CounterVec
	restarts          prometheus.Counter

	cancel  func()
	stopped chan struct{}
}

func NewArchiver(opts ArchiverOptions) (*Archiver, error) {
	if opts.MetricsRegisterer == nil {
		opts.MetricsRegisterer = prometheus.DefaultRegisterer
	}

	versionsArchived := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_archiver_documents_total",
			Help: "Number of document versions archived.",
		},
		[]string{"status"},
	)
	if err := opts.MetricsRegisterer.Register(versionsArchived); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	statusesArchived := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_archiver_statuses_total",
			Help: "Number of document statuses archived.",
		},
		[]string{"status"},
	)
	if err := opts.MetricsRegisterer.Register(statusesArchived); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	deletesProcessed := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_archiver_deletes_total",
			Help: "Number of document deletes processed.",
		},
		[]string{"status"},
	)
	if err := opts.MetricsRegisterer.Register(deletesProcessed); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	restoresProcessed := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_archiver_restores_total",
			Help: "Number of document restores processed.",
		},
		[]string{"status"},
	)
	if err := opts.MetricsRegisterer.Register(restoresProcessed); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	purgesProcessed := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_archiver_purges_total",
			Help: "Number of document purges processed.",
		},
		[]string{"status"},
	)
	if err := opts.MetricsRegisterer.Register(purgesProcessed); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	restarts := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "elephant_archiver_restarts_total",
			Help: "Number of times the archiver has restarted.",
		},
	)
	if err := opts.MetricsRegisterer.Register(restarts); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	a := Archiver{
		logger:            opts.Logger,
		s3:                opts.S3,
		pool:              opts.DB,
		bucket:            opts.Bucket,
		versionsArchived:  versionsArchived,
		statusesArchived:  statusesArchived,
		deletesProcessed:  deletesProcessed,
		restoresProcessed: restoresProcessed,
		purgesProcessed:   purgesProcessed,
		restarts:          restarts,
	}

	a.reader = NewArchiveReader(ArchiveReaderOptions{
		S3:          opts.S3,
		Bucket:      opts.Bucket,
		SigningKeys: &a.signingKeys,
	})

	return &a, nil
}

func (a *Archiver) Run(ctx context.Context) error {
	err := a.ensureSigningKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to ensure signing keys: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)

	a.cancel = cancel
	a.stopped = make(chan struct{})

	go a.run(ctx)

	return nil
}

func (a *Archiver) Stop() {
	a.cancel()
	<-a.stopped
}

func (a *Archiver) run(ctx context.Context) {
	const restartWaitSeconds = 10

	defer close(a.stopped)

	for {
		err := a.loop(ctx)
		if errors.Is(err, context.Canceled) {
			return
		} else if err != nil {
			a.restarts.Inc()

			a.logger.ErrorContext(ctx, "archiver error, restarting",
				elephantine.LogKeyError, err,
				elephantine.LogKeyDelay, slog.DurationValue(restartWaitSeconds),
			)
		}

		select {
		case <-time.After(restartWaitSeconds * time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func (a *Archiver) loop(ctx context.Context) error {
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
			"archive document versions", now,
			a.versionsArchived, a.archiveDocumentVersions)
		if err != nil {
			return err
		}

		err = runWithDelay(
			"archive document statuses", now,
			a.statusesArchived, a.archiveDocumentStatuses)
		if err != nil {
			return err
		}

		err = runWithDelay(
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
	LastVersion int64            `json:"last_version"`
	Heads       map[string]int64 `json:"heads"`
	ACL         []ACLEntry       `json:"acl"`
	Archived    time.Time        `json:"archived"`
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

	defer pg.SafeRollback(ctx, a.logger, tx,
		"document delete processing")

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

	for i := 0; i < workerCount; i++ {
		group.Go(func() error {
			for key := range moves {
				baseKey := strings.TrimPrefix(key, prefix)
				dstKey := dstPrefix + baseKey

				err := a.moveDeletedObject(gCtx, key, dstKey)
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

	err = group.Wait()
	if err != nil {
		return false, fmt.Errorf("failed to move all document objects: %w", err)
	}

	manifest := DeleteManifest{
		Archived:    time.Now(),
		LastVersion: deleteOrder.Version,
	}

	err = json.Unmarshal(deleteOrder.Acl, &manifest.ACL)
	if err != nil {
		return false, fmt.Errorf("unmarshal delete order ACL: %w", err)
	}

	err = json.Unmarshal(deleteOrder.Heads, &manifest.Heads)
	if err != nil {
		return false, fmt.Errorf("unmarshal delete order heads: %w", err)
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
		return false, fmt.Errorf("failed to delete document row: %w", err)
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
		return false, fmt.Errorf("failed to set delete record as finished: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
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
) (bool, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin transaction: %w", err)
	}

	defer pg.SafeRollback(ctx, a.logger, tx,
		"document restores")

	q := postgres.New(tx)

	// Drop restore requests whose delete record has been purged.
	err = q.DropInvalidRestoreRequests(ctx)
	if err != nil {
		return false, fmt.Errorf("")
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

	requestPrefix := fmt.Sprintf("deleted/%s/%019d/",
		req.UUID, req.DeleteRecordID)
	manifestKey := requestPrefix + "manifest.json"
	versionsPrefix := requestPrefix + "versions/"
	statusesPrefix := requestPrefix + "statuses/"

	manifest, _, err := a.reader.ReadDeleteManifest(ctx, manifestKey)
	if err != nil {
		return false, fmt.Errorf("failed to read manifest: %w", err)
	}

	// Default to using the previous ACL.
	if len(spec.ACL) == 0 {
		spec.ACL = manifest.ACL
	}

	versionsPages := s3.NewListObjectsV2Paginator(a.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(a.bucket),
		Prefix: aws.String(versionsPrefix),
	})

	var (
		parentSig string
		lastID    int64
	)

	for versionsPages.HasMorePages() {
		page, err := versionsPages.NextPage(ctx)
		if err != nil {
			return false, fmt.Errorf(
				"get s3 listing of document versions: %w", err)
		}

		for _, o := range page.Contents {
			if !strings.HasSuffix(*o.Key, ".json") {
				continue
			}

			// Trim prefix and suffix so that we're left with the
			// version number.
			vStr := strings.TrimSuffix(
				strings.TrimPrefix(*o.Key, versionsPrefix),
				".json")

			id, err := strconv.ParseInt(vStr, 10, 64)
			if err != nil {
				return false, fmt.Errorf(
					"invalid version object name: %w", err)
			}

			if id != lastID+1 {
				return false, fmt.Errorf(
					"expected version %d, got %d",
					lastID+1, id)
			}

			sig, err := a.restoreDocumentVersion(
				ctx, q, &parentSig, id, req, *o.Key)
			if err != nil {
				return false, fmt.Errorf(
					"restore version %d: %w", id, err)
			}

			lastID = id
			parentSig = sig
		}
	}

	if lastID != manifest.LastVersion {
		return false, fmt.Errorf("expected %d versions, got %d",
			manifest.LastVersion, lastID)
	}

	statusesPages := s3.NewListObjectsV2Paginator(a.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(a.bucket),
		Prefix: aws.String(statusesPrefix),
	})

	var lastStatus string

	observedHeads := make(map[string]int64)

	for statusesPages.HasMorePages() {
		page, err := statusesPages.NextPage(ctx)
		if err != nil {
			return false, fmt.Errorf(
				"get s3 listing of document statuses: %w", err)
		}

		for _, o := range page.Contents {
			if !strings.HasSuffix(*o.Key, ".json") {
				continue
			}

			// Trim prefix and suffix so that we're left with the
			// status name and ID.
			identStr := strings.TrimSuffix(
				strings.TrimPrefix(*o.Key, statusesPrefix),
				".json")

			name, idStr, ok := strings.Cut(identStr, "/")
			if !ok {
				// We only want objects in subfolders.
				continue
			}

			_, expected := manifest.Heads[name]
			if !expected {
				return false, fmt.Errorf(
					"the status %q was not included in the delete manifest",
					name)
			}

			// Reset the chain if we're on a new status.
			if name != lastStatus {
				lastID = 0
				parentSig = ""
				lastStatus = name
			}

			id, err := strconv.ParseInt(idStr, 10, 64)
			if err != nil {
				return false, fmt.Errorf(
					"invalid status ID in object key: %w", err)
			}

			if id != lastID+1 {
				return false, fmt.Errorf(
					"expected version %d, got %d",
					lastID+1, id)
			}

			sig, err := a.restoreDocumentStatus(
				ctx, q, &parentSig, id, *o.Key)
			if err != nil {
				return false, fmt.Errorf(
					"restore status %q %d: %w", name, id, err)
			}

			lastID = id
			parentSig = sig
			observedHeads[name] = id
		}
	}

	// Verify the restored statuses against the manifest.
	for name, head := range manifest.Heads {
		if observedHeads[name] == head {
			continue
		}

		return false, fmt.Errorf("expected %d %q statuses, got %d",
			head, name, observedHeads[name])
	}

	var acls []postgres.ACLUpdateParams

	for _, acl := range spec.ACL {
		acls = append(acls, postgres.ACLUpdateParams{
			UUID:        req.UUID,
			URI:         acl.URI,
			Permissions: acl.Permissions,
		})
	}

	docInfo, err := q.GetDocumentRow(ctx, req.UUID)
	if err != nil {
		return false, fmt.Errorf(
			"read document information after restore: %w", err)
	}

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

	err = tx.Commit(ctx)
	if err != nil {
		return false, fmt.Errorf("commit restore: %w", err)
	}

	return false, nil
}

func (a *Archiver) restoreDocumentVersion(
	ctx context.Context, q *postgres.Queries, parentSig *string,
	id int64, req postgres.GetNextRestoreRequestRow, key string,
) (string, error) {
	dv, sig, err := a.reader.ReadDocumentVersion(ctx, key, parentSig)
	if err != nil {
		return "", fmt.Errorf("read archived version: %w", err)
	}

	updatedMetaData, err := annotateMeta(dv.Meta, newsdoc.DataMap{
		"restored_at": time.Now().Format(time.RFC3339),
		"restored_by": req.Creator,
	})
	if err != nil {
		return "", fmt.Errorf(
			"annotate metadata: %w", err)
	}

	err = q.CreateDocumentVersion(ctx, postgres.CreateDocumentVersionParams{
		UUID:         dv.UUID,
		Version:      id,
		Created:      pg.Time(dv.Created),
		CreatorUri:   dv.CreatorURI,
		Meta:         updatedMetaData,
		DocumentData: dv.DocumentData,
	})
	if err != nil {
		return "", fmt.Errorf("create document version: %w", err)
	}

	err = q.UpsertDocument(ctx, postgres.UpsertDocumentParams{
		UUID:       dv.UUID,
		URI:        dv.URI,
		Type:       dv.Type,
		Created:    pg.Time(dv.Created),
		CreatorUri: dv.CreatorURI,
		Version:    id,
		MainDoc:    pg.PUUID(dv.MainDocument),
		Language:   pg.Text(dv.Language),
	})
	if err != nil {
		return "", fmt.Errorf("update document row: %w", err)
	}

	return sig, nil
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

	for k, v := range add {
		meta[k] = v
	}

	updatedMetaData, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal updated metadata: %w", err)
	}

	return updatedMetaData, nil
}

func (a *Archiver) restoreDocumentStatus(
	ctx context.Context, q *postgres.Queries, parentSig *string,
	id int64, key string,
) (string, error) {
	stat, sig, err := a.reader.ReadDocumentStatus(ctx, key, parentSig)
	if err != nil {
		return "", fmt.Errorf("read archived status: %w", err)
	}

	err = q.CreateStatusHead(ctx, postgres.CreateStatusHeadParams{
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
		return "", fmt.Errorf("failed to create status head: %w", err)
	}

	err = q.InsertDocumentStatus(ctx, postgres.InsertDocumentStatusParams{
		UUID:           stat.UUID,
		Name:           stat.Name,
		ID:             id,
		Version:        stat.Version,
		Created:        pg.Time(stat.Created),
		CreatorUri:     stat.CreatorURI,
		Meta:           stat.Meta,
		MetaDocVersion: stat.MetaDocVersion,
	})
	if err != nil {
		return "", fmt.Errorf("failed to insert document status: %w", err)
	}

	return sig, nil
}

func (a *Archiver) processPurges(
	ctx context.Context,
) (bool, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin transaction: %w", err)
	}

	defer pg.SafeRollback(ctx, a.logger, tx,
		"document purges")

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
			return false, fmt.Errorf("delete objects: %w", err)
		}

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
		ID:       req.DeleteRecordID,
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
	ctx context.Context, key string, dstKey string,
) error {
	_, err := a.s3.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(dstKey),
		CopySource: aws.String(
			a.bucket + "/" + key,
		),
	})
	if err != nil {
		return fmt.Errorf(
			"failed to copy object: %w", err)
	}

	_, err = a.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(a.bucket),
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
}

func (av *ArchivedDocumentVersion) GetArchivedTime() time.Time {
	return av.Archived
}

func (av *ArchivedDocumentVersion) GetParentSignature() string {
	return av.ParentSignature
}

func (a *Archiver) archiveDocumentVersions(
	ctx context.Context,
) (_ bool, outErr error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer pg.SafeRollback(ctx, a.logger, tx,
		"document version archiving")

	q := postgres.New(tx)

	unarchived, err := q.GetDocumentVersionForArchiving(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf(
			"failed to get unarchived version from the database: %w",
			err)
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
	}

	key := fmt.Sprintf("documents/%s/versions/%019d.json", dv.UUID, dv.Version)

	ref, err := a.storeArchiveObject(ctx, key, &dv)
	if err != nil {
		return false, err
	}

	// We try to clean up the S3 object if the operation fails.
	defer func() {
		if outErr == nil {
			return
		}

		ref.Remove(ctx)
	}()

	err = q.SetDocumentVersionAsArchived(ctx, postgres.SetDocumentVersionAsArchivedParams{
		UUID:      dv.UUID,
		Version:   dv.Version,
		Signature: ref.Signature,
	})
	if err != nil {
		return false, fmt.Errorf("failed to update archived status in db: %w",
			err)
	}

	err = notifyArchived(ctx, a.logger, q, ArchivedEvent{
		Type:    ArchiveEventTypeVersion,
		UUID:    dv.UUID,
		Version: dv.Version,
	})
	if err != nil {
		return false, fmt.Errorf("failed to send archive notification: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return true, nil
}

type ArchivedDocumentStatus struct {
	UUID             uuid.UUID       `json:"uuid"`
	Name             string          `json:"name"`
	ID               int64           `json:"id"`
	Type             string          `json:"type"`
	Version          int64           `json:"version"`
	Created          time.Time       `json:"created"`
	CreatorURI       string          `json:"creator_uri"`
	Meta             json.RawMessage `json:"meta,omitempty"`
	ParentSignature  string          `json:"parent_signature,omitempty"`
	VersionSignature string          `json:"version_signature"`
	Archived         time.Time       `json:"archived"`
	Language         string          `json:"language"`
	MetaDocVersion   int64           `json:"meta_doc_version,omitempty"`
}

func (as *ArchivedDocumentStatus) GetArchivedTime() time.Time {
	return as.Archived
}

func (as *ArchivedDocumentStatus) GetParentSignature() string {
	return as.ParentSignature
}

func (a *Archiver) archiveDocumentStatuses(
	ctx context.Context,
) (_ bool, outErr error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer pg.SafeRollback(ctx, a.logger, tx,
		"document version archiving")

	q := postgres.New(tx)

	unarchived, err := q.GetDocumentStatusForArchiving(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf(
			"failed to get unarchived version from the database: %w",
			err)
	}

	ds := ArchivedDocumentStatus{
		UUID:             unarchived.UUID,
		Name:             unarchived.Name,
		ID:               unarchived.ID,
		Type:             unarchived.Type,
		Version:          unarchived.Version,
		Created:          unarchived.Created.Time,
		CreatorURI:       unarchived.CreatorUri,
		Meta:             unarchived.Meta,
		ParentSignature:  unarchived.ParentSignature.String,
		VersionSignature: unarchived.VersionSignature.String,
		Archived:         time.Now(),
		Language:         unarchived.Language.String,
		MetaDocVersion:   unarchived.MetaDocVersion.Int64,
	}

	key := fmt.Sprintf("documents/%s/statuses/%s/%019d.json",
		ds.UUID, ds.Name, ds.ID)

	ref, err := a.storeArchiveObject(ctx, key, &ds)
	if err != nil {
		return false, err
	}

	// We try to clean up the S3 object if the operation fails.
	defer func() {
		if outErr == nil {
			return
		}

		ref.Remove(ctx)
	}()

	err = q.SetDocumentStatusAsArchived(ctx, postgres.SetDocumentStatusAsArchivedParams{
		UUID:      ds.UUID,
		Name:      ds.Name,
		ID:        ds.ID,
		Signature: ref.Signature,
	})
	if err != nil {
		return false, fmt.Errorf(
			"failed to update archived status in db: %w", err)
	}

	err = notifyArchived(ctx, a.logger, q, ArchivedEvent{
		Type:    ArchiveEventTypeStatus,
		UUID:    ds.UUID,
		Name:    ds.Name,
		Version: ds.ID,
	})
	if err != nil {
		return false, fmt.Errorf("failed to send archive notification: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return true, nil
}

func (a *Archiver) ensureSigningKeys(ctx context.Context) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer pg.SafeRollback(ctx, a.logger, tx, "signing keys")

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
