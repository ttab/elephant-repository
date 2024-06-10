package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/newsdoc"
	"golang.org/x/sync/errgroup"
)

type RestorerOptions struct {
	Logger            *slog.Logger
	S3                *s3.Client
	Bucket            string
	DB                *pgxpool.Pool
	MetricsRegisterer prometheus.Registerer
}

type Restorer struct {
	logger             *slog.Logger
	s3                 *s3.Client
	bucket             string
	pool               *pgxpool.Pool
	signingKeys        SigningKeySet
	signingKeysChecked time.Time

	versionsRestored  *prometheus.CounterVec
	statusesRestored  *prometheus.CounterVec
	restoresProcessed *prometheus.CounterVec

	cancel  func()
	stopped chan struct{}
}

type RestoreSpec struct {
	ACL []ACLEntry
}

func NewRestorer(opts RestorerOptions) (*Restorer, error) {
	if opts.MetricsRegisterer == nil {
		opts.MetricsRegisterer = prometheus.DefaultRegisterer
	}

	versionsRestored := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_restored_document_versions_total",
			Help: "Number of document versions restored.",
		},
		[]string{"status"},
	)
	if err := opts.MetricsRegisterer.Register(versionsRestored); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	statusesRestored := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_restored_statuses_total",
			Help: "Number of document statuses restored.",
		},
		[]string{"status"},
	)
	if err := opts.MetricsRegisterer.Register(statusesRestored); err != nil {
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

	return &Restorer{
		logger:            opts.Logger,
		s3:                opts.S3,
		pool:              opts.DB,
		bucket:            opts.Bucket,
		versionsRestored:  versionsRestored,
		statusesRestored:  statusesRestored,
		restoresProcessed: restoresProcessed,
	}, nil
}

func (r *Restorer) Run(ctx context.Context) error {
	var failStart time.Time

	for {
		wait := 500 * time.Millisecond
		wait += time.Duration(rand.Intn(500)) * time.Millisecond

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}

		err := r.iteration(ctx)

		// Allow the restorer to fail for a minute before exiting with
		// an error.
		switch {
		case err != nil && failStart.IsZero():
			failStart = time.Now()
		case err != nil && time.Since(failStart) > 1*time.Minute:
			return err
		case err == nil:
			failStart = time.Time{}
		}
	}
}

func (r *Restorer) iteration(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer pg.SafeRollback(ctx, r.logger, tx,
		"document restore")

	q := postgres.New(tx)

	req, err := q.GetNextRestoreRequest(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	} else if err != nil {
		return fmt.Errorf("get next restore request: %w", err)
	}

	var spec RestoreSpec

	err = json.Unmarshal(req.Spec, &spec)
	if err != nil {
		return fmt.Errorf("invalid restore spec: %w", err)
	}

	record, err := q.GetDeleteRecord(ctx, postgres.GetDeleteRecordParams{
		ID:   req.DeleteRecordID,
		UUID: req.UUID,
	})
	if err != nil {
		return fmt.Errorf("get delete record: %w", err)
	}

	var heads map[string]int64

	err = json.Unmarshal(record.Heads, &heads)
	if err != nil {
		return fmt.Errorf("unmarshal document heads: %w", err)
	}

	info, err := q.GetDocumentForUpdate(ctx,
		postgres.GetDocumentForUpdateParams{
			UUID: req.UUID,
			Now:  pg.Time(time.Now()),
		})
	if err != nil {
		return fmt.Errorf("failed to lock document for restore: %w", err)
	}

	if info.SystemState.String != SystemStateRestoring {
		return errors.New("document is not in the required restore state")
	}

	// This shouldn't happen, but adding it as a sanity check.
	if info.CurrentVersion != 0 {
		return errors.New("document has current versions and cannot be restored")
	}

	versions, err := r.listVersionObjects(ctx, req.UUID, req.DeleteRecordID)
	if err != nil {
		return fmt.Errorf("list version objects: %w", err)
	}

	if int64(len(versions)) != record.Version {
		return fmt.Errorf("expected %d versions in archive, got %d",
			record.Version, len(versions))
	}

	statuses, err := r.listStatusObjects(ctx, req.UUID, req.DeleteRecordID)
	if err != nil {
		return fmt.Errorf("list status objects: %w", err)
	}

	// Check for unexpected statuses in archive.
	for status := range statuses {
		_, expected := heads[status]
		if !expected {
			return fmt.Errorf("found unexpected status %q in archive", status)
		}
	}

	// Check that all statuses are accounted for in the archive.
	for status, count := range heads {
		objects, present := statuses[status]

		switch {
		case !present:
			return fmt.Errorf("missing status objects in archive for %q", status)
		case int64(len(objects)) != count:
			return fmt.Errorf("expected %d archived %q statuses, got %d",
				count, status, len(objects))
		}
	}

	const loadConcurrency = 3

	loadVersionChan := make(chan *objectRef[ArchivedDocumentVersion], loadConcurrency)
	loadStatusChan := make(chan *objectRef[ArchivedDocumentStatus], loadConcurrency)

	grp, gCtx := errgroup.WithContext(ctx)

	grp.Go(func() error {
		defer close(loadVersionChan)

		for i := range versions {
			loadVersionChan <- &versions[i]
		}

		return nil
	})

	grp.Go(func() error {
		defer close(loadStatusChan)

		for _, items := range statuses {
			for i := range items {
				loadStatusChan <- &items[i]
			}
		}

		return nil
	})

	for i := 0; i < loadConcurrency; i++ {
		grp.Go(func() error {
			for ref := range loadVersionChan {
				err := loadObject(
					gCtx, r.logger, &r.signingKeys,
					r.s3, r.bucket, ref,
				)
				if err != nil {
					return err
				}
			}

			return nil
		})

		grp.Go(func() error {
			for ref := range loadStatusChan {
				err := loadObject(
					gCtx, r.logger, &r.signingKeys,
					r.s3, r.bucket, ref,
				)
				if err != nil {
					return err
				}
			}

			return nil
		})
	}

	var previousV *objectRef[ArchivedDocumentVersion]

	// Restore all document versions and verify signature chain while doing
	// it.
	for i, v := range versions {
		err := r.restoreVersion(
			ctx, tx, q, grp,
			v, previousV, req.Creator,
		)
		if err != nil {
			return fmt.Errorf("restore version %w", err)
		}

		previousV = &v

		if i == 0 {
			err := updateACL(
				ctx, q, req.Creator, req.UUID,
				record.Type, record.Language.String, spec.ACL,
			)
			if err != nil {
				return fmt.Errorf("set ACL: %w", err)
			}
		}
	}

	// Once the document versions have been restored we can restore each
	// status concurrently.
	for name, items := range statuses {
		grp.Go(func() error {
			// In previous versions of Go (before 1.22) directly
			// using loop variables in a goroutine was a no-go as
			// the scope captured a reference to to a loop variable
			// that was shared between iterations. They are now
			// scoped per iteration instead, so it's completely safe
			// to do.
			err := r.restoreStatus(ctx, name, items)
			if err != nil {
				return fmt.Errorf("restore %q statuses: %w",
					name, err)
			}

			return nil
		})
	}

	return nil
}

func (r *Restorer) restoreStatus(
	ctx context.Context,
	name string,
	items []objectRef[ArchivedDocumentStatus],
) error {
	return nil
}

func (r *Restorer) restoreVersion(
	ctx context.Context,
	tx pgx.Tx,
	q *postgres.Queries,
	grp *errgroup.Group,
	v objectRef[ArchivedDocumentVersion],
	previousV *objectRef[ArchivedDocumentVersion],
	restorerSub string,
) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf(
			"document loading aborted: %w", grp.Wait())
	case <-v.Loaded:
	}

	if v.Data.Version != v.ID {
		return fmt.Errorf(
			"declares the wrong version number")
	}

	// A document version always contains its parents signature so
	// that the versions of a documents become a verifiable chain.
	if previousV != nil && v.Data.ParentSignature != previousV.Signature {
		return fmt.Errorf("signature chain broken")
	}

	var docAttr struct {
		Type     string `json:"type"`
		URI      string `json:"uri"`
		Language string `json:"language"`
	}

	// Read basic attributes off the document data.
	err := json.Unmarshal(v.Data.DocumentData, &docAttr)
	if err != nil {
		return fmt.Errorf(
			"invalid document data: %w", err)
	}

	updatedMetaData, err := annotateMeta(v.Data.Meta, newsdoc.DataMap{
		"restored_at": time.Now().Format(time.RFC3339),
		"restored_by": restorerSub,
	})
	if err != nil {
		return fmt.Errorf(
			"annotate metadata: %w", err)
	}

	props := documentVersionProps{
		UUID:         v.Data.UUID,
		Version:      v.Data.Version,
		Type:         docAttr.Type,
		URI:          docAttr.URI,
		Language:     docAttr.Language,
		Created:      v.Data.Created,
		Creator:      v.Data.CreatorURI,
		MainDocument: v.Data.MainDocument,
		MetaJSON:     updatedMetaData,
		DocJSON:      v.Data.DocumentData,
		Document:     nil,
	}

	err = createNewDocumentVersion(ctx, tx, q, props)
	if err != nil {
		return fmt.Errorf("write to database: %w", err)
	}

	return nil
}

func annotateMeta(originalMeta []byte, add newsdoc.DataMap) ([]byte, error) {
	var meta newsdoc.DataMap

	if originalMeta != nil {
		err := json.Unmarshal(originalMeta, &meta)
		if err != nil {
			return nil, fmt.Errorf(
				"invalid orginal metadata: %w", err)
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

func loadObject[T any](
	ctx context.Context, logger *slog.Logger, keys *SigningKeySet,
	s3Svc *s3.Client, bucket string, ref *objectRef[T],
) error {
	res, err := s3Svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(ref.Key),
	})
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}

	defer elephantine.SafeClose(logger, ref.Key, res.Body)

	sigMeta, hasSignature := res.Metadata["elephant-signature"]
	if !hasSignature {
		return errors.New("missing signature metadata")
	}

	signature, err := ParseArchiveSignature(sigMeta)
	if err != nil {
		return fmt.Errorf("malformed signature: %w", err)
	}

	key := keys.GetKeyByID(signature.KeyID)
	if key == nil {
		return errors.New("unknown signature key")
	}

	err = signature.Verify(key)
	if err != nil {
		return fmt.Errorf("verify signature: %w", err)
	}

	var buf bytes.Buffer

	// We will be reading the entire body as we want to verify the
	// signature.
	if res.ContentLength != nil {
		buf.Grow(int(*res.ContentLength))
	}

	hasher := sha256.New()

	// Decode data and hash it in one go.
	reader := io.TeeReader(res.Body, hasher)

	dec := json.NewDecoder(reader)

	err = dec.Decode(&ref.Data)
	if err != nil {
		return fmt.Errorf("unmarshal object: %w", err)
	}

	digest := hasher.Sum(nil)

	// This means that the content has been manipulated.
	if !bytes.Equal(digest, signature.Hash[:]) {
		return errors.New("signature mismatch")
	}

	close(ref.Loaded)

	return nil
}

func (r *Restorer) listVersionObjects(
	ctx context.Context, docID uuid.UUID, recordID int64,
) ([]objectRef[ArchivedDocumentVersion], error) {
	prefix := fmt.Sprintf(
		"deleted/%s/%d/versions/",
		docID, recordID,
	)
	format := prefix + "%d.json"

	versionsPg := s3.NewListObjectsV2Paginator(r.s3,
		&s3.ListObjectsV2Input{
			Bucket: aws.String(r.bucket),
			Prefix: aws.String(prefix),
		})

	var versions []objectRef[ArchivedDocumentVersion]

	for versionsPg.HasMorePages() {
		page, err := versionsPg.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list versions in S3: %w", err)
		}

		for _, obj := range page.Contents {
			vRef := objectRef[ArchivedDocumentVersion]{
				Key: *obj.Key,
			}

			_, err := fmt.Sscanf(vRef.Key, format, &vRef.ID)
			if err != nil {
				return nil, fmt.Errorf(
					"parse key structure: %w", err)
			}

			versions = append(versions, vRef)
		}
	}

	// Ensure ascending order.
	slices.SortFunc(versions, func(a, b objectRef[ArchivedDocumentVersion]) int {
		return int(a.ID - b.ID)
	})

	// Verify that we have a continuous range of versions and create the
	// loaded signal channel.
	for i, ref := range versions {
		if ref.ID != int64(i+1) {
			return nil, fmt.Errorf(
				"gap in archived document versions, got %d when %d was expected",
				ref.ID, i+1)
		}

		versions[i].Loaded = make(chan struct{})

	}

	return versions, nil
}

func (r *Restorer) listStatusObjects(
	ctx context.Context, docID uuid.UUID, recordID int64,
) (map[string][]objectRef[ArchivedDocumentStatus], error) {
	prefix := fmt.Sprintf(
		"deleted/%s/%d/statuses/",
		docID, recordID,
	)

	versionsPg := s3.NewListObjectsV2Paginator(r.s3,
		&s3.ListObjectsV2Input{
			Bucket: aws.String(r.bucket),
			Prefix: aws.String(prefix),
		})

	var statuses map[string][]objectRef[ArchivedDocumentStatus]

	for versionsPg.HasMorePages() {
		page, err := versionsPg.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list versions in S3: %w", err)
		}

		for _, obj := range page.Contents {
			vRef := objectRef[ArchivedDocumentStatus]{
				Key: *obj.Key,
			}

			status, name, ok := strings.Cut(
				strings.TrimPrefix(*obj.Key, prefix), "/",
			)
			if !ok {
				return nil, fmt.Errorf(
					"unexpected key structure: %q", *obj.Key)
			}

			versions := statuses[status]

			_, err := fmt.Sscanf(name, "%d.json", &vRef.ID)
			if err != nil {
				return nil, fmt.Errorf(
					"parse key structure: %w", err)
			}

			versions = append(versions, vRef)

			statuses[status] = versions
		}
	}

	for _, versions := range statuses {
		// Ensure ascending order.
		slices.SortFunc(versions, func(a, b objectRef[ArchivedDocumentStatus]) int {
			return int(a.ID - b.ID)
		})
	}

	for _, versions := range statuses {
		// Verify that we have a continuous range of versions and create the
		// loaded signal channel.
		for i, ref := range versions {
			if ref.ID != int64(i+1) {
				return nil, fmt.Errorf(
					"gap in archived document versions, got %d when %d was expected",
					ref.ID, i+1)
			}

			versions[i].Loaded = make(chan struct{})
		}
	}

	return statuses, nil
}

type objectRef[T any] struct {
	Key       string
	ID        int64
	Loaded    chan struct{}
	Data      T
	Signature string
}
