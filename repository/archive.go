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
	"github.com/ttab/elephant/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"golang.org/x/exp/slog"
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
		customResolver := aws.EndpointResolverWithOptionsFunc(func(
			service, region string, options ...interface{},
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

		options = append(options,
			config.WithEndpointResolverWithOptions(customResolver))

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

	versionsArchived *prometheus.CounterVec
	statusesArchived *prometheus.CounterVec
	deletesProcessed *prometheus.CounterVec
	restarts         prometheus.Counter

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

	restarts := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "elephant_archiver_restarts_total",
			Help: "Number of times the archiver has restarted.",
		},
	)
	if err := opts.MetricsRegisterer.Register(restarts); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	return &Archiver{
		logger:           opts.Logger,
		s3:               opts.S3,
		pool:             opts.DB,
		bucket:           opts.Bucket,
		versionsArchived: versionsArchived,
		statusesArchived: statusesArchived,
		deletesProcessed: deletesProcessed,
		restarts:         restarts,
	}, nil
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

			a.logger.ErrorCtx(ctx, "archiver error, restarting",
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
		name string, now time.Time, d time.Duration,
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
			"archive document versions", now, 500*time.Millisecond,
			a.versionsArchived, a.archiveDocumentVersions)
		if err != nil {
			return err
		}

		err = runWithDelay(
			"archive document statuses", now, 500*time.Millisecond,
			a.statusesArchived, a.archiveDocumentStatuses)
		if err != nil {
			return err
		}

		err = runWithDelay(
			"process document deletes", now, 500*time.Millisecond,
			a.deletesProcessed, a.processDeletes)
		if err != nil {
			return err
		}

		// TODO: Archive ACLs

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

func (a *Archiver) processDeletes(
	ctx context.Context,
) (bool, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer pg.SafeRollback(ctx, a.logger, tx,
		"document version archiving")

	q := postgres.New(tx)

	deleteOrder, err := q.GetDocumentForDeletion(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to get pending deletions: %w", err)
	}

	prefix := fmt.Sprintf("documents/%s/", deleteOrder.UUID)
	dstPrefix := fmt.Sprintf("deleted/%s/%d/",
		deleteOrder.UUID, deleteOrder.DeleteRecordID)

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

	count, err := q.FinaliseDelete(ctx, deleteOrder.UUID)
	if err != nil {
		return false, fmt.Errorf("failed to finalise delete: %w", err)
	}

	if count != 1 {
		return false, errors.New(
			"no match for document delete, database might be inconsisten")
	}

	err = tx.Commit(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
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

type ArchivedDocumentVersion struct {
	UUID            uuid.UUID       `json:"uuid"`
	Version         int64           `json:"version"`
	Created         time.Time       `json:"created"`
	CreatorURI      string          `json:"creator_uri"`
	Meta            json.RawMessage `json:"meta,omitempty"`
	DocumentData    json.RawMessage `json:"document_data"`
	ParentSignature string          `json:"parent_signature,omitempty"`
}

func (a *Archiver) archiveDocumentVersions(
	ctx context.Context,
) (bool, error) {
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
		Version:         unarchived.Version,
		Created:         unarchived.Created.Time,
		CreatorURI:      unarchived.CreatorUri,
		Meta:            unarchived.Meta,
		DocumentData:    unarchived.DocumentData,
		ParentSignature: unarchived.ParentSignature.String,
	}

	objectBody, err := json.Marshal(&dv)
	if err != nil {
		return false, fmt.Errorf(
			"failed to marshal archive data to JSON: %w", err)
	}

	checksum := sha256.Sum256(objectBody)

	signingKey := a.signingKeys.CurrentKey(time.Now())
	if signingKey == nil {
		return false, fmt.Errorf(
			"no signing keys have been configured: %w", err)
	}

	signature, err := NewArchiveSignature(signingKey, checksum)
	if err != nil {
		return false, fmt.Errorf("failed to sign archive data: %w", err)
	}

	key := fmt.Sprintf("documents/%s/versions/%d.json", dv.UUID, dv.Version)

	err = q.SetDocumentVersionAsArchived(ctx, postgres.SetDocumentVersionAsArchivedParams{
		UUID:      dv.UUID,
		Version:   dv.Version,
		Signature: signature.String(),
	})
	if err != nil {
		return false, fmt.Errorf("failed to update archived status in db: %w",
			err)
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
		ContentLength: int64(len(objectBody)),
		Metadata: map[string]string{
			"elephant-signature": signature.String(),
		},
	})
	if err != nil {
		return false, fmt.Errorf(
			"failed to store archive object: %w", err)
	}

	notifyArchived(ctx, a.logger, q, ArchivedEvent{
		Type:    ArchiveEventTypeVersion,
		UUID:    dv.UUID,
		Version: dv.Version,
	})

	err = tx.Commit(ctx)
	if err != nil {
		_, cErr := a.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket:    aws.String(a.bucket),
			Key:       aws.String(key),
			VersionId: putRes.VersionId,
		})
		if cErr != nil {
			a.logger.ErrorCtx(ctx,
				"failed to clean up archive object after commit failed",
				elephantine.LogKeyError, cErr,
				elephantine.LogKeyBucket, a.bucket,
				elephantine.LogKeyObjectKey, key)
		}

		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return true, nil
}

type ArchivedDocumentStatus struct {
	UUID             uuid.UUID       `json:"uuid"`
	Name             string          `json:"name"`
	ID               int64           `json:"id"`
	Version          int64           `json:"version"`
	Created          time.Time       `json:"created"`
	CreatorURI       string          `json:"creator_uri"`
	Meta             json.RawMessage `json:"meta,omitempty"`
	ParentSignature  string          `json:"parent_signature,omitempty"`
	VersionSignature string          `json:"version_signature"`
}

func (a *Archiver) archiveDocumentStatuses(
	ctx context.Context,
) (bool, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer pg.SafeRollback(ctx, a.logger, tx,
		"document version archiving")

	q := postgres.New(tx)

	// TODO: Update query so that it takes into account that version can be
	// -1. Potentially update the schema & code so that version is nullable
	// instead.
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
		Version:          unarchived.Version,
		Created:          unarchived.Created.Time,
		CreatorURI:       unarchived.CreatorUri,
		Meta:             unarchived.Meta,
		ParentSignature:  unarchived.ParentSignature.String,
		VersionSignature: unarchived.VersionSignature.String,
	}

	objectBody, err := json.Marshal(&ds)
	if err != nil {
		return false, fmt.Errorf(
			"failed to marshal archive data to JSON: %w", err)
	}

	checksum := sha256.Sum256(objectBody)

	signingKey := a.signingKeys.CurrentKey(time.Now())
	if signingKey == nil {
		return false, fmt.Errorf(
			"no signing keys have been configured: %w", err)
	}

	signature, err := NewArchiveSignature(signingKey, checksum)
	if err != nil {
		return false, fmt.Errorf("failed to sign archive data: %w", err)
	}

	key := fmt.Sprintf("documents/%s/statuses/%s/%d.json",
		ds.UUID, ds.Name, ds.ID)

	err = q.SetDocumentStatusAsArchived(ctx, postgres.SetDocumentStatusAsArchivedParams{
		UUID:      ds.UUID,
		ID:        ds.ID,
		Signature: signature.String(),
	})
	if err != nil {
		return false, fmt.Errorf("failed to update archived status in db: %w",
			err)
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
		ContentLength: int64(len(objectBody)),
		Metadata: map[string]string{
			"elephant-signature": signature.String(),
		},
	})
	if err != nil {
		return false, fmt.Errorf(
			"failed to store archive object: %w", err)
	}

	notifyArchived(ctx, a.logger, q, ArchivedEvent{
		Type:    ArchiveEventTypeStatus,
		UUID:    ds.UUID,
		Name:    ds.Name,
		Version: ds.ID,
	})

	err = tx.Commit(ctx)
	if err != nil {
		_, cErr := a.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket:    aws.String(a.bucket),
			Key:       aws.String(key),
			VersionId: putRes.VersionId,
		})
		if cErr != nil {
			a.logger.ErrorCtx(ctx,
				"failed to clean up archive object after commit failed",
				elephantine.LogKeyError, cErr,
				elephantine.LogKeyBucket, a.bucket,
				elephantine.LogKeyObjectKey, key)
		}

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

	keys, err := q.GetSigningKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch signing keys: %w", err)
	}

	var set SigningKeySet

	for i := range keys {
		var spec SigningKey

		err := json.Unmarshal(keys[i].Spec, &spec)
		if err != nil {
			return fmt.Errorf("failed to unmarshal key %q: %w",
				keys[i].Kid, err)
		}

		set.Keys = append(set.Keys, spec)
	}

	validFor := 180 * 24 * time.Hour
	headsUpPeriod := 30 * 24 * time.Hour
	generateNewAfter := validFor - headsUpPeriod
	notBefore := time.Now()

	var keyID int64

	currentKey := set.LatestKey()
	if currentKey != nil {
		cid, err := strconv.ParseInt(currentKey.Spec.KeyID, 36, 64)
		if err != nil {
			return fmt.Errorf("invalid key ID in keyset: %w", err)
		}

		keyID = cid

		notBefore = notBefore.Add(headsUpPeriod)
	}

	if currentKey == nil || time.Since(currentKey.NotBefore) > generateNewAfter {
		key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		if err != nil {
			return fmt.Errorf("failed to generate key: %w", err)
		}

		signingKey := SigningKey{
			Spec: jwk.NewSpecWithID(
				strconv.FormatInt(keyID+1, 36), key,
			),
			IssuedAt:  time.Now(),
			NotBefore: notBefore,
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
