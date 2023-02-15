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
	"github.com/rakutentech/jwk-go/jwk"
	"github.com/sirupsen/logrus"
	"github.com/ttab/elephant/postgres"
	"golang.org/x/sync/errgroup"
)

type S3Options struct {
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
	DisableHTTPS    bool
}

func ArchiveS3Client(
	ctx context.Context, opts S3Options,
) (*s3.Client, error) {
	var options []func(*config.LoadOptions) error
	var s3Options []func(*s3.Options)

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

	cfg, err := config.LoadDefaultConfig(context.Background(), options...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS SDK config: %w", err)
	}

	client := s3.NewFromConfig(cfg, s3Options...)

	return client, nil
}

type ArchiverOptions struct {
	Logger *logrus.Logger
	S3     *s3.Client
	Bucket string
	DB     *pgxpool.Pool
}

// Archiver reads unarchived document versions, and statuses and writes a copy to S3. It does this using SELECT ... FOR UPDATE SKIP LOCKED.
type Archiver struct {
	logger             *logrus.Logger
	s3                 *s3.Client
	bucket             string
	pool               *pgxpool.Pool
	signingKeys        SigningKeySet
	signingKeysChecked time.Time
}

func NewArchiver(opts ArchiverOptions) *Archiver {
	return &Archiver{
		logger: opts.Logger,
		s3:     opts.S3,
		pool:   opts.DB,
		bucket: opts.Bucket,
	}
}

func (a *Archiver) Run(ctx context.Context) error {
	err := a.ensureSigningKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to ensure signing keys: %w", err)
	}

	go a.run(ctx)

	return nil
}

func (a *Archiver) run(ctx context.Context) {
	const restartWaitSeconds = 10

	for {
		err := a.loop(ctx)
		if err != nil {
			a.logger.WithContext(ctx).WithError(err).Errorf(
				"archiver error, restarting in %d seconds",
				restartWaitSeconds,
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
	for {
		wait := 1 * time.Second

		if time.Since(a.signingKeysChecked) > 24*time.Hour {
			err := a.ensureSigningKeys(ctx)
			if err != nil {
				return fmt.Errorf(
					"failed to ensure signing keys: %w", err)
			}
		}

		err := a.archiveDocumentVersions(ctx)
		if err != nil {
			return fmt.Errorf(
				"failed to archive document versions: %w", err)
		}

		err = a.archiveDocumentStatuses(ctx)
		if err != nil {
			return fmt.Errorf(
				"failed to archive document statuses: %w", err)
		}

		err = a.processDeletes(ctx)
		if err != nil {
			return fmt.Errorf(
				"failed to process document deletes: %w", err)
		}

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil

		}
	}
}

func (a *Archiver) processDeletes(
	ctx context.Context,
) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer a.safeRollback(ctx, tx, "document version archiving")

	q := postgres.New(tx)

	deleteOrder, err := q.GetDocumentForDeletion(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to get pending deletions: %w", err)
	}

	prefix := fmt.Sprintf("documents/%s/", deleteOrder.Uuid)
	dstPrefix := fmt.Sprintf("deleted/%s/%d/",
		deleteOrder.Uuid, deleteOrder.DeleteRecordID)

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
		return fmt.Errorf("failed to move all document objects: %w", err)
	}

	count, err := q.FinaliseDelete(ctx, deleteOrder.Uuid)
	if err != nil {
		return fmt.Errorf("failed to finalise delete: %w", err)
	}

	if count != 1 {
		return errors.New(
			"no match for document delete, database might be inconsisten")
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
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
) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer a.safeRollback(ctx, tx, "document version archiving")

	q := postgres.New(tx)

	unarchived, err := q.GetDocumentVersionForArchiving(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	} else if err != nil {
		return fmt.Errorf(
			"failed to get unarchived version from the database: %w",
			err)
	}

	dv := ArchivedDocumentVersion{
		UUID:            unarchived.Uuid,
		Version:         unarchived.Version,
		Created:         unarchived.Created.Time,
		CreatorURI:      unarchived.CreatorUri,
		Meta:            unarchived.Meta,
		DocumentData:    unarchived.DocumentData,
		ParentSignature: unarchived.ParentSignature.String,
	}

	objectBody, err := json.Marshal(&dv)
	if err != nil {
		return fmt.Errorf(
			"failed to marshal archive data to JSON: %w", err)
	}

	checksum := sha256.Sum256(objectBody)

	signingKey := a.signingKeys.CurrentKey(time.Now())
	if signingKey == nil {
		return fmt.Errorf(
			"no signing keys have been configured: %w", err)
	}

	signature, err := NewArchiveSignature(signingKey, checksum)
	if err != nil {
		return fmt.Errorf("failed to sign archive data: %w", err)
	}

	key := fmt.Sprintf("documents/%s/versions/%d.json", dv.UUID, dv.Version)

	err = q.SetDocumentVersionAsArchived(ctx, postgres.SetDocumentVersionAsArchivedParams{
		Uuid:      dv.UUID,
		Version:   dv.Version,
		Signature: signature.String(),
	})
	if err != nil {
		return fmt.Errorf("failed to update archived status in db: %w",
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
		return fmt.Errorf(
			"failed to store archive object: %w", err)
	}

	err = q.Notify(ctx, postgres.NotifyParams{
		Channel: "archived",
		Message: fmt.Sprintf("version:%s:%d", dv.UUID, dv.Version),
	})
	if err != nil {
		a.logger.WithContext(ctx).WithFields(logrus.Fields{
			"document_uuid":    dv.UUID,
			"document_version": dv.Version,
		}).Error("failed to send archive notification")
	}

	err = tx.Commit(ctx)
	if err != nil {
		_, cErr := a.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket:    aws.String(a.bucket),
			Key:       aws.String(key),
			VersionId: putRes.VersionId,
		})
		if cErr != nil {
			a.logger.WithContext(ctx).WithError(cErr).Error(
				"failed to clean up archive object after commit failed",
			)
		}

		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
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
) error {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer a.safeRollback(ctx, tx, "document version archiving")

	q := postgres.New(tx)

	unarchived, err := q.GetDocumentStatusForArchiving(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	} else if err != nil {
		return fmt.Errorf(
			"failed to get unarchived version from the database: %w",
			err)
	}

	ds := ArchivedDocumentStatus{
		UUID:             unarchived.Uuid,
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
		return fmt.Errorf(
			"failed to marshal archive data to JSON: %w", err)
	}

	checksum := sha256.Sum256(objectBody)

	signingKey := a.signingKeys.CurrentKey(time.Now())
	if signingKey == nil {
		return fmt.Errorf(
			"no signing keys have been configured: %w", err)
	}

	signature, err := NewArchiveSignature(signingKey, checksum)
	if err != nil {
		return fmt.Errorf("failed to sign archive data: %w", err)
	}

	key := fmt.Sprintf("documents/%s/statuses/%s/%d.json",
		ds.UUID, ds.Name, ds.ID)

	err = q.SetDocumentStatusAsArchived(ctx, postgres.SetDocumentStatusAsArchivedParams{
		Uuid:      ds.UUID,
		ID:        ds.ID,
		Signature: signature.String(),
	})
	if err != nil {
		return fmt.Errorf("failed to update archived status in db: %w",
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
		return fmt.Errorf(
			"failed to store archive object: %w", err)
	}

	err = q.Notify(ctx, postgres.NotifyParams{
		Channel: "archived",
		Message: fmt.Sprintf("status:%s:%s:%d",
			ds.UUID, ds.Name, ds.ID),
	})
	if err != nil {
		a.logger.WithContext(ctx).WithFields(logrus.Fields{
			"document_uuid":      ds.UUID,
			"document_status":    ds.Name,
			"document_status_id": ds.ID,
		}).Error("failed to send archive notification")
	}

	err = tx.Commit(ctx)
	if err != nil {
		_, cErr := a.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket:    aws.String(a.bucket),
			Key:       aws.String(key),
			VersionId: putRes.VersionId,
		})
		if cErr != nil {
			a.logger.WithContext(ctx).WithError(cErr).Error(
				"failed to clean up archive object after commit failed",
			)
		}

		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (a *Archiver) ensureSigningKeys(ctx context.Context) error {
	tx, err := a.pool.Begin(ctx)

	defer a.safeRollback(ctx, tx, "signing keys")

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

		set.keys = append(set.keys, spec)
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

		set.keys = append(set.keys, signingKey)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit signing keys: %w", err)
	}

	a.signingKeysChecked = time.Now()
	a.signingKeys.Replace(set.keys)

	return nil
}

func (a *Archiver) safeRollback(ctx context.Context, tx pgx.Tx, txName string) {
	err := tx.Rollback(context.Background())
	if err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		a.logger.WithContext(ctx).WithError(err).WithField(
			"transaction", txName,
		).Error("failed to roll back")
	}
}
