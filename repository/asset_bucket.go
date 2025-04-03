package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/ttab/elephantine"
)

func NewAssetBucket(
	log *slog.Logger,
	presign *s3.PresignClient,
	client *s3.Client,
	name string,
) *AssetBucket {
	return &AssetBucket{
		log:     log,
		presign: presign,
		client:  client,
		name:    name,
	}
}

type AssetBucket struct {
	log     *slog.Logger
	presign *s3.PresignClient
	client  *s3.Client
	name    string
}

// CreateUploadURL creates a presigned upload URL that clients can use to upload
// an asset to the object store.
func (ab *AssetBucket) CreateUploadURL(
	ctx context.Context, id uuid.UUID,
) (string, error) {
	req, err := ab.presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: &ab.name,
		Key:    aws.String(fmt.Sprintf("uploads/%s", id)),
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		return "", fmt.Errorf("sign upload URL: %w", err)
	}

	return req.URL, nil
}

// CreateDownloadURL creates a presigned download URL that clients can use to
// download an asset from the object store.
func (ab *AssetBucket) CreateDownloadURL(
	ctx context.Context, document uuid.UUID, name string,
) (string, error) {
	req, err := ab.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: &ab.name,
		Key:    aws.String(ab.objKey(document, name)),
	}, s3.WithPresignExpires(15*time.Minute))
	if err != nil {
		return "", fmt.Errorf("sign download URL: %w", err)
	}

	return req.URL, nil
}

func (ab *AssetBucket) UploadExists(
	ctx context.Context, id uuid.UUID,
) (bool, error) {
	var ae smithy.APIError

	_, err := ab.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(ab.name),
		Key:    aws.String(fmt.Sprintf("uploads/%s", id)),
	})

	switch {
	// See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
	case errors.As(err, &ae) && ae.ErrorCode() == "NoSuchKey":
		return false, nil
	case err != nil:
		return false, fmt.Errorf("check if object exists: %w", err)
	}

	return true, nil
}

// AttachUpload to a document and returns the object version. Name here is the
// object name for the attachment.
func (ab *AssetBucket) AttachUpload(
	ctx context.Context,
	upload uuid.UUID,
	document uuid.UUID,
	name string,
) (string, error) {
	key := ab.objKey(document, name)
	sourceKey := fmt.Sprintf("uploads/%s", upload)
	source := fmt.Sprintf("%s/%s", ab.name, sourceKey)

	res, err := ab.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(ab.name),
		Key:        aws.String(key),
		CopySource: &source,
	})
	if err != nil {
		return "", fmt.Errorf("copy upload to document: %w", err)
	}

	if res.VersionId == nil {
		return "", errors.New("unversioned asset bucket")
	}

	_, err = ab.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(ab.name),
		Key:    aws.String(sourceKey),
	})
	if err != nil {
		ab.log.WarnContext(ctx,
			"failed to delete upload object, will be removed by lifecycle rules",
			elephantine.LogKeyBucket, ab.name,
			elephantine.LogKeyObjectKey, sourceKey,
		)
	}

	return *res.VersionId, nil
}

// RevertObject to an earlier version. This will create a new version of the
// object based on the contents of the earlier version.
func (ab *AssetBucket) RevertObject(
	ctx context.Context,
	document uuid.UUID,
	name string,
	version string,
) (string, error) {
	key := ab.objKey(document, name)
	source := fmt.Sprintf("%s/%s?versionId=%s", ab.name, key, version)

	res, err := ab.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(ab.name),
		Key:        aws.String(key),
		CopySource: &source,
	})
	if err != nil {
		return "", fmt.Errorf("copy old version: %w", err)
	}

	return *res.VersionId, nil
}

func (ab *AssetBucket) DeleteObject(
	ctx context.Context,
	document uuid.UUID,
	name string,
) error {
	key := ab.objKey(document, name)

	_, err := ab.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(ab.name),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}

	return nil
}

func (ab *AssetBucket) objKey(
	document uuid.UUID,
	name string,
) string {
	return fmt.Sprintf("objects/%s/%s", name, document)
}
