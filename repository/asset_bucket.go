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

// assetURLExpiry is how long a presigned upload or download URL stays valid.
const assetURLExpiry = 15 * time.Minute

func NewAssetBucket(
	log *slog.Logger,
	client *s3.Client,
	name string,
) *AssetBucket {
	// The presign client is built here rather than passed in because a
	// presigned URL this service hands out has to be complete on its own: it
	// goes to a browser or a plain HTTP client, over the API, and whoever
	// fetches it sends nothing but Host.
	//
	// Response checksum validation is what would break that, and it is on
	// by default. s3 v1.107.0 started asking for it as an
	// x-amz-checksum-mode header, and the core this branch was first pinned
	// to (v1.43.7) signed that header instead of hoisting it into the query
	// string, so the signature came to cover a header the caller was never
	// told to send -- which MinIO answers with AccessDenied and S3 with a
	// signature mismatch.
	//
	// core v1.46.0 took it back out of the signature, by excluding
	// X-Amz-Checksum-Mode from the signer's RequiredSignedHeaders, so at the
	// current pins the URLs are self-contained whether or not this option is
	// set. It stays because that is an SDK implementation detail that has
	// moved twice already, and because validating a response checksum is
	// only of use to a client that reads the checksum headers, which no
	// holder of one of these URLs is. TestAssetURLsAreSelfContained asserts
	// the property rather than either mechanism, so it holds whichever way
	// the SDK moves next -- but note that it can only fail on a core older
	// than v1.46.0, which is the axis that actually moves this.
	presign := s3.NewPresignClient(client,
		s3.WithPresignExpires(assetURLExpiry),
		s3.WithPresignClientFromClientOptions(func(o *s3.Options) {
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}))

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
	}, s3.WithPresignExpires(assetURLExpiry))
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
	}, s3.WithPresignExpires(assetURLExpiry))
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
