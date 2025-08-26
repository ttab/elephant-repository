package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type ArchiveReaderOptions struct {
	S3          *s3.Client
	Bucket      string
	SigningKeys *SigningKeySet
}

func NewArchiveReader(opts ArchiveReaderOptions) *ArchiveReader {
	return &ArchiveReader{
		s3:          opts.S3,
		bucket:      opts.Bucket,
		signingKeys: opts.SigningKeys,
	}
}

type ArchiveReader struct {
	s3          *s3.Client
	bucket      string
	signingKeys *SigningKeySet
}

// ReadDeleteManifest reads and verifies a delete manifest from the archive.
func (a *ArchiveReader) ReadDeleteManifest(
	ctx context.Context,
	key string,
) (_ *DeleteManifest, _ string, outErr error) {
	var obj DeleteManifest

	sigStr, err := a.fetchAndVerify(
		ctx, key, nil, &obj,
	)
	if err != nil {
		return
	}

	return &obj, sigStr, nil
}

// ReadEvent reads and verifies a archived event from the archive. If
// parentSignature is provided the read version will be verified against it.
func (a *ArchiveReader) ReadEvent(
	ctx context.Context,
	id int64, parentSignature *string,
) (_ *ArchivedEventlogItem, _ string, outErr error) {
	var obj ArchivedEventlogItem

	key := fmt.Sprintf("events/%020d.json", id)

	sigStr, err := a.fetchAndVerify(
		ctx, key, parentSignature, &obj,
	)
	if err != nil {
		return nil, "", err
	}

	return &obj, sigStr, nil
}

// ReadDocumentVersion reads and verifies a document version from the
// archive. If parentSignature is provided the read version will be verified
// against it.
func (a *ArchiveReader) ReadDocumentVersion(
	ctx context.Context,
	key string, parentSignature *string,
) (_ *ArchivedDocumentVersion, _ string, outErr error) {
	var obj ArchivedDocumentVersion

	sigStr, err := a.fetchAndVerify(
		ctx, key, parentSignature, &obj,
	)
	if err != nil {
		return nil, "", err
	}

	return &obj, sigStr, nil
}

// ReadDocumentStatus reads and verifies a document status from the
// archive. If parentSignature is provided the read version will be verified
// against it.
func (a *ArchiveReader) ReadDocumentStatus(
	ctx context.Context,
	key string, parentSignature *string,
) (*ArchivedDocumentStatus, string, error) {
	var obj ArchivedDocumentStatus

	sigStr, err := a.fetchAndVerify(
		ctx, key, parentSignature, &obj,
	)
	if err != nil {
		return nil, "", err
	}

	return &obj, sigStr, nil
}

func (a *ArchiveReader) fetchAndVerify(
	ctx context.Context,
	key string, parentSignature *string, obj ArchivedObject,
) (_ string, outErr error) {
	res, err := a.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf(
			"get archive object from S3: %w", err)
	}

	defer func() {
		err := res.Body.Close()
		if err != nil {
			outErr = errors.Join(outErr, fmt.Errorf(
				"close S3 response body: %w", err))
		}
	}()

	sigStr := res.Metadata["elephant-signature"]

	signature, err := ParseArchiveSignature(sigStr)
	if err != nil {
		return "", fmt.Errorf("invalid object signature: %w", err)
	}

	signingKey := a.signingKeys.GetKeyByID(signature.KeyID)
	if signingKey == nil {
		return "", errors.New("unknown signing key")
	}

	err = signature.Verify(signingKey)
	if err != nil {
		return "", fmt.Errorf("verify signature: %w", err)
	}

	hash := sha256.New()
	dec := json.NewDecoder(io.TeeReader(res.Body, hash))

	err = dec.Decode(&obj)
	if err != nil {
		return "", fmt.Errorf(
			"unmarshal archived object: %w", err)
	}

	if !bytes.Equal(hash.Sum(nil), signature.Hash[:]) {
		return "", errors.New("object does not match the signature")
	}

	if !signingKey.UsableAt(obj.GetArchivedTime()) {
		return "", errors.New(
			"signing key was not valid at the time of archiving")
	}

	if parentSignature != nil && obj.GetParentSignature() != *parentSignature {
		return "", errors.New("parent signature mismatch")
	}

	return sigStr, nil
}
