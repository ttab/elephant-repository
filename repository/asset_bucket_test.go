package repository_test

import (
	"log/slog"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine/test"
)

// TestAssetURLsAreSelfContained pins the property the presigned asset URLs have
// to have: everything the signature covers is in the URL. They are handed to
// callers over the API and fetched by browsers and plain HTTP clients that send
// nothing but Host, so a signature covering a request header is a URL nobody
// can use -- MinIO answers it with AccessDenied and S3 with a signature
// mismatch. The SDK decides that, and it has changed its mind before: s3
// v1.107.0 moved response checksum validation from a query parameter to a
// signed x-amz-checksum-mode header, which broke every download link until the
// asset bucket turned validation off. This test is what catches the next one,
// in a sentence rather than as an unexplained AccessDenied in the asset tests.
//
// It needs no backing services: presigning is offline.
func TestAssetURLsAreSelfContained(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	client, err := repository.S3Client(ctx, repository.S3Options{
		Endpoint:        "http://localhost:9000/",
		AccessKeyID:     "test",
		AccessKeySecret: "test",
	})
	test.Mustf(t, err, "create the S3 client")

	bucket := repository.NewAssetBucket(logger, client, "assets")

	id := uuid.MustParse("ffa05627-be7a-4f09-8bfc-bc3361b0b0b5")

	upload, err := bucket.CreateUploadURL(ctx, id)
	test.Mustf(t, err, "create an upload URL")

	download, err := bucket.CreateDownloadURL(ctx, id, "plaintext")
	test.Mustf(t, err, "create a download URL")

	for _, c := range []struct {
		name string
		url  string
	}{
		{name: "upload", url: upload},
		{name: "download", url: download},
	} {
		parsed, err := url.Parse(c.url)
		test.Mustf(t, err, "parse the %s URL", c.name)

		test.Equalf(t, "host",
			parsed.Query().Get("X-Amz-SignedHeaders"),
			"sign nothing but the host header in the %s URL", c.name)
	}
}
