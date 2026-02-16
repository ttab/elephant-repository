package repository

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/jackc/pgx/v5"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine/pg"
)

const (
	batchSize1k         = 1000
	batchSize10k        = 10000
	batchPrefix1k       = "events_1k"
	batchPrefix10k      = "events_10k"
	batchPollInterval   = 1 * time.Minute
	batchMetricSize1k   = "1k"
	batchMetricSize10k  = "10k"
	batchMetricStatusOK = "ok"
)

func (a *Archiver) runEventlogBatchArchiver(ctx context.Context) error {
	lock, err := pg.NewJobLock(a.pool, a.logger, "eventlog-batch-archiver",
		pg.JobLockOptions{})
	if err != nil {
		return fmt.Errorf("acquire job lock: %w", err)
	}

	return lock.RunWithContext(ctx, a.archiveEventlogBatches)
}

func (a *Archiver) archiveEventlogBatches(ctx context.Context) error {
	a.logger.Info("starting eventlog batch archiver")

	q := postgres.New(a.pool)

	archived := make(chan ArchivedEvent, 10)
	a.store.OnArchivedUpdate(ctx, archived)

	state1k, err := q.GetEventlogArchiver(ctx, batchSize1k)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("get 1k batch position: %w", err)
	}

	state10k, err := q.GetEventlogArchiver(ctx, batchSize10k)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("get 10k batch position: %w", err)
	}

	a.batchArchiverPos1k.Set(float64(state1k.Position))
	a.batchArchiverPos10k.Set(float64(state10k.Position))

	for {
		singleState, err := q.GetEventlogArchiver(ctx, 1)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("get single archiver position: %w", err)
		}

		// Create 1k batches while there are enough archived events.
		for singleState.Position >= state1k.Position+batchSize1k {
			firstID := state1k.Position + 1
			lastID := state1k.Position + batchSize1k

			err := a.createAndUpload1kBatch(ctx, q, firstID, lastID)
			if err != nil {
				a.batchesCreated.WithLabelValues(
					batchMetricSize1k, "error").Inc()

				return fmt.Errorf("create 1k batch: %w", err)
			}

			a.batchesCreated.WithLabelValues(
				batchMetricSize1k, batchMetricStatusOK).Inc()

			state1k.Position = lastID
			a.batchArchiverPos1k.Set(float64(lastID))

			a.logger.InfoContext(ctx,
				"created 1k event batch",
				"first_id", firstID,
				"last_id", lastID)

			// Check if we should create a 10k batch.
			if lastID%batchSize10k == 0 && lastID > state10k.Position {
				first10k := state10k.Position + 1

				err := a.createAndUpload10kBatch(
					ctx, q, first10k, lastID)
				if err != nil {
					a.batchesCreated.WithLabelValues(
						batchMetricSize10k, "error").Inc()

					return fmt.Errorf("create 10k batch: %w", err)
				}

				a.batchesCreated.WithLabelValues(
					batchMetricSize10k, batchMetricStatusOK).Inc()

				state10k.Position = lastID
				a.batchArchiverPos10k.Set(float64(lastID))

				a.logger.InfoContext(ctx,
					"created 10k event batch",
					"first_id", first10k,
					"last_id", lastID)
			}
		}

		// Wait for notifications or poll timeout.
		select {
		case e := <-archived:
			if e.Type != ArchiveEventTypeLogItem {
				continue
			}
		case <-time.After(batchPollInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *Archiver) createAndUpload1kBatch(
	ctx context.Context, q *postgres.Queries,
	firstID, lastID int64,
) error {
	data, err := a.buildBatchZip(ctx, firstID, lastID)
	if err != nil {
		return fmt.Errorf("build batch zip: %w", err)
	}

	key := fmt.Sprintf("%s/%020d_%020d.zip",
		batchPrefix1k, firstID, lastID)

	err = a.uploadBatchZip(ctx, key, data)
	if err != nil {
		return fmt.Errorf("upload batch zip: %w", err)
	}

	err = q.SetEventlogArchiver(ctx,
		postgres.SetEventlogArchiverParams{
			Size:      batchSize1k,
			Position:  lastID,
			Signature: "",
		})
	if err != nil {
		return fmt.Errorf("update 1k archiver state: %w", err)
	}

	return nil
}

func (a *Archiver) buildBatchZip(
	ctx context.Context, firstID, lastID int64,
) ([]byte, error) {
	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	var sigs bytes.Buffer

	for id := firstID; id <= lastID; id++ {
		raw, err := a.reader.ReadEventRaw(ctx, id)
		if err != nil {
			var ae smithy.APIError
			if a.tolerateGaps && errors.As(err, &ae) &&
				ae.ErrorCode() == "NoSuchKey" {
				continue
			}

			return nil, fmt.Errorf(
				"read event %d: %w", id, err)
		}

		name := strconv.FormatInt(id, 10) + ".json"

		w, err := zw.Create(name)
		if err != nil {
			return nil, fmt.Errorf(
				"create zip entry %s: %w", name, err)
		}

		_, err = w.Write(raw.Data)
		if err != nil {
			return nil, fmt.Errorf(
				"write zip entry %s: %w", name, err)
		}

		fmt.Fprintf(&sigs, "%d\t%s\n", id, raw.Signature)
	}

	sw, err := zw.Create("signatures.txt")
	if err != nil {
		return nil, fmt.Errorf("create signatures entry: %w", err)
	}

	_, err = sigs.WriteTo(sw)
	if err != nil {
		return nil, fmt.Errorf("write signatures entry: %w", err)
	}

	err = zw.Close()
	if err != nil {
		return nil, fmt.Errorf("close zip writer: %w", err)
	}

	return buf.Bytes(), nil
}

func (a *Archiver) createAndUpload10kBatch(
	ctx context.Context, q *postgres.Queries,
	firstID, lastID int64,
) error {
	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	var sigs bytes.Buffer

	// Iterate over 1k chunks.
	for start := firstID; start <= lastID; start += batchSize1k {
		end := start + batchSize1k - 1

		srcKey := fmt.Sprintf("%s/%020d_%020d.zip",
			batchPrefix1k, start, end)

		srcData, err := a.downloadBatchZip(ctx, srcKey)
		if err != nil {
			return fmt.Errorf(
				"download 1k batch %s: %w", srcKey, err)
		}

		srcReader, err := zip.NewReader(
			bytes.NewReader(srcData), int64(len(srcData)))
		if err != nil {
			return fmt.Errorf(
				"open 1k batch zip %s: %w", srcKey, err)
		}

		for _, f := range srcReader.File {
			if f.Name == "signatures.txt" {
				err := copyZipEntry(&sigs, f)
				if err != nil {
					return fmt.Errorf(
						"read signatures from %s: %w",
						srcKey, err)
				}

				continue
			}

			w, err := zw.Create(f.Name)
			if err != nil {
				return fmt.Errorf(
					"create 10k zip entry %s: %w",
					f.Name, err)
			}

			err = copyZipEntry(w, f)
			if err != nil {
				return fmt.Errorf(
					"copy entry %s from %s: %w",
					f.Name, srcKey, err)
			}
		}
	}

	sw, err := zw.Create("signatures.txt")
	if err != nil {
		return fmt.Errorf("create 10k signatures entry: %w", err)
	}

	_, err = sigs.WriteTo(sw)
	if err != nil {
		return fmt.Errorf("write 10k signatures entry: %w", err)
	}

	err = zw.Close()
	if err != nil {
		return fmt.Errorf("close 10k zip writer: %w", err)
	}

	key := fmt.Sprintf("%s/%020d_%020d.zip",
		batchPrefix10k, firstID, lastID)

	err = a.uploadBatchZip(ctx, key, buf.Bytes())
	if err != nil {
		return fmt.Errorf("upload 10k batch zip: %w", err)
	}

	err = q.SetEventlogArchiver(ctx,
		postgres.SetEventlogArchiverParams{
			Size:      batchSize10k,
			Position:  lastID,
			Signature: "",
		})
	if err != nil {
		return fmt.Errorf("update 10k archiver state: %w", err)
	}

	return nil
}

func (a *Archiver) uploadBatchZip(
	ctx context.Context, key string, data []byte,
) error {
	checksum := sha256.Sum256(data)

	_, err := a.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(a.bucket),
		Key:               aws.String(key),
		Body:              bytes.NewReader(data),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumSHA256: aws.String(
			base64.StdEncoding.EncodeToString(checksum[:]),
		),
		ContentType:   aws.String("application/zip"),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return fmt.Errorf("put S3 object: %w", err)
	}

	return nil
}

func copyZipEntry(dst io.Writer, f *zip.File) error {
	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("open zip entry: %w", err)
	}

	defer rc.Close()

	// Use LimitReader to bound the decompressed size, which prevents
	// decompression bomb attacks (gosec G110).
	limit := int64(f.UncompressedSize64) //nolint:gosec

	_, err = io.Copy(dst, io.LimitReader(rc, limit+1))
	if err != nil {
		return fmt.Errorf("copy zip entry: %w", err)
	}

	return nil
}

func (a *Archiver) downloadBatchZip(
	ctx context.Context, key string,
) ([]byte, error) {
	res, err := a.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get S3 object: %w", err)
	}

	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("read S3 response body: %w", err)
	}

	return data, nil
}
