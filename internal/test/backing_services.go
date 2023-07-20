package test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/tern/v2/migrate"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephant-repository/schema"
	"github.com/ttab/elephantine"
	"golang.org/x/sync/errgroup"
)

var (
	m    sync.Mutex
	bs   *BackingServices
	bErr error
)

type Environment struct {
	S3           *s3.Client
	Bucket       string
	PostgresURI  string
	ReportingURI string
	Migrator     *migrate.Migrator
}

type T interface {
	Name() string
	Helper()
	Fatalf(format string, args ...any)
}

func SetUpBackingServices(
	t T,
	instrument *elephantine.HTTPClientInstrumentation,
	skipMigrations bool,
) Environment {
	t.Helper()

	ctx := context.Background()

	bs, err := GetBackingServices()
	must(t, err, "get backing services")

	var client http.Client

	err = instrument.Client("s3", &client)
	must(t, err, "instrument s3 http client")

	s3Client, err := bs.getS3Client(&client)
	must(t, err, "get S3 client")

	bucket := strings.ToLower(t.Name())

	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	must(t, err, "create bucket")

	adminConn, err := pgx.Connect(ctx,
		bs.getPostgresURI("elephant", "elephant"))
	must(t, err, "open postgres admin connection")

	defer adminConn.Close(ctx)

	ident := pgx.Identifier{t.Name()}.Sanitize()

	_, err = adminConn.Exec(ctx, fmt.Sprintf(`
CREATE ROLE %s WITH LOGIN PASSWORD '%s' REPLICATION`,
		ident, t.Name()))
	must(t, err, "create user")

	_, err = adminConn.Exec(ctx,
		"CREATE DATABASE "+ident+" WITH OWNER "+ident)
	must(t, err, "create database")

	reportingRole := pgx.Identifier{t.Name() + "Reporting"}.Sanitize()
	reportingUser := pgx.Identifier{t.Name() + "ReportingUser"}.Sanitize()

	_, err = adminConn.Exec(ctx, fmt.Sprintf(
		"CREATE ROLE %s",
		reportingRole))
	must(t, err, "create reporting role")

	_, err = adminConn.Exec(ctx, fmt.Sprintf(
		`
CREATE ROLE %[1]s
WITH LOGIN PASSWORD '%[2]s'
IN ROLE %[3]s`,
		reportingUser, t.Name()+"ReportingUser", reportingRole))
	must(t, err, "create reporting user")

	env := Environment{
		S3:          s3Client,
		Bucket:      bucket,
		PostgresURI: bs.getPostgresURI(t.Name(), t.Name()),
		ReportingURI: bs.getPostgresURI(
			t.Name()+"ReportingUser", t.Name(),
		),
	}

	conn, err := pgx.Connect(ctx, env.PostgresURI)
	must(t, err, "open postgres user connection")

	defer conn.Close(ctx)

	m, err := migrate.NewMigrator(ctx, conn, "schema_vesion")
	must(t, err, "create migrator")

	err = m.LoadMigrations(schema.Migrations)
	must(t, err, "create load migrations")

	if !skipMigrations {
		err = m.Migrate(ctx)
		must(t, err, "migrate to current DB schema")

		_, err = conn.Exec(ctx, fmt.Sprintf(`
GRANT SELECT
ON TABLE
   document, delete_record, document_version,
   document_status, status_heads, status, status_rule,
   acl, acl_audit
TO %s`,
			reportingRole))
		must(t, err, "failed to grant reporting role permissions")
	}

	env.Migrator = m

	return env
}

func must(t T, err error, action string) {
	t.Helper()

	if err != nil {
		t.Fatalf("failed: %s: %v", action, err)
	}
}

func PurgeBackingServices() error {
	m.Lock()
	defer m.Unlock()

	if bs == nil {
		return nil
	}

	return bs.Purge()
}

func GetBackingServices() (*BackingServices, error) {
	m.Lock()
	defer m.Unlock()

	if bs != nil || bErr != nil {
		return bs, bErr
	}

	bs, bErr = createBackingServices()

	return bs, bErr
}

func createBackingServices() (*BackingServices, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, fmt.Errorf("failed to create docker pool: %w", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		return nil, fmt.Errorf("could not connect to docker: %w", err)
	}

	b := BackingServices{
		pool: pool,
	}

	var grp errgroup.Group

	grp.Go(b.bootstrapMinio)
	grp.Go(b.bootstrapPostgres)

	err = grp.Wait()
	if err != nil {
		pErr := b.Purge()
		if pErr != nil {
			return nil, errors.Join(err, pErr)
		}

		return nil, err //nolint:wrapcheck
	}

	return &b, nil
}

type BackingServices struct {
	pool     *dockertest.Pool
	minio    *dockertest.Resource
	postgres *dockertest.Resource
}

func (bs *BackingServices) bootstrapMinio() error {
	res, err := bs.pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "RELEASE.2023-02-22T18-23-45Z",
		Cmd:        []string{"server", "/data"},
	}, func(hc *docker.HostConfig) {
		hc.AutoRemove = true
	})
	if err != nil {
		return fmt.Errorf("failed to run minio container: %w", err)
	}

	bs.minio = res

	// Make sure that containers don't stick around for more than an hour,
	// even if in-process cleanup fails.
	_ = res.Expire(3600)

	client, err := bs.getS3Client(http.DefaultClient)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	err = bs.pool.Retry(func() error {
		_, err := client.ListBuckets(
			context.Background(), &s3.ListBucketsInput{})
		if err != nil {
			log.Println(err.Error())

			return fmt.Errorf("failed to list buckets: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to connect to minio: %w", err)
	}

	return nil
}

func (bs *BackingServices) getS3Client(client *http.Client) (*s3.Client, error) {
	svc, err := repository.S3Client(context.Background(),
		repository.S3Options{
			Endpoint: fmt.Sprintf("http://localhost:%s/",
				bs.minio.GetPort("9000/tcp")),
			AccessKeyID:     "minioadmin",
			AccessKeySecret: "minioadmin",
			HTTPClient:      client,
		})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	return svc, nil
}

func (bs *BackingServices) getPostgresURI(user, database string) string {
	return fmt.Sprintf(
		"postgres://%[1]s:%[1]s@localhost:%[3]s/%[2]s",
		user, database, bs.postgres.GetPort("5432/tcp"))
}

func (bs *BackingServices) bootstrapPostgres() error {
	res, err := bs.pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "15.2",
		Env: []string{
			"POSTGRES_USER=elephant",
			"POSTGRES_PASSWORD=elephant",
		},
		Cmd: []string{
			"-c", "wal_level=logical",
		},
	}, func(hc *docker.HostConfig) {
		hc.AutoRemove = true
	})
	if err != nil {
		return fmt.Errorf("failed to run postgres container: %w", err)
	}

	bs.postgres = res

	// Make sure that containers don't stick around for more than an hour,
	// even if in-process cleanup fails.
	_ = res.Expire(3600)

	err = bs.pool.Retry(func() error {
		conn, err := pgx.Connect(context.Background(),
			bs.getPostgresURI("elephant", "elephant"))
		if err != nil {
			return fmt.Errorf("failed to create postgres connection: %w", err)
		}

		err = conn.Ping(context.Background())
		if err != nil {
			log.Println(err.Error())

			return fmt.Errorf("failed to ping database: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}

	return nil
}

func (bs *BackingServices) Purge() error {
	var errs []error

	if bs.minio != nil {
		err := bs.pool.Purge(bs.minio)
		if err != nil {
			errs = append(errs, fmt.Errorf(
				"failed to purge minio container: %w", err))
		}
	}

	if bs.postgres != nil {
		err := bs.pool.Purge(bs.postgres)
		if err != nil {
			errs = append(errs, fmt.Errorf(
				"failed to purge postgres container: %w", err))
		}
	}

	switch len(errs) {
	case 1:
		return errs[0]
	case 0:
		return nil
	default:
		return errors.Join(errs...)
	}
}
