package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ttab/docformat"
	"github.com/ttab/docformat/private/cmd"
	"github.com/urfave/cli/v2"
)

var app = cli.App{
	Name:  "repository",
	Usage: "The Elephant repository",
	Commands: []*cli.Command{
		{
			Name:        "run",
			Description: "Runs the repository server",
			Action:      runServer,
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "db",
					Value: "postgres://repository:pass@localhost/repository",
				},
				&cli.StringFlag{
					Name:  "addr",
					Value: ":1337",
				},
				&cli.StringFlag{
					Name:    "archive-bucket",
					Value:   "elephant-archive",
					EnvVars: []string{"ARCHIVE_BUCKET"},
				},
				&cli.StringFlag{
					Name:    "s3-endpoint",
					Usage:   "Override the S3 endpoint for use with Minio",
					EnvVars: []string{"S3_ENDPOINT"},
				},
				&cli.StringFlag{
					Name:    "s3-key-id",
					Usage:   "Access key ID to use as a static credential with Minio",
					EnvVars: []string{"S3_ACCESS_KEY_ID"},
				},
				&cli.StringFlag{
					Name:    "s3-key-secret",
					Usage:   "Access key secret to use as a static credential with Minio",
					EnvVars: []string{"S3_ACCESS_KEY_SECRET"},
				},
				&cli.BoolFlag{
					Name:  "s3-insecure",
					Usage: "Disable https for S3 access when using minio",
				},
			},
		},
	},
}

func main() {
	if err := app.Run(os.Args); err != nil {
		println(err.Error())
		os.Exit(1)
	}
}

func runServer(c *cli.Context) error {
	var (
		dbURI = c.String("db")
		addr  = c.String("addr")
		// bucket      = c.String("archive-bucket")
		s3Endpoint  = c.String("s3-endpoint")
		s3KeyID     = c.String("s3-key-id")
		s3KeySecret = c.String("s3-key-secret")
		s3Insecure  = c.Bool("s3-insecure")
	)

	dbpool, err := pgxpool.New(context.Background(), dbURI)
	if err != nil {
		return fmt.Errorf("unable to create connection pool: %w", err)
	}
	defer dbpool.Close()

	repl := docformat.NewPGReplication(dbpool)

	err = repl.Run(c.Context, dbURI)
	if err != nil {
		return fmt.Errorf("failed to run replicator: %w", err)
	}

	store, err := docformat.NewPGDocStore(dbpool)
	if err != nil {
		return fmt.Errorf("failed to create doc store: %w", err)
	}

	_, err = docformat.ArchiveS3Client(c.Context, docformat.S3Options{
		Endpoint:        s3Endpoint,
		AccessKeyID:     s3KeyID,
		AccessKeySecret: s3KeySecret,
		DisableHTTPS:    s3Insecure,
	})
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	validator, err := cmd.DefaultValidator()
	if err != nil {
		return err
	}

	return docformat.RunServer(c.Context, addr,
		docformat.WithAPIServer(nil, store, validator),
	)
}
