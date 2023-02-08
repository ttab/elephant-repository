package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
	"github.com/ttab/docformat"
	"github.com/ttab/docformat/private/cmd"
	"github.com/urfave/cli/v2"
)

func main() {
	runCmd := cli.Command{
		Name:        "run",
		Description: "Runs the repository server",
		Action:      runServer,
		Flags: append([]cli.Flag{
			&cli.StringFlag{
				Name:  "addr",
				Value: ":1080",
			},
			&cli.StringFlag{
				Name:    "log-level",
				EnvVars: []string{"LOG_LEVEL"},
				Value:   "error",
			},
		}, cmd.BackendFlags()...),
	}

	var app = cli.App{
		Name:  "repository",
		Usage: "The Elephant repository",
		Commands: []*cli.Command{
			&runCmd,
		},
	}

	if err := app.Run(os.Args); err != nil {
		println(err.Error())
		os.Exit(1)
	}
}

func runServer(c *cli.Context) error {
	var (
		addr     = c.String("addr")
		logLevel = c.String("log-level")
		conf     = cmd.BackendConfigFromContext(c)
		logger   = logrus.New()
	)

	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		level = logrus.ErrorLevel
		logger.Errorf("invalid log level %q", logLevel)
	}

	logger.SetLevel(level)

	var signingKey *ecdsa.PrivateKey

	if conf.JWTSigningKey != "" {
		keyData, err := base64.RawURLEncoding.DecodeString(
			conf.JWTSigningKey)
		if err != nil {
			return fmt.Errorf(
				"invalid base64 encoding for JWT signing key: %w", err)
		}

		k, err := x509.ParseECPrivateKey(keyData)
		if err != nil {
			return fmt.Errorf(
				"invalid JWT signing key: %w", err)
		}

		signingKey = k
	}

	dbpool, err := pgxpool.New(context.Background(), conf.DB)
	if err != nil {
		return fmt.Errorf("unable to create connection pool: %w", err)
	}
	defer dbpool.Close()

	store, err := docformat.NewPGDocStore(dbpool)
	if err != nil {
		return fmt.Errorf("failed to create doc store: %w", err)
	}

	if !conf.NoReplicator {
		logger.Debug("setting up replication")

		repl := docformat.NewPGReplication(logger, dbpool, conf.DB)

		go repl.Run(c.Context)
	}

	if !conf.NoArchiver {
		logger.Debug("setting up archiver")

		aS3, err := docformat.ArchiveS3Client(c.Context, conf.S3Options)
		if err != nil {
			return fmt.Errorf("failed to create S3 client: %w", err)
		}

		archiver := docformat.NewArchiver(docformat.ArchiverOptions{
			Logger: logger,
			S3:     aS3,
			Bucket: conf.ArchiveBucket,
			DB:     dbpool,
		})

		err = archiver.Run(c.Context)
		if err != nil {
			return fmt.Errorf("failed to run archiver: %w", err)
		}
	}

	validator, err := cmd.DefaultValidator()
	if err != nil {
		return fmt.Errorf("failed to create validator: %w", err)
	}

	apiServer := docformat.NewAPIServer(store, validator)

	logger.Debug("starting API server")

	return docformat.RunServer(c.Context, addr,
		docformat.WithAPIServer(logger, signingKey, apiServer),
	)
}
