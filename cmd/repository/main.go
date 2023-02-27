package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"
	"github.com/ttab/elephant/internal/cmd"
	"github.com/ttab/elephant/repository"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
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
		fmt.Fprintln(os.Stderr, err.Error())
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

	dbpool, err := pgxpool.New(c.Context, conf.DB)
	if err != nil {
		return fmt.Errorf("unable to create connection pool: %w", err)
	}
	defer dbpool.Close()

	err = dbpool.Ping(c.Context)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	store, err := repository.NewPGDocStore(logger, dbpool)
	if err != nil {
		return fmt.Errorf("failed to create doc store: %w", err)
	}

	go store.RunListener(c.Context)

	setupCtx, cancel := context.WithTimeout(c.Context, 10*time.Second)
	defer cancel()

	group, gCtx := errgroup.WithContext(setupCtx)

	if !conf.NoReplicator {
		group.Go(func() error {
			logger.Debug("setting up replication")

			repl := repository.NewPGReplication(logger, dbpool, conf.DB)

			go repl.Run(c.Context)

			return nil
		})
	}

	if !conf.NoArchiver {
		group.Go(func() error {
			return startArchiver(c.Context, gCtx,
				logger, conf, dbpool)
		})
	}

	err = group.Wait()
	if err != nil {
		return fmt.Errorf("subsystem setup failed: %w", err)
	}

	validator, err := repository.NewValidator(
		c.Context, logger, store)
	if err != nil {
		return fmt.Errorf("failed to create validator: %w", err)
	}

	apiServer := repository.NewAPIServer(store, validator)

	logger.Debug("starting API server")

	router := httprouter.New()

	err = repository.SetUpRouter(router,
		repository.WithAPIServer(logger, signingKey, apiServer),
	)
	if err != nil {
		return fmt.Errorf("failed to set up router: %w", err)
	}

	err = repository.ListenAndServe(c.Context, addr, router)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	return nil
}

func startArchiver(
	ctx context.Context, setupCtx context.Context, logger *logrus.Logger,
	conf cmd.BackendConfig, dbpool *pgxpool.Pool,
) error {
	logger.Debug("setting up archiver")

	aS3, err := repository.ArchiveS3Client(setupCtx, conf.S3Options)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	archiver := repository.NewArchiver(repository.ArchiverOptions{
		Logger: logger,
		S3:     aS3,
		Bucket: conf.ArchiveBucket,
		DB:     dbpool,
	})

	logger.Debug("starting archiver")

	err = archiver.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run archiver: %w", err)
	}

	return nil
}
