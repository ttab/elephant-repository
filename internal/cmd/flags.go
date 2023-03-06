package cmd

import (
	"github.com/ttab/elephant/repository"
	"github.com/urfave/cli/v2"
)

type BackendConfig struct {
	repository.S3Options
	DB            string
	ArchiveBucket string
	S3Endpoint    string
	S3KeyID       string
	S3KeySecret   string
	S3Insecure    bool
	NoArchiver    bool
	ArchiverCount int
	NoReplicator  bool
	JWTSigningKey string
}

func BackendConfigFromContext(c *cli.Context) BackendConfig {
	return BackendConfig{
		DB:            c.String("db"),
		ArchiveBucket: c.String("archive-bucket"),
		NoArchiver:    c.Bool("no-archiver"),
		ArchiverCount: c.Int("archiver-count"),
		NoReplicator:  c.Bool("no-replicator"),
		JWTSigningKey: c.String("jwt-signing-key"),
		S3Options: repository.S3Options{
			Endpoint:        c.String("s3-endpoint"),
			AccessKeyID:     c.String("s3-key-id"),
			AccessKeySecret: c.String("s3-key-secret"),
			DisableHTTPS:    c.Bool("s3-insecure"),
		},
	}
}

func BackendFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "db",
			Value: "postgres://repository:pass@localhost/repository",
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
		&cli.BoolFlag{
			Name:  "no-archiver",
			Usage: "Disable the archiver",
		},
		&cli.IntFlag{
			Name:  "archiver-count",
			Usage: "Number of archivers to run",
			Value: 1,
		},
		&cli.BoolFlag{
			Name:  "no-replicator",
			Usage: "Disable the replicator",
		},
		&cli.StringFlag{
			Name:    "jwt-signing-key",
			Usage:   "ECDSA signing key used for mock JWTs",
			EnvVars: []string{"JWT_SIGNING_KEY"},
		},
	}
}
