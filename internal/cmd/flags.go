package cmd

import (
	"github.com/ttab/elephant/repository"
	"github.com/urfave/cli/v2"
)

type BackendConfig struct {
	repository.S3Options
	DB            string
	ReportingDB   string
	ArchiveBucket string
	ReportBucket  string
	S3Endpoint    string
	S3KeyID       string
	S3KeySecret   string
	S3Insecure    bool
	NoArchiver    bool
	ArchiverCount int
	NoReplicator  bool
	NoReporter    bool
	JWTSigningKey string
	SharedSecret  string
}

func BackendConfigFromContext(c *cli.Context) BackendConfig {
	return BackendConfig{
		DB:            c.String("db"),
		ReportingDB:   c.String("reporting-db"),
		ArchiveBucket: c.String("archive-bucket"),
		ReportBucket:  c.String("report-bucket"),
		NoArchiver:    c.Bool("no-archiver"),
		NoReporter:    c.Bool("no-reporter"),
		NoReplicator:  c.Bool("no-replicator"),
		JWTSigningKey: c.String("jwt-signing-key"),
		SharedSecret:  c.String("shared-secret"),
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
			Name:    "db",
			Value:   "postgres://repository:pass@localhost/repository",
			EnvVars: []string{"CONN_STRING"},
		},
		&cli.StringFlag{
			Name:    "reporting-db",
			Value:   "postgres://reportuser:reportuser@localhost/repository",
			EnvVars: []string{"REPORTING_CONN_STRING"},
		},
		&cli.StringFlag{
			Name:    "archive-bucket",
			Value:   "elephant-archive",
			EnvVars: []string{"ARCHIVE_BUCKET"},
		},
		&cli.StringFlag{
			Name:    "report-bucket",
			Value:   "elephant-reports",
			EnvVars: []string{"REPORT_BUCKET"},
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
		&cli.BoolFlag{
			Name:  "no-reporter",
			Usage: "Disable the reporter",
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
		&cli.StringFlag{
			Name:    "shared-secret",
			Usage:   "Shared secret to be used in password grants",
			EnvVars: []string{"SHARED_PASSWORD_SECRET"},
		},
	}
}
