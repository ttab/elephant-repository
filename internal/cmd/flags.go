package cmd

import (
	"context"
	"fmt"

	"github.com/ttab/elephant-repository/repository"
	"github.com/urfave/cli/v2"
)

type BackendConfig struct {
	repository.S3Options
	DB                  string
	ReportingDB         string
	Eventsink           string
	ArchiveBucket       string
	ReportBucket        string
	S3Endpoint          string
	S3KeyID             string
	S3KeySecret         string
	S3Insecure          bool
	NoArchiver          bool
	ArchiverCount       int
	NoCoreSchema        bool
	NoReplicator        bool
	NoEventsink         bool
	NoReporter          bool
	MockJWTEndpoint     bool
	MockJWTSigningKey   string
	MockJWTSharedSecret string
	JWKSUrl             string
	JWTIssuer           string
	JWTAudience         string
	JWTScopePrefix      string
}

type ParameterSource func(ctx context.Context, name string) (string, error)

func BackendConfigFromContext(c *cli.Context, src ParameterSource) (BackendConfig, error) {
	cfg := BackendConfig{
		DB:                  c.String("db"),
		ReportingDB:         c.String("reporting-db"),
		Eventsink:           c.String("eventsink"),
		ArchiveBucket:       c.String("archive-bucket"),
		ReportBucket:        c.String("report-bucket"),
		NoArchiver:          c.Bool("no-archiver"),
		NoEventsink:         c.Bool("no-eventsink"),
		NoReporter:          c.Bool("no-reporter"),
		NoReplicator:        c.Bool("no-replicator"),
		MockJWTEndpoint:     c.Bool("mock-jwt-endpoint"),
		MockJWTSigningKey:   c.String("mock-jwt-signing-key"),
		MockJWTSharedSecret: c.String("mock-jwt-shared-secret"),
		JWKSUrl:             c.String("jwks-url"),
		JWTIssuer:           c.String("jwt-issuer"),
		JWTAudience:         c.String("jwt-audience"),
		JWTScopePrefix:      c.String("jwt-scope-prefix"),
		S3Options: repository.S3Options{
			Endpoint:        c.String("s3-endpoint"),
			AccessKeyID:     c.String("s3-key-id"),
			AccessKeySecret: c.String("s3-key-secret"),
			DisableHTTPS:    c.Bool("s3-insecure"),
		},
	}

	db, err := resolveParam(c, src, "db-parameter", cfg.DB)
	if err != nil {
		return BackendConfig{}, err
	}

	cfg.DB = db

	reportingDB, err := resolveParam(c, src, "reporting-db-parameter", cfg.ReportingDB)
	if err != nil {
		return BackendConfig{}, err
	}

	cfg.ReportingDB = reportingDB

	sharedSecret, err := resolveParam(c, src, "mock-jwt-shared-secret-parameter", cfg.MockJWTSharedSecret)
	if err != nil {
		return BackendConfig{}, err
	}

	cfg.MockJWTSharedSecret = sharedSecret

	return cfg, nil
}

func resolveParam(
	c *cli.Context, src ParameterSource, name string, defaultValue string,
) (string, error) {
	paramName := c.String(name)
	if paramName == "" {
		return defaultValue, nil
	}

	value, err := src(c.Context, paramName)
	if err != nil {
		return "", fmt.Errorf("failed to fetch %q (%s) parameter value: %w",
			paramName, name, err)
	}

	return value, nil
}

func BackendFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    "db",
			Value:   "postgres://elephant-repository:pass@localhost/elephant-repository",
			EnvVars: []string{"CONN_STRING"},
		},
		&cli.StringFlag{
			Name:    "db-parameter",
			EnvVars: []string{"CONN_STRING_PARAMETER"},
		},
		&cli.StringFlag{
			Name:    "reporting-db",
			Value:   "postgres://repository-reportuser:pass@localhost/elephant-repository",
			EnvVars: []string{"REPORTING_CONN_STRING"},
		},
		&cli.StringFlag{
			Name:    "reporting-db-parameter",
			EnvVars: []string{"REPORTING_CONN_STRING_PARAMETER"},
		},
		&cli.StringFlag{
			Name:    "eventsink",
			Value:   "aws-eventbridge",
			EnvVars: []string{"EVENTSINK"},
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
			Name:  "no-core-schema",
			Usage: "Don't register the built in core schema",
		},
		&cli.BoolFlag{
			Name:  "no-archiver",
			Usage: "Disable the archiver",
		},
		&cli.BoolFlag{
			Name:  "no-eventsink",
			Usage: "Disable the eventsink",
		},
		&cli.BoolFlag{
			Name:  "no-reporter",
			Usage: "Disable the reporter",
		},
		&cli.BoolFlag{
			Name:  "no-replicator",
			Usage: "Disable the replicator",
		},
		&cli.BoolFlag{
			Name:    "mock-jwt-endpoint",
			Usage:   "Enable mock JWT endpoint for development use",
			EnvVars: []string{"MOCK_JWT_ENDPOINT"},
		},
		&cli.StringFlag{
			Name:    "mock-jwt-signing-key",
			Usage:   "ECDSA signing key used for mock JWTs",
			EnvVars: []string{"MOCK_JWT_SIGNING_KEY"},
		},
		&cli.StringFlag{
			Name:    "mock-jwt-shared-secret",
			Usage:   "Shared secret to be used in password grants in the mock JWT endpoint",
			EnvVars: []string{"MOCK_JWT_SHARED_PASSWORD_SECRET"},
		},
		&cli.StringFlag{
			Name:    "mock-jwt-shared-secret-parameter",
			Usage:   "Shared secret to be used in password grants in the mock JWT endpoint",
			EnvVars: []string{"MOCK_JWT_SHARED_PASSWORD_SECRET_PARAMETER"},
		},
		&cli.StringFlag{
			Name:    "jwks-url",
			Usage:   "URL of external JWKS",
			EnvVars: []string{"JWKS_URL"},
		},
		&cli.StringFlag{
			Name:    "jwt-issuer",
			Usage:   "String to validate the iss claim against",
			EnvVars: []string{"JWT_ISSUER"},
		},
		&cli.StringFlag{
			Name:    "jwt-audience",
			Usage:   "String to validate the aud claim against",
			EnvVars: []string{"JWT_AUDIENCE"},
		},
		&cli.StringFlag{
			Name:    "jwt-scope-prefix",
			Usage:   "Prefix to strip from JWT scopes",
			EnvVars: []string{"JWT_SCOPE_PREFIX"},
		},
	}
}
