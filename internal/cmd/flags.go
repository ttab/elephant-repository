package cmd

import (
	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine"
	"github.com/urfave/cli/v2"
)

type BackendConfig struct {
	repository.S3Options
	DB             string
	ReportingDB    string
	Eventsink      string
	ArchiveBucket  string
	ReportBucket   string
	S3Endpoint     string
	S3KeyID        string
	S3KeySecret    string
	S3Insecure     bool
	NoArchiver     bool
	ArchiverCount  int
	NoCoreSchema   bool
	NoReplicator   bool
	NoEventsink    bool
	NoReporter     bool
	JWTAudience    string
	JWTScopePrefix string
}

func BackendConfigFromContext(c *cli.Context, src elephantine.ParameterSource) (BackendConfig, error) {
	cfg := BackendConfig{
		Eventsink:      c.String("eventsink"),
		ArchiveBucket:  c.String("archive-bucket"),
		ReportBucket:   c.String("report-bucket"),
		NoArchiver:     c.Bool("no-archiver"),
		NoEventsink:    c.Bool("no-eventsink"),
		NoReporter:     c.Bool("no-reporter"),
		NoReplicator:   c.Bool("no-replicator"),
		JWTAudience:    c.String("jwt-audience"),
		JWTScopePrefix: c.String("jwt-scope-prefix"),
		S3Options: repository.S3Options{
			Endpoint:        c.String("s3-endpoint"),
			AccessKeyID:     c.String("s3-key-id"),
			AccessKeySecret: c.String("s3-key-secret"),
			DisableHTTPS:    c.Bool("s3-insecure"),
		},
	}

	db, err := elephantine.ResolveParameter(c.Context, c, src, "db")
	if err != nil {
		return BackendConfig{}, err //nolint: wrapcheck
	}

	cfg.DB = db

	reportingDB, err := elephantine.ResolveParameter(c.Context, c, src, "reporting-db")
	if err != nil {
		return BackendConfig{}, err //nolint: wrapcheck
	}

	cfg.ReportingDB = reportingDB

	return cfg, nil
}
