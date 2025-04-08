package cmd

import (
	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine"
	"github.com/urfave/cli/v2"
)

type BackendConfig struct {
	repository.S3Options
	DB                string
	Eventsink         string
	ArchiveBucket     string
	AssetBucket       string
	S3Endpoint        string
	S3KeyID           string
	S3KeySecret       string
	S3Insecure        bool
	NoArchiver        bool
	ArchiverCount     int
	NoCoreSchema      bool
	NoEventlogBuilder bool
	NoEventsink       bool
	NoReporter        bool
	NoScheduler       bool
	JWTAudience       string
	JWTScopePrefix    string
}

func BackendConfigFromContext(c *cli.Context, src elephantine.ParameterSource) (BackendConfig, error) {
	cfg := BackendConfig{
		Eventsink:         c.String("eventsink"),
		ArchiveBucket:     c.String("archive-bucket"),
		AssetBucket:       c.String("asset-bucket"),
		NoArchiver:        c.Bool("no-archiver"),
		NoEventsink:       c.Bool("no-eventsink"),
		NoEventlogBuilder: c.Bool("no-eventlog-builder"),
		NoScheduler:       c.Bool("no-scheduler"),
		JWTAudience:       c.String("jwt-audience"),
		JWTScopePrefix:    c.String("jwt-scope-prefix"),
		S3Options: repository.S3Options{
			Endpoint:        c.String("s3-endpoint"),
			AccessKeyID:     c.String("s3-key-id"),
			AccessKeySecret: c.String("s3-key-secret"),
		},
	}

	db, err := elephantine.ResolveParameter(c.Context, c, src, "db")
	if err != nil {
		return BackendConfig{}, err //nolint: wrapcheck
	}

	cfg.DB = db

	return cfg, nil
}
