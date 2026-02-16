package cmd

import (
	"github.com/ttab/elephant-repository/repository"
	"github.com/urfave/cli/v3"
)

type BackendConfig struct {
	repository.S3Options
	DB                string
	DBBouncer         string
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

	// TolerateEventlogGaps to deal with old inconsistent data.
	TolerateEventlogGaps bool
}

func BackendConfigFromContext(c *cli.Command) (BackendConfig, error) {
	dbBouncer := c.String("db-bouncer")
	if dbBouncer == "" {
		dbBouncer = c.String("db")
	}

	cfg := BackendConfig{
		DB:                c.String("db"),
		DBBouncer:         dbBouncer,
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
		TolerateEventlogGaps: c.Bool("tolerate-eventlog-gaps"),
	}

	return cfg, nil
}
