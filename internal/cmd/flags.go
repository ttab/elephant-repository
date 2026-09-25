package cmd

import (
	"github.com/ttab/elephant-repository/repository"
	"github.com/urfave/cli/v3"
)

// DefaultDBMaxConns is the default size of the pool the repository runs its
// queries on: the direct pool when no bouncer is configured, the bouncer pool
// when one is. It does not size the direct pubsub pool behind a bouncer, which
// pg.NewPools pins at pg.DefaultPubSubMaxConns.
//
// It is set here rather than left to pgx, whose default is max(4, NumCPU())
// read from the node's cpuset rather than the cgroup quota, so an unset pool
// tracks whichever node the pod lands on and changes size invisibly on
// reschedule.
//
// The number comes from the background workers that each hold one connection
// at a time: the four archiver loops (poll, eventlog, eventlog batch,
// generation), the eventlog builder, the event forwarder, the scheduler and
// the doc store cleaner. On the instance holding the job locks all eight can
// be checked out at once — the archiver's delete loop holds its transaction
// across the S3 moves — while no RPC is running. The other eight are for RPCs, which run short queries
// but fan out to two concurrent ones on bulk reads and eventlog enrichment, so
// eight leaves room for a burst of four such calls, or eight single-query ones,
// on top of the background work. Trim or raise it once
// pgxpool_empty_acquire_wait_seconds_total says what it actually needs.
const DefaultDBMaxConns = 16

type BackendConfig struct {
	repository.S3Options
	DB                string
	DBBouncer         string
	DBMaxConns        int
	Eventsink         string
	ArchiveBucket     string
	AssetBucket       string
	S3Endpoint        string
	S3KeyID           string
	S3KeySecret       string
	S3Insecure        bool
	NoArchiver        bool
	ArchiverCount     int
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
		DBMaxConns:        c.Int("db-max-conns"),
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
