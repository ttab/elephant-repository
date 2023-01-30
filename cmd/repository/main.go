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

func main() {
	app := &cli.App{
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
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		println(err.Error())
		os.Exit(1)
	}
}

func runServer(c *cli.Context) error {
	var (
		dbURI = c.String("db")
		addr  = c.String("addr")
	)

	dbpool, err := pgxpool.New(context.Background(), dbURI)
	if err != nil {
		return fmt.Errorf("unable to create connection pool: %w", err)
	}
	defer dbpool.Close()

	store, err := docformat.NewPGDocStore(dbpool)
	if err != nil {
		return fmt.Errorf("failed to create doc store: %w", err)
	}

	validator, err := cmd.DefaultValidator()
	if err != nil {
		return err
	}

	return docformat.RunServer(c.Context, addr,
		docformat.WithAPIServer(nil, store, validator),
	)
}
