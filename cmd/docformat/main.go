package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/ttab/elephant/ingest"
	"github.com/ttab/elephant/rpc/repository"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
)

func main() {
	convertCCACommand := cli.Command{
		Name:        "cca-document",
		Description: "Converts a CCA document",
		Action:      convertCcaAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "url",
				Value: "https://cca-eu-west-1.saas-prod.infomaker.io",
			},
			&cli.StringFlag{
				Name:     "uuid",
				Required: true,
			},
			&cli.IntFlag{
				Name: "version",
			},
			&cli.StringFlag{
				Name:     "token",
				Required: true,
				EnvVars: []string{
					"NAVIGA_BEARER_TOKEN",
				},
			},
			&cli.StringFlag{
				Name:  "state-dir",
				Value: "state",
			},
		},
	}

	loadSchema := cli.Command{
		Name:        "load-schema",
		Description: "Load a revisor schema into the repository",
		Action:      loadSchemaAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "repository",
				Value: "http://localhost:1080",
			},
			&cli.PathFlag{
				Name:  "schema",
				Value: "revisor/constraints/tt.json",
			},
			&cli.StringFlag{
				Name:  "name",
				Value: "tt",
			},
			&cli.StringFlag{
				Name:  "version",
				Value: "v1.0.0",
			},
			&cli.BoolFlag{
				Name:  "activate",
				Value: true,
			},
		},
	}

	updateStatus := cli.Command{
		Name:        "update-status",
		Description: "Update status",
		Action:      updateStatusAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "repository",
				Value: "http://localhost:1080",
			},
			&cli.PathFlag{
				Name:  "name",
				Value: "usable",
			},
			&cli.BoolFlag{
				Name: "disable",
			},
		},
	}

	ingestCommand := cli.Command{
		Name:        "ingest",
		Description: "Imports documents using the content log",
		Action:      ingestAction,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "cca-url",
				Value: "https://cca-eu-west-1.saas-stage.infomaker.io",
			},
			&cli.StringFlag{
				Name:  "oc-url",
				Value: "https://xlibris.editorial.stage.oc.tt.infomaker.io:7777",
			},
			&cli.StringFlag{
				Name:     "token",
				Required: true,
				EnvVars: []string{
					"NAVIGA_BEARER_TOKEN",
				},
			},
			&cli.StringFlag{
				Name:  "repository",
				Value: "http://localhost:1080",
			},
			&cli.StringFlag{
				Name:  "state-dir",
				Value: "state",
			},
			&cli.StringFlag{
				Name:  "default-lang",
				Value: "sv",
			},
			&cli.IntFlag{
				Name: "start-pos",
			},
			&cli.BoolFlag{
				Name:    "tail",
				Aliases: []string{"f"},
			},
			&cli.BoolFlag{
				Name:  "replacements",
				Usage: "Preload replacements",
			},
		},
	}

	app := &cli.App{
		Name:  "docformat",
		Usage: "work with document data",
		Commands: []*cli.Command{
			&convertCCACommand,
			&ingestCommand,
			&loadSchema,
			&updateStatus,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func updateStatusAction(c *cli.Context) error {
	var (
		repositoryURL = c.String("repository")
		name          = c.String("name")
		disable       = c.Bool("disable")
	)

	authConf := oauth2.Config{
		Endpoint: oauth2.Endpoint{
			TokenURL: repositoryURL + "/token",
		},
		Scopes: []string{
			"workflow_admin",
		},
	}

	repoToken, err := authConf.PasswordCredentialsToken(c.Context,
		"OC Importer <system://oc-importer>", "")
	if err != nil {
		return fmt.Errorf("failed to get access token: %w", err)
	}

	schemaClient := repository.NewWorkflowsProtobufClient(repositoryURL,
		authConf.Client(c.Context, repoToken))

	_, err = schemaClient.UpdateStatus(c.Context,
		&repository.UpdateStatusRequest{
			Name:     name,
			Disabled: disable,
		})
	if err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	return nil
}

func loadSchemaAction(c *cli.Context) error {
	var (
		repositoryURL = c.String("repository")
		schema        = c.Path("schema")
		name          = c.String("name")
		version       = c.String("version")
		activate      = c.Bool("activate")
	)

	data, err := os.ReadFile(schema)
	if err != nil {
		return fmt.Errorf("failed to read schema file: %w", err)
	}

	authConf := oauth2.Config{
		Endpoint: oauth2.Endpoint{
			TokenURL: repositoryURL + "/token",
		},
		Scopes: []string{
			"schema_admin",
		},
	}

	repoToken, err := authConf.PasswordCredentialsToken(c.Context,
		"OC Importer <system://oc-importer>", "")
	if err != nil {
		return fmt.Errorf("failed to get access token: %w", err)
	}

	schemaClient := repository.NewSchemasProtobufClient(repositoryURL,
		authConf.Client(c.Context, repoToken))

	_, err = schemaClient.Register(c.Context,
		&repository.RegisterSchemaRequest{
			Schema: &repository.Schema{
				Name:    name,
				Version: version,
				Spec:    string(data),
			},
			Activate: activate,
		})
	if err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}

	return nil
}

func ingestAction(c *cli.Context) error {
	var (
		ccaURL           = c.String("cca-url")
		ocURL            = c.String("oc-url")
		token            = c.String("token")
		repositoryURL    = c.String("repository")
		stateDir         = c.String("state-dir")
		lang             = c.String("default-lang")
		startPos         = c.Int("start-pos")
		tail             = c.Bool("tail")
		loadReplacements = c.Bool("replacements")
	)

	stateDB := filepath.Join(stateDir, "data", "state.db")
	blocklistPath := filepath.Join(stateDir, "blocklist.txt")
	cacheDir := filepath.Join(stateDir, "cache")

	logger := slog.New(slog.NewTextHandler(os.Stderr))

	db, err := ingest.NewBadgerStore(stateDB)
	if err != nil {
		return fmt.Errorf("failed to create badger store: %w", err)
	}

	if startPos != 0 {
		err := db.SetLogPosition(startPos)
		if err != nil {
			return fmt.Errorf("failed to set starting position %d: %w",
				startPos, err)
		}
	} else {
		pos, err := db.GetLogPosition()
		if err != nil {
			return fmt.Errorf("failed to get current position: %w",
				err)
		}

		startPos = pos
	}

	client := &http.Client{
		Transport: ingest.TransportWithToken{
			Transport: http.DefaultTransport,
			Token:     token,
		},
	}

	cca, err := ingest.NewCCAlient(client, ccaURL)
	if err != nil {
		return fmt.Errorf("failed to create CCA client: %w", err)
	}

	oc, err := ingest.NewOCClient(client, ocURL)
	if err != nil {
		return fmt.Errorf("failed to create OC client: %w", err)
	}

	if loadReplacements {
		err = preloadReplacements(c.Context, oc, db)
		if err != nil {
			return err
		}
	}

	blocklist, err := ingest.BlocklistFromFile(blocklistPath)
	if err != nil {
		return fmt.Errorf("failed to load blocklist: %w", err)
	}

	lastEvent, err := oc.GetEventLog(context.Background(), -1)
	if err != nil {
		return fmt.Errorf("failed to get last contentlog event: %w", err)
	}

	if len(lastEvent.Events) == 0 {
		return errors.New("no events in contentlog")
	}

	maxPos := lastEvent.Events[0].ID
	bar := progressbar.NewOptions(maxPos-startPos,
		progressbar.OptionSetWriter(os.Stderr))
	doneChan := make(chan ingest.OCLogEvent)

	go func() {
		for e := range doneChan {
			if startPos < 0 {
				startPos = e.ID
				bar.ChangeMax(maxPos - startPos)
			}

			bar.Describe(fmt.Sprintf(
				"%08d %s %10s",
				e.ID, e.UUID[0:8], e.Content.ContentType))

			if e.ID >= maxPos {
				break
			}

			_ = bar.Set(e.ID - startPos)
		}

		_ = bar.Finish()

		if tail {
			_ = bar.Clear()
			_ = bar.Exit()

			bar = progressbar.Default(-1, "tailing log")

			for {
				select {
				case <-doneChan:
					_ = bar.Add(1)
				case <-time.After(200 * time.Millisecond):
					// Trigger spinner animation so that we
					// know that things still are up and
					// running.
					_ = bar.Add(0)
				}
			}
		}
	}()

	fsCache := ingest.NewFSCache(filepath.Join(cacheDir, "documents"))
	cachedGet := ingest.WrapGetDocumentFunc(cca.GetDocument, fsCache)

	propFSCache := ingest.NewFSCache(filepath.Join(cacheDir, "properties"))
	cachedProps := ingest.NewPropertyCache(propFSCache, oc)

	objectFSCache := ingest.NewFSCache(filepath.Join(cacheDir, "oc_objects"))
	cachedObjectGet := ingest.NewObjectCache(logger, objectFSCache, oc)

	authConf := oauth2.Config{
		Endpoint: oauth2.Endpoint{
			TokenURL: repositoryURL + "/token",
		},
		Scopes: []string{
			"doc_write", "doc_delete",
			"import_directive", "superuser",
		},
	}

	repoToken, err := authConf.PasswordCredentialsToken(c.Context,
		"OC Importer <system://oc-importer>", "")
	if err != nil {
		return fmt.Errorf("failed to get access token: %w", err)
	}

	repoClient := repository.NewDocumentsProtobufClient(repositoryURL,
		authConf.Client(c.Context, repoToken))

	opts := ingest.Options{
		Logger:          logger,
		DefaultLanguage: lang,
		Identity:        db,
		LogPos:          db,
		OCLog:           oc,
		GetDocument:     cachedGet,
		Objects:         cachedObjectGet,
		OCProps:         cachedProps,
		API:             repoClient,
		Blocklist:       blocklist,
		Done:            doneChan,
	}

	ingester := ingest.NewIngester(opts)

	ingestCtx, cancel := context.WithCancel(c.Context)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-interrupt
		cancel()
	}()

	group, gCtx := errgroup.WithContext(ingestCtx)

	group.Go(func() error {
		var ve *ingest.ValidationError

		err = ingester.Start(gCtx, tail)

		if errors.As(err, &ve) {
			for i := range ve.Errors {
				fmt.Fprintf(os.Stderr, "-%s\n", ve.Errors[i])
			}

			data, _ := json.MarshalIndent(ve.Document, "", "  ")

			_ = os.WriteFile(filepath.Join(stateDir, "invalid_doc.json"),
				data, 0600)
		}

		if err != nil {
			return err //nolint:wrapcheck
		}

		// Cancel ingest context when we're done, that will shut down
		// the server as well.
		cancel()

		return nil
	})

	return group.Wait() //nolint:wrapcheck
}

func preloadReplacements(ctx context.Context,
	oc *ingest.OCClient, db ingest.IdentityStore,
) error {
	var (
		start int
		bar   *progressbar.ProgressBar
	)

	for {
		res, err := oc.Search(ctx, "ArticleCopySource:*",
			[]string{"uuid", "ArticleCopySource"}, start, 1000)
		if err != nil {
			return fmt.Errorf(
				"failed to query for article sources: %w", err)
		}

		if bar == nil {
			bar = progressbar.NewOptions(res.TotalHits,
				progressbar.OptionSetWriter(os.Stderr),
				progressbar.OptionSetDescription("preloading replacements"))
			defer func() {
				_ = bar.Finish()
			}()
		}

		for _, hit := range res.Hits {
			if len(hit.Properties["ArticleCopySource"]) != 1 {
				continue
			}

			oldUUID := hit.Properties["ArticleCopySource"][0]

			err = db.RegisterContinuation(oldUUID, hit.UUID)
			if err != nil {
				return fmt.Errorf(
					"failed to register continuation for %q to %q: %w",
					oldUUID, hit.UUID, err)
			}

			_ = bar.Add(1)
		}

		start += len(res.Hits)

		if start >= res.TotalHits {
			break
		}
	}

	return nil
}

func convertCcaAction(c *cli.Context) error {
	var (
		baseURL = c.String("cca-url")
		uuid    = c.String("uuid")
		version = c.Int("version")
		token   = c.String("token")
	)

	client := &http.Client{
		Transport: ingest.TransportWithToken{
			Transport: http.DefaultTransport,
			Token:     token,
		},
	}

	cca, err := ingest.NewCCAlient(client, baseURL)
	if err != nil {
		return fmt.Errorf("failed to create CCA client: %w", err)
	}

	fsCache := ingest.NewFSCache(filepath.Join("cache", "documents"))

	cachedGet := ingest.WrapGetDocumentFunc(cca.GetDocument, fsCache)

	res, err := cachedGet(c.Context, ingest.GetDocumentRequest{
		UUID:    uuid,
		Version: version,
	})
	if err != nil {
		return fmt.Errorf("failed to fetch document through CCA: %w", err)
	}

	doc, err := ingest.ConvertNavigaDoc(res.Document)
	if err != nil {
		return fmt.Errorf("failed to convert document: %w", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	err = enc.Encode(doc)
	if err != nil {
		return fmt.Errorf("failed to write document to stdout: %w",
			err)
	}

	return nil
}
