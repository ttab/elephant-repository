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

	"github.com/navigacontentlab/revisor"
	"github.com/schollz/progressbar/v3"
	"github.com/ttab/docformat"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

func main() {
	app := &cli.App{
		Name:  "docformat",
		Usage: "work with document data",
		Commands: []*cli.Command{
			{
				Name:        "cca-document",
				Description: "Converts a CCA document",
				Action:      convertCCA,
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
			},
			{
				Name:        "ingest",
				Description: "Imports documents using the content log",
				Action:      ingestCommand,
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
						Name:  "ui",
						Usage: "Enable web ui",
					},
					&cli.StringFlag{
						Name:  "addr",
						Value: "127.0.0.1:1080",
					},
					&cli.BoolFlag{
						Name:  "replacements",
						Usage: "Preload replacements",
					},
				},
			},
			{
				Name:        "index",
				Description: "re-index documents",
				Action:      indexCommand,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "state-dir",
						Value: "state",
					},
				},
			},
			{
				Name:        "ui",
				Description: "Serve a web UI",
				Action:      webUICommand,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "state-dir",
						Value: "state",
					},
					&cli.StringFlag{
						Name:  "addr",
						Value: "127.0.0.1:1080",
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
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		println(err.Error())
		os.Exit(1)
	}
}

func webUICommand(c *cli.Context) error {
	var (
		stateDir = c.String("state-dir")
		addr     = c.String("addr")
		ocURL    = c.String("oc-url")
		token    = c.String("token")
	)

	docDir := filepath.Join(stateDir, "data", "documents")
	indexPath := filepath.Join(stateDir, "data", "index.bleve")

	index, err := docformat.NewSearchIndex(indexPath)
	if err != nil {
		return fmt.Errorf("failed to create search index: %w", err)
	}

	store := docformat.NewFSDocStore(docDir, index)

	client := &http.Client{
		Transport: docformat.TransportWithToken{
			Transport: http.DefaultTransport,
			Token:     token,
		},
	}

	oc, err := docformat.NewOCClient(client, ocURL)
	if err != nil {
		return fmt.Errorf("failed to create OC client: %w", err)
	}

	return docformat.ServeWebUI(c.Context, store, index, oc, addr)
}

func indexCommand(c *cli.Context) error {
	var (
		stateDir = c.String("state-dir")
	)

	docDir := filepath.Join(stateDir, "data", "documents")
	indexPath := filepath.Join(stateDir, "data", "index.bleve")

	index, err := docformat.NewSearchIndex(indexPath)
	if err != nil {
		return fmt.Errorf("failed to create search index: %w", err)
	}

	bar := progressbar.Default(-1, "indexing")
	doneChan := make(chan string)

	defer close(doneChan)

	go func() {
		for range doneChan {
			bar.Add(1)
		}

		_ = bar.Finish()
	}()

	store := docformat.NewFSDocStore(docDir, index)

	return store.ReIndex(doneChan)
}

func ingestCommand(c *cli.Context) error {
	var (
		ccaURL           = c.String("cca-url")
		ocURL            = c.String("oc-url")
		token            = c.String("token")
		stateDir         = c.String("state-dir")
		lang             = c.String("default-lang")
		startPos         = c.Int("start-pos")
		tail             = c.Bool("tail")
		ui               = c.Bool("ui")
		addr             = c.String("addr")
		loadReplacements = c.Bool("replacements")
	)

	docDir := filepath.Join(stateDir, "data", "documents")
	stateDB := filepath.Join(stateDir, "data", "state.db")
	indexPath := filepath.Join(stateDir, "data", "index.bleve")
	blocklistPath := filepath.Join(stateDir, "blocklist.txt")
	cacheDir := filepath.Join(stateDir, "cache")

	db, err := docformat.NewBadgerStore(stateDB)
	if err != nil {
		return fmt.Errorf("failed to create badger store: %w", err)
	}

	if startPos != 0 {
		err := db.SetLogPosition(startPos)
		if err != nil {
			return fmt.Errorf("failed to set starting position %d",
				err)
		}
	} else {
		pos, err := db.GetLogPosition()
		if err != nil {
			return fmt.Errorf("failed to get current position: %w",
				err)
		}

		startPos = pos
	}

	index, err := docformat.NewSearchIndex(indexPath)
	if err != nil {
		return fmt.Errorf("failed to create search index: %w", err)
	}

	client := &http.Client{
		Transport: docformat.TransportWithToken{
			Transport: http.DefaultTransport,
			Token:     token,
		},
	}

	cca, err := docformat.NewCCAlient(client, ccaURL)
	if err != nil {
		return fmt.Errorf("failed to create CCA client: %w", err)
	}

	oc, err := docformat.NewOCClient(client, ocURL)
	if err != nil {
		return fmt.Errorf("failed to create OC client: %w", err)
	}

	if loadReplacements {
		err = preloadReplacements(c.Context, oc, db)
		if err != nil {
			return err
		}
	}

	blocklist, err := docformat.BlocklistFromFile(blocklistPath)
	if err != nil {
		return fmt.Errorf("failed to load blocklist: %w", err)
	}

	validator, err := createValidator(
		"constraints/core.json",
		"constraints/tt.json",
	)
	if err != nil {
		return err
	}

	lastEvent, err := oc.GetContentLog(context.Background(), -1)
	if err != nil {
		return fmt.Errorf("failed to get last contentlog event: %w", err)
	}

	if len(lastEvent.Events) == 0 {
		return errors.New("no events in contentlog")
	}

	maxPos := lastEvent.Events[0].ID
	bar := progressbar.NewOptions(maxPos-startPos,
		progressbar.OptionSetWriter(os.Stderr))
	doneChan := make(chan docformat.ContentLogEvent)

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

			bar.Set(e.ID - startPos)
		}

		_ = bar.Finish()

		if tail {
			_ = bar.Clear()
			_ = bar.Exit()

			bar = progressbar.Default(-1, "tailing log")

			for {
				select {
				case <-doneChan:
					bar.Add(1)
				case <-time.After(200 * time.Millisecond):
					// Trigger spinner animation so that we
					// know that things still are up and
					// running.
					bar.Add(0)
				}
			}
		}
	}()

	fsCache := docformat.NewFSCache(filepath.Join(cacheDir, "documents"))
	cachedGet := docformat.WrapGetDocumentFunc(cca.GetDocument, fsCache)

	propFSCache := docformat.NewFSCache(filepath.Join(cacheDir, "properties"))
	cachedProps := docformat.NewPropertyCache(propFSCache, oc)

	opts := docformat.IngestOptions{
		DefaultLanguage: lang,
		Identity:        db,
		LogPos:          db,
		ContentLog:      oc,
		GetDocument:     cachedGet,
		Objects:         oc,
		OCProps:         cachedProps,
		DocStore:        docformat.NewFSDocStore(docDir, index),
		Blocklist:       blocklist,
		Validator:       validator,
		Done:            doneChan,
	}

	ingest := docformat.NewIngester(opts)

	ingestCtx, cancel := context.WithCancel(c.Context)

	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-interrupt
		cancel()
	}()

	var ve *docformat.ValidationError

	group, gCtx := errgroup.WithContext(ingestCtx)

	if ui {
		group.Go(func() error {
			return docformat.ServeWebUI(
				gCtx, opts.DocStore, index, oc, addr)
		})
	}

	group.Go(func() error {
		err = ingest.Start(gCtx, tail)

		if errors.As(err, &ve) {
			for i := range ve.Errors {
				println("-", ve.Errors[i].String())
			}

			data, _ := json.MarshalIndent(ve.Document, "", "  ")
			_ = os.WriteFile(filepath.Join(stateDir, "invalid_doc.json"),
				data, 0666)
		}

		if err != nil {
			return err
		}

		return nil
	})

	return group.Wait()
}

func preloadReplacements(ctx context.Context,
	oc *docformat.OCClient, db docformat.IdentityStore,
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

			bar.Add(1)
		}

		start += len(res.Hits)

		if start >= res.TotalHits {
			break
		}
	}

	return nil
}

func createValidator(paths ...string) (*revisor.Validator, error) {
	var constraints []revisor.ConstraintSet

	for _, name := range paths {
		data, err := docformat.BuiltInConstraints.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to read constraints file: %w", err)
		}

		var c revisor.ConstraintSet

		err = json.Unmarshal(data, &c)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to unmarshal %q constraints: %w",
				name, err)
		}

		constraints = append(constraints, c)
	}

	v, err := revisor.NewValidator(constraints...)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create validator with constraints: %w", err)
	}

	return v, nil
}

func convertCCA(c *cli.Context) error {
	var (
		baseURL = c.String("cca-url")
		uuid    = c.String("uuid")
		version = c.Int("version")
		token   = c.String("token")
	)

	client := &http.Client{
		Transport: docformat.TransportWithToken{
			Transport: http.DefaultTransport,
			Token:     token,
		},
	}

	cca, err := docformat.NewCCAlient(client, baseURL)
	if err != nil {
		return fmt.Errorf("failed to create CCA client: %w", err)
	}

	fsCache := docformat.NewFSCache(filepath.Join("cache", "documents"))

	cachedGet := docformat.WrapGetDocumentFunc(cca.GetDocument, fsCache)

	res, err := cachedGet(c.Context, docformat.GetDocumentRequest{
		UUID:    uuid,
		Version: version,
	})
	if err != nil {
		return fmt.Errorf("failed to fetch document through CCA: %w", err)
	}

	doc, err := docformat.ConvertNavigaDoc(res.Document)
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
