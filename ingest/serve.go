package ingest

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
	"github.com/ttab/elephant/doc"
	"github.com/ttab/elephant/internal"
	"github.com/ttab/elephant/repository"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed assets/*
var assetFS embed.FS

func LinkedDocs(b doc.Block) []doc.Block {
	return linkedDocs(b, nil)
}

func linkedDocs(b doc.Block, links []doc.Block) []doc.Block {
	if b.UUID != "" {
		links = append(links, b)
	}

	for _, c := range b.Links {
		links = linkedDocs(c, links)
	}

	for _, c := range b.Meta {
		links = linkedDocs(c, links)
	}

	for _, c := range b.Content {
		links = linkedDocs(c, links)
	}

	return links
}

func WithUIServer(store repository.DocStore, oc *OCClient) repository.RouterOption {
	return func(router *httprouter.Router) error {
		return uiServer(router, store, oc)
	}
}

func uiServer(
	router *httprouter.Router,
	store repository.DocStore, oc *OCClient,
) error {
	tFuncs := template.New("").Funcs(template.FuncMap{
		"json": func(o any) string {
			data, err := json.MarshalIndent(o, "", "  ")
			if err != nil {
				return err.Error()
			}

			return string(data)
		},
		"linked": LinkedDocs,
	})

	templates, err := tFuncs.ParseFS(templateFS, "**/*.html")
	if err != nil {
		return fmt.Errorf("failed to parse templates: %w", err)
	}

	errWrap := func(fn func(
		w http.ResponseWriter, r *http.Request, p httprouter.Params,
	) error) httprouter.Handle {
		return func(
			w http.ResponseWriter, r *http.Request, p httprouter.Params,
		) {
			err := fn(w, r, p)
			if err != nil {
				_ = templates.ExecuteTemplate(w, "error.html",
					ErrorPageData{
						Error: err.Error(),
					})
			}
		}
	}

	assetDir, err := fs.Sub(assetFS, "assets")
	if err != nil {
		return fmt.Errorf("failed to prepare asset filesystem: %w", err)
	}

	router.ServeFiles("/assets/*filepath", http.FS(assetDir))

	router.GET("/", func(
		w http.ResponseWriter, r *http.Request, _ httprouter.Params,
	) {
		params := r.URL.Query()

		data := IndexPageData{
			Query: params.Get("q"),
		}

		if data.Query == "" {
			data.Query = "title:*"
		}

		_ = templates.ExecuteTemplate(w, "index.html", data)
	})

	router.GET("/document/:uuid/", errWrap(func(
		w http.ResponseWriter, r *http.Request, ps httprouter.Params,
	) error {
		uuid, err := uuid.Parse(ps.ByName("uuid"))
		if err != nil {
			return fmt.Errorf("failed to parse uuid: %w", err)
		}

		data := DocumentPageData{
			Title: fmt.Sprintf("Document %s", uuid),
			UUID:  uuid.String(),
		}

		doc, err := store.GetDocumentMeta(r.Context(), uuid)
		if err != nil {
			return fmt.Errorf("failed to fetch document meta: %w", err)
		}

		data.Meta = *doc

		_ = templates.ExecuteTemplate(w, "document.html", data)

		return nil
	}))

	router.GET("/document/:uuid/:version/", errWrap(func(
		w http.ResponseWriter, r *http.Request, ps httprouter.Params,
	) error {
		uuid, err := uuid.Parse(ps.ByName("uuid"))
		if err != nil {
			return fmt.Errorf("failed to parse uuid: %w", err)
		}

		data := VersionPageData{
			UUID: uuid.String(),
		}

		version, err := strconv.ParseInt(ps.ByName("version"), 10, 64)
		if err != nil {
			return fmt.Errorf(
				"invalid version number: %w", err)
		}

		data.Version = version

		doc, err := store.GetDocument(r.Context(),
			uuid, data.Version)
		if err != nil {
			return fmt.Errorf("failed to load document: %w", err)
		}

		data.Document = *doc

		renderers := map[string]func(c RenderedContent) template.HTML{
			"core/heading-1":  tagWrap("h1"),
			"core/heading-2":  tagWrap("h1"),
			"core/heading-3":  tagWrap("h1"),
			"core/heading-4":  tagWrap("h1"),
			"core/paragraph":  tagWrap("p"),
			"tt/dateline":     tagWrap("span", "dateline"),
			"core/preamble":   tagWrap("p", "ingress"),
			"core/blockquote": tagWrap("blockquote"),
			"tt/visual":       renderTTVisual,
			"core/factbox":    renderFactbox,
		}

		for _, b := range doc.Content {
			c := RenderedContent{
				Block: b,
			}

			for _, childBlock := range b.Content {
				cc := RenderedContent{
					Block: childBlock,
				}

				renderer, ok := renderers[childBlock.Type]
				if ok {
					cc.Rendered = renderer(cc)
				}

				c.Children = append(c.Children, cc)
			}

			renderer, ok := renderers[b.Type]
			if ok {
				c.Rendered = renderer(c)
			}

			data.RenderedContent = append(data.RenderedContent, c)
		}

		_ = templates.ExecuteTemplate(w, "version.html", data)

		return nil
	}))

	router.GET("/document/:uuid/:version/document", errWrap(func(
		w http.ResponseWriter, r *http.Request, ps httprouter.Params,
	) error {
		uuid, err := uuid.Parse(ps.ByName("uuid"))
		if err != nil {
			return fmt.Errorf("failed to parse UUID: %w", err)
		}

		version, err := strconv.ParseInt(ps.ByName("version"), 10, 64)
		if err != nil {
			return fmt.Errorf(
				"invalid version number: %w", err)
		}

		doc, err := store.GetDocument(r.Context(), uuid, version)
		if err != nil {
			return fmt.Errorf(
				"failed to get document: %w", err)
		}

		w.Header().Set("Content-Type", "application/json")

		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")

		_ = enc.Encode(doc)

		return nil
	}))

	router.GET("/document/:uuid/:version/newsml", errWrap(func(
		w http.ResponseWriter, r *http.Request, ps httprouter.Params,
	) error {
		uuid, err := uuid.Parse(ps.ByName("uuid"))
		if err != nil {
			return fmt.Errorf("failed to parse UUID: %w", err)
		}

		version, err := strconv.ParseInt(ps.ByName("version"), 10, 64)
		if err != nil {
			return fmt.Errorf(
				"invalid version number: %w", err)
		}

		vInfo, err := store.GetVersion(r.Context(), uuid, version)
		if err != nil {
			return fmt.Errorf(
				"failed to get version information: %w", err)
		}

		if vInfo.Meta == nil || vInfo.Meta["oc-source"] == "" {
			return errors.New("not a OC document")
		}

		ocUUID := vInfo.Meta["oc-source"]

		var ocVersion int
		if ocv, ok := vInfo.Meta["oc-version"]; ok {
			ocVersion, _ = strconv.Atoi(ocv)
		}

		resp, err := oc.GetRawObject(r.Context(), ocUUID, ocVersion)
		if err != nil {
			return fmt.Errorf(
				"failed to load NewsML: %w", err)
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf(
				"OC responded with: %s", resp.Status)
		}

		transfer := []string{
			"X-Opencontent-Object-Version",
			"Content-Length",
			"Etag",
		}

		for _, key := range transfer {
			v := resp.Header.Get(key)
			if v == "" {
				continue
			}

			w.Header().Add(key, v)
		}

		w.Header().Add("Content-Type", "text/xml")

		_, _ = io.Copy(w, resp.Body)

		return nil
	}))

	return nil
}

func renderFactbox(
	c RenderedContent,
) template.HTML {
	b := c.Block

	tpl, err := template.New("ttvisual").Parse(`
<div class="factbox">
  <h4>{{.Title}}</h4>
  <div class="byline">{{.Byline}}</div>
  <div class="contents">
  {{range .Content}}
  {{.Rendered}}
  {{end}}
  </div>
</div>
`)
	if err != nil {
		panic(err)
	}

	data := struct {
		Title   string
		Byline  string
		Content []RenderedContent
	}{
		Title:   b.Title,
		Byline:  internal.GetData(b.Data, "byline"),
		Content: c.Children,
	}

	var buf bytes.Buffer

	_ = tpl.Execute(&buf, data)

	return template.HTML(buf.String()) //nolint:gosec
}

func renderTTVisual(c RenderedContent) template.HTML {
	b := c.Block

	tpl, err := template.New("ttvisual").Parse(`
<figure>
<img src={{.Src}}/>
<figcaption>{{.Caption}}</figcaption>
</figure>
`)
	if err != nil {
		panic(err)
	}

	values := make(url.Values)

	values.Set("w", "750")
	values.Set("q", "75")

	for _, l := range b.Links {
		if l.Rel != "self" {
			continue
		}

		values.Set("url", strings.Replace(l.URL,
			"_NormalPreview", "_WatermarkPreview", 1))
	}

	imageSiteURL := url.URL{
		Scheme:   "https",
		Host:     "tt.se",
		Path:     "/bild/_next/image",
		RawQuery: values.Encode(),
	}

	data := struct {
		Src     string
		Caption string
	}{
		Src:     imageSiteURL.String(),
		Caption: internal.GetData(b.Data, "caption"),
	}

	var buf bytes.Buffer

	_ = tpl.Execute(&buf, data)

	return template.HTML(buf.String()) //nolint:gosec
}

func tagWrap(
	tag string, className ...string,
) func(c RenderedContent) template.HTML {
	var class string

	if len(className) > 0 {
		class = fmt.Sprintf(`class="%s"`, strings.Join(className, " "))
	}

	return func(c RenderedContent) template.HTML {
		//nolint:gosec
		return template.HTML(fmt.Sprintf(
			"<%[1]s %[2]s>%[3]s</%[1]s>",
			tag, class, c.Block.Data["text"]))
	}
}

type ErrorPageData struct {
	Error string
}

type IndexPageData struct {
	Query string
}

type DocumentPageData struct {
	Title  string
	UUID   string
	Meta   repository.DocumentMeta
	Fields map[string][]string
}

type VersionPageData struct {
	UUID            string
	Version         int64
	Document        doc.Document
	RenderedContent []RenderedContent
}

type RenderedContent struct {
	Rendered template.HTML
	Block    doc.Block
	Children []RenderedContent
}
