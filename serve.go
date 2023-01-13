package docformat

import (
	"bytes"
	"context"
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

	"github.com/julienschmidt/httprouter"
	"github.com/ttab/docformat/rpc/elephant"
	"github.com/twitchtv/twirp"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed assets/*
var assetFS embed.FS

func LinkedDocs(b Block) []Block {
	return linkedDocs(b, nil)
}

func linkedDocs(b Block, links []Block) []Block {
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

func RunServer(
	ctx context.Context,
	store DocStore, index *SearchIndex, oc *OCClient, addr string,
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

	router := httprouter.New()

	server := http.Server{
		Addr:    addr,
		Handler: router,
	}

	api := elephant.NewDocumentsServer(
		&APIServer{
			store: store,
		},
		twirp.WithServerJSONSkipDefaults(true),
	)

	router.POST(api.PathPrefix()+":method", func(
		w http.ResponseWriter, r *http.Request, _ httprouter.Params,
	) {
		api.ServeHTTP(w, r)
	})

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

		query := data.Query
		if query == "" {
			query = "title:*"
		}

		hits, err := index.Search(query)
		if err != nil {
			templates.ExecuteTemplate(w, "error.html", ErrorPageData{
				Error: err.Error(),
			})
			return
		}

		data.Hits = hits

		templates.ExecuteTemplate(w, "index.html", data)
	})

	router.GET("/document/:uuid/", func(
		w http.ResponseWriter, r *http.Request, ps httprouter.Params,
	) {
		uuid := ps.ByName("uuid")

		data := DocumentPageData{
			Title: fmt.Sprintf("Document %s", uuid),
			UUID:  uuid,
		}

		fields, err := index.Fields(data.UUID)
		if err != nil {
			templates.ExecuteTemplate(w, "error.html", ErrorPageData{
				Error: err.Error(),
			})
			return
		}

		data.Fields = fields

		doc, err := store.GetDocumentMeta(r.Context(), data.UUID)
		if err != nil {
			templates.ExecuteTemplate(w, "error.html", ErrorPageData{
				Error: err.Error(),
			})
			return
		}

		data.Meta = *doc

		templates.ExecuteTemplate(w, "document.html", data)
	})

	router.GET("/document/:uuid/:version/", func(
		w http.ResponseWriter, r *http.Request, ps httprouter.Params,
	) {
		data := VersionPageData{
			UUID: ps.ByName("uuid"),
		}

		version, err := strconv.Atoi(ps.ByName("version"))
		if err != nil {
			templates.ExecuteTemplate(w, "error.html", ErrorPageData{
				Error: fmt.Sprintf("invalid version number: %v", err),
			})
			return
		}

		data.Version = version

		doc, err := store.GetDocument(r.Context(),
			data.UUID, data.Version)
		if err != nil {
			templates.ExecuteTemplate(w, "error.html", ErrorPageData{
				Error: err.Error(),
			})
			return
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

		templates.ExecuteTemplate(w, "version.html", data)
	})

	router.GET("/document/:uuid/:version/document", func(
		w http.ResponseWriter, r *http.Request, ps httprouter.Params,
	) {
		uuid := ps.ByName("uuid")

		version, err := strconv.Atoi(ps.ByName("version"))
		if err != nil {
			templates.ExecuteTemplate(w, "error.html", ErrorPageData{
				Error: fmt.Sprintf("invalid version number: %v", err),
			})
			return
		}

		doc, err := store.GetDocument(r.Context(), uuid, version)
		if err != nil {
			templates.ExecuteTemplate(w, "error.html", ErrorPageData{
				Error: fmt.Sprintf("failed to get document: %v", err),
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")

		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")

		_ = enc.Encode(doc)
	})

	router.GET("/document/:uuid/:version/newsml", func(
		w http.ResponseWriter, r *http.Request, ps httprouter.Params,
	) {
		uuid := ps.ByName("uuid")

		version, err := strconv.Atoi(ps.ByName("version"))
		if err != nil {
			templates.ExecuteTemplate(w, "error.html", ErrorPageData{
				Error: fmt.Sprintf("invalid version number: %v", err),
			})
			return
		}

		meta, err := store.GetDocumentMeta(ctx, uuid)
		if err != nil {
			templates.ExecuteTemplate(w, "error.html", ErrorPageData{
				Error: fmt.Sprintf("failed to load metadata: %v", err),
			})
			return
		}

		var (
			ocUUID    string
			ocVersion int
		)

		for _, u := range meta.Updates {
			if u.Version != version {
				continue
			}

			for _, m := range u.Meta {
				switch m.Key {
				case "oc-source":
					ocUUID = m.Value
				case "oc-version":
					ocVersion, _ = strconv.Atoi(m.Value)

				}
			}
		}

		resp, err := oc.GetRawObject(r.Context(), ocUUID, ocVersion)
		if err != nil {
			templates.ExecuteTemplate(w, "error.html", ErrorPageData{
				Error: fmt.Sprintf("failed to load NewsML: %v", err),
			})
			return
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			templates.ExecuteTemplate(w, "error.html", ErrorPageData{
				Error: fmt.Sprintf("OC responded with: %s",
					resp.Status),
			})
			return
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
	})

	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()

	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	} else if err != nil {
		return err
	}

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
		Byline:  getData(b.Data, "byline"),
		Content: c.Children,
	}

	var buf bytes.Buffer

	_ = tpl.Execute(&buf, data)

	return template.HTML(buf.String())
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
		Caption: getData(b.Data, "caption"),
	}

	var buf bytes.Buffer

	_ = tpl.Execute(&buf, data)

	return template.HTML(buf.String())
}

func tagWrap(
	tag string, className ...string,
) func(c RenderedContent) template.HTML {
	var class string

	if len(className) > 0 {
		class = fmt.Sprintf(`class="%s"`, strings.Join(className, " "))
	}

	return func(c RenderedContent) template.HTML {
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
	Hits  []SearchHit
}

type DocumentPageData struct {
	Title  string
	UUID   string
	Meta   DocumentMeta
	Fields map[string][]string
}

type VersionPageData struct {
	UUID            string
	Version         int
	Document        Document
	RenderedContent []RenderedContent
}

type RenderedContent struct {
	Rendered template.HTML
	Block    Block
	Children []RenderedContent
}
