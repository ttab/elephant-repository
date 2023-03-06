package ingest

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	navigadoc "github.com/navigacontentlab/navigadoc/doc"
	"github.com/ttab/elephant/doc"
	"github.com/ttab/elephant/internal"
	"github.com/ttab/elephant/repository"
	"github.com/ttab/elephant/revisor"
	rpc "github.com/ttab/elephant/rpc/repository"
	"golang.org/x/exp/slog"
)

type OCLogGetter interface {
	GetContentLog(ctx context.Context, lastEvent int) (*OCLogResponse, error)
	GetEventLog(ctx context.Context, lastEvent int) (*OCLogResponse, error)
}

type PropertyGetter interface {
	GetProperties(
		ctx context.Context, uuid string, version int, props []string,
	) (map[string][]string, error)
}

type ObjectGetter interface {
	GetObject(
		ctx context.Context, uuid string, version int, o any,
	) (http.Header, error)
}

type WriteAPI interface {
	Update(
		ctx context.Context, req *rpc.UpdateRequest,
	) (*rpc.UpdateResponse, error)
	Delete(
		ctx context.Context, req *rpc.DeleteDocumentRequest,
	) (*rpc.DeleteDocumentResponse, error)
}

type Options struct {
	Logger          *slog.Logger
	DefaultLanguage string
	Identity        IdentityStore
	LogPos          LogPosStore
	OCLog           OCLogGetter
	GetDocument     GetDocumentFunc
	Objects         ObjectGetter
	OCProps         PropertyGetter
	API             WriteAPI
	Blocklist       *Blocklist
	Done            chan OCLogEvent
}

type ValidationError struct {
	Document doc.Document
	Errors   []revisor.ValidationResult
}

func (ve ValidationError) Error() string {
	var causeMsg string

	if len(ve.Errors) > 0 {
		causeMsg = ": " + ve.Errors[0].String()
	}

	return fmt.Sprintf("document has %d validation errors %s",
		len(ve.Errors), causeMsg)
}

type Ingester struct {
	opt Options

	AsyncError func(_ context.Context, err error)
}

func NewIngester(opt Options) *Ingester {
	return &Ingester{
		opt: opt,
		AsyncError: func(_ context.Context, err error) {
			log.Println(err.Error())
		},
	}
}

type includeCheckerFunc func(
	ctx context.Context, evt OCLogEvent,
) (bool, OCEventType, error)

func (in *Ingester) Start(ctx context.Context, tail bool) error {
	pos, err := in.opt.LogPos.GetLogPosition()
	if err != nil {
		return fmt.Errorf("failed to read log position: %w", err)
	}

	fmt.Fprintf(os.Stdout, "starting at %d\n",
		pos)

	for {
		newPos, err := in.iteration(ctx, pos)
		if err != nil {
			return err
		}

		delay := 1 * time.Millisecond

		if newPos == pos {
			if !tail {
				break
			}

			delay = 5 * time.Second
		}

		pos = newPos

		select {
		case <-ctx.Done():
			return ctx.Err() //nolint:wrapcheck
		case <-time.After(delay):
		}
	}

	return nil
}

func (in *Ingester) iteration(ctx context.Context, pos int) (int, error) {
	log, err := in.opt.OCLog.GetEventLog(ctx, pos)
	if err != nil {
		return 0, fmt.Errorf("failed to read content log: %w", err)
	}

	ctx = repository.SetAuthInfo(ctx, &repository.AuthInfo{
		Claims: repository.JWTClaims{
			RegisteredClaims: jwt.RegisteredClaims{
				Subject: "system://oc-importer",
			},
			Scope: "doc_write doc_delete import_directive superuser",
		},
	})

	for _, e := range log.Events {
		err := in.handleEvent(ctx, e)
		if err != nil {
			return 0, fmt.Errorf(
				"failed to handle event %d: %w",
				e.ID, err)
		}

		pos = e.ID

		err = in.opt.LogPos.SetLogPosition(pos)
		if err != nil {
			return 0, fmt.Errorf(
				"failed to persist log position %d: %w",
				pos, err)
		}

		select {
		case in.opt.Done <- e:
		default:
		}
	}

	return pos, nil
}

func (in *Ingester) shouldArticleBeImported(
	ctx context.Context, evt OCLogEvent,
) (bool, OCEventType, error) {
	if evt.EventType == OCDeleteEvent {
		return true, evt.EventType, nil
	}

	props, err := in.opt.OCProps.GetProperties(ctx, evt.UUID, evt.Content.Version,
		[]string{"TTArticleType"})

	switch {
	case internal.IsHTTPErrorWithStatus(err, http.StatusNotFound):
		return true, OCDeleteEvent, nil
	case internal.IsHTTPErrorWithStatus(err, http.StatusInternalServerError):
		// TODO: this is not sound logic outside of a test, there might
		// be 500-errors that have nothing to do with corrupted data.
		in.opt.Blocklist.Add(evt.UUID, fmt.Errorf(
			"article import check failed: %w", err))

		return false, "", nil
	case err != nil:
		return false, "", fmt.Errorf(
			"failed to get TTArticleType from OC: %w", err)
	}

	values := props["TTArticleType"]

	if len(values) > 0 && values[0] != "Artikel" {
		return false, "", nil
	}

	return true, evt.EventType, nil
}

var blockMatch = []string{
	`unknown block type "TT/http://tt.se/spec/person/1.0/,rel=same-as,role="`,
}

func (in *Ingester) handleEvent(ctx context.Context, evt OCLogEvent) error {
	if in.opt.Blocklist.Blocked(evt.UUID) {
		return nil
	}

	include := map[string]includeCheckerFunc{
		"Article":    in.shouldArticleBeImported,
		"Assignment": nil,
		"Planning":   nil,
		"Concept":    nil,
		"Event":      nil,
	}

	fn, known := include[evt.Content.ContentType]
	if !known {
		return nil
	}

	eventType := evt.EventType

	if fn != nil {
		shouldImport, t, err := fn(ctx, evt)
		if err != nil {
			return fmt.Errorf(
				"failed to check if %s %d should be imported: %w",
				evt.UUID, evt.Content.Version, err)
		}

		if !shouldImport {
			return nil
		}

		eventType = t
	}

	switch eventType {
	case OCAddEvent, OCUpdateEvent:
		err := in.backfillIngest(ctx, evt)
		if err != nil {
			// Check for known errors that should lead to the
			// document being blocked.
			for _, pf := range blockMatch {
				if !strings.Contains(err.Error(), pf) {
					continue
				}

				in.opt.Blocklist.Add(evt.UUID, err)

				return nil
			}

			return fmt.Errorf(
				"failed to ingest %q: %w", evt.UUID, err)
		}
	case OCDeleteEvent:
		err := in.delete(ctx, evt)
		if err != nil {
			return fmt.Errorf(
				"failed to delete %q: %w", evt.UUID, err)
		}
	default:
		return fmt.Errorf("unknown event type %q", eventType)
	}

	return nil
}

func (in *Ingester) delete(ctx context.Context, evt OCLogEvent) error {
	// Patch up the bad medtop data.
	if strings.HasPrefix(evt.UUID, "medtop-") {
		evt.UUID, _ = mediaTopicIdentity(evt.UUID)
	}

	info, err := in.opt.Identity.GetCurrentVersion(evt.UUID)
	if err != nil {
		return fmt.Errorf("failed to get current version info: %w", err)
	}

	if info.OriginalUUID != "" {
		_, err = in.opt.API.Delete(ctx, &rpc.DeleteDocumentRequest{
			Uuid: info.OriginalUUID,
		})
		if err != nil {
			return fmt.Errorf(
				"failed to delete document with original UUID: %w", err)
		}
	}

	if info.OriginalUUID != evt.UUID {
		_, err = in.opt.API.Delete(ctx, &rpc.DeleteDocumentRequest{
			Uuid: evt.UUID,
		})
		if err != nil {
			return fmt.Errorf(
				"failed to delete document with event UUID: %w", err)
		}
	}

	return nil
}

type ConvertedDoc struct {
	Document     doc.Document
	ReplacesUUID []string
	Updater      string
	Creator      string
	Units        []string
	Status       string
}

type converterFunc func(ctx context.Context, evt OCLogEvent) (*ConvertedDoc, error)

var (
	errDeletedInSource = errors.New("deleted in source system")
	errIgnoreDocument  = errors.New("ignore document")
)

func (in *Ingester) backfillIngest(
	ctx context.Context, evt OCLogEvent,
) error {
	current, err := in.opt.Identity.GetCurrentVersion(evt.UUID)
	if err != nil {
		return fmt.Errorf(
			"failed to get the current version: %w", err)
	}

	if evt.Content.Version <= current.CurrentVersion {
		return nil
	}

	fillFrom := current.CurrentVersion + 1

	// Don't backfill for non-articles
	if evt.Content.ContentType != "Article" {
		fillFrom = evt.Content.Version
	}

	for v := fillFrom; v <= evt.Content.Version; v++ {
		evtCopy := evt

		evtCopy.Content.Version = v

		err := in.ingest(ctx, evtCopy)
		if errors.Is(err, errDeletedInSource) {
			// Ignore deletes for old versions
			if v != evt.Content.Version {
				continue
			}

			return in.delete(ctx, evt)
		} else if err != nil {
			return fmt.Errorf("failed to ingest version %d: %w",
				v, err)
		}
	}

	return nil
}

func (in *Ingester) ingest(ctx context.Context, evt OCLogEvent) error {
	var cFunc converterFunc

	switch evt.Content.ContentType {
	case "Assignment":
		cFunc = func(
			ctx context.Context, evt OCLogEvent,
		) (*ConvertedDoc, error) {
			return assignmentImport(ctx, evt, in.opt, in.ccaImport)
		}
	default:
		cFunc = in.ccaImport
	}

	cd, err := cFunc(ctx, evt)
	if errors.Is(err, errIgnoreDocument) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to convert source doc: %w", err)
	}

	switch cd.Document.Type {
	case "core/contact":
		err := sideloadContactInformation(
			ctx, in.opt.Objects, evt, &cd.Document)
		if err != nil {
			return fmt.Errorf("failed to sideload NewsML data: %w", err)
		}
	case "core/organisation":
		err := sideloadOrganisationInformation(
			ctx, in.opt.Objects, evt, &cd.Document)
		if err != nil {
			return fmt.Errorf("failed to sideload NewsML data: %w", err)
		}
	}

	// Patch up the bad medtop data.
	if strings.HasPrefix(evt.UUID, "medtop-") {
		uuid, uri := mediaTopicIdentity(evt.UUID)

		evt.UUID = uuid
		cd.Document.UUID = uuid
		cd.Document.URI = uri
	}

	if len(cd.ReplacesUUID) == 1 {
		err = in.opt.Identity.RegisterContinuation(
			cd.ReplacesUUID[0], evt.UUID,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to register that %s replaces %s: %w",
				evt.UUID, cd.ReplacesUUID[0], err)
		}
	}

	if len(cd.ReplacesUUID) > 1 {
		fmt.Fprintf(os.Stdout, "replaces multiple %s %d\n",
			evt.UUID, evt.Content.Version)
	}

	info, err := in.opt.Identity.RegisterReference(VersionReference{
		UUID:    evt.UUID,
		Version: evt.Content.Version,
	})
	if err != nil {
		return fmt.Errorf("failed to get current version info: %w", err)
	}

	d := cd.Document

	if info.UUID != d.UUID {
		if strings.Contains(d.URI, evt.UUID) {
			d.URI = strings.ReplaceAll(
				d.URI, evt.UUID, info.UUID,
			)
		}

		d.UUID = info.UUID
	}

	err = fixUUIDs(&d)
	if err != nil {
		in.opt.Blocklist.Add(evt.UUID, fmt.Errorf(
			"failed to fix UUIDs: %w", err))

		return nil
	}

	docUUID, err := uuid.Parse(d.UUID)
	if err != nil {
		return fmt.Errorf("invalid document UUID %q: %w", d.UUID, err)
	}

	status := in.checkStatus(ctx, cd.Status, d.Meta)

	acl := []*rpc.ACLEntry{
		{
			Uri:         cd.Creator,
			Permissions: []string{"r", "w"},
		},
	}

	for _, unit := range cd.Units {
		acl = append(acl, &rpc.ACLEntry{
			Uri:         unit,
			Permissions: []string{"r", "w"},
		})
	}

	d.URI = fixDocumentURI(d.UUID, d.URI)

	_, err = in.opt.API.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID.String(),
		Document: repository.DocumentToRPC(&d),
		Meta: doc.DataMap{
			"oc-source":  evt.UUID,
			"oc-version": strconv.Itoa(evt.Content.Version),
			"oc-event":   strconv.Itoa(evt.ID),
		},
		Status: status,
		Acl:    acl,
		ImportDirective: &rpc.ImportDirective{
			OriginallyCreated: cd.Timestamp.Format(time.RFC3339),
			OriginalCreator:   cd.Creator,
		},
	})
	if err != nil {
		in.opt.Logger.ErrorCtx(
			ctx, "failed to store update", err,
			internal.LogKeyDocumentUUID, docUUID.String(),
			internal.LogKeyOCSource, evt.UUID,
			internal.LogKeyOCVersion, evt.Content.Version,
			internal.LogKeyOCEvent, evt.ID,
		)
	}

	return nil
}

func fixDocumentURI(docUUID, uri string) string {
	if uri == "core://article/" {
		return "core://article/" + docUUID
	}

	segs := strings.Split(uri, "/")
	base := segs[len(segs)-1]

	validUUID, err := uuid.Parse(base)
	if err != nil {
		return uri
	}

	if validUUID.String() != docUUID {
		segs[len(segs)-1] = docUUID

		return strings.Join(segs, "/")
	}

	return uri
}

func (in *Ingester) ccaImport(ctx context.Context, evt OCLogEvent) (*ConvertedDoc, error) {
	var out ConvertedDoc

	docRes, err := in.opt.GetDocument(ctx, GetDocumentRequest{
		UUID:    evt.UUID,
		Version: evt.Content.Version,
	})
	if IsRPCErrorCode(err, "not_found") {
		return nil, errDeletedInSource
	}

	if IsRPCErrorCode(err, "internal") {
		var permError bool

		flags := []string{
			"UTC offset",
			"failed to parse created",
		}

		for _, f := range flags {
			if strings.Contains(err.Error(), f) {
				permError = true

				break
			}
		}

		if permError {
			in.opt.Blocklist.Add(evt.UUID, err)
		}

		return nil, errIgnoreDocument
	}

	if err != nil {
		return nil, fmt.Errorf("failed to fetch original document: %w", err)
	}

	nDoc := docRes.Document

	doc, err := ConvertNavigaDoc(nDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to convert document: %w", err)
	}

	out.Document = doc
	out.Status = nDoc.Status

	for _, link := range docRes.Document.Links {
		switch link.Rel {
		case "irel:previousVersion":
			out.ReplacesUUID = append(out.ReplacesUUID, link.UUID)
		case "updater":
			out.Updater = link.URI
		case "creator":
			out.Creator = link.URI

			unit, ok := unitReference(link)
			if ok {
				out.Units = append(out.Units, unit)
			}
		case "shared-with":
			unit, ok := sharedWithUnit(link)
			if ok {
				out.Units = append(out.Units, unit)
			}
		}
	}

	return &out, nil
}

func sharedWithUnit(link navigadoc.Block) (string, bool) {
	for _, u := range link.Links {
		if u.Type != unitMIMEType ||
			u.Rel != "shared-with" {
			continue
		}

		return strings.Replace(u.URI,
			"imid://unit/", "core://unit/", 1,
		), true
	}

	return "", false
}

func unitReference(link navigadoc.Block) (string, bool) {
	for _, l := range link.Links {
		if l.Type != orgMimeType || l.Rel != relAffiliation {
			continue
		}

		for _, u := range l.Links {
			if u.Type != unitMIMEType || u.Rel != relAffiliation {
				continue
			}

			return strings.Replace(u.URI,
				"imid://unit/", "core://unit/", 1,
			), true
		}
	}

	return "", false
}

func (in *Ingester) checkStatus(
	_ context.Context, status string, metaBlocks []doc.Block,
) []*rpc.StatusUpdate {
	if status == "draft" {
		return nil
	}

	up := rpc.StatusUpdate{Name: status}

	up.Meta = populateSlugMeta(up.Meta, metaBlocks)

	return []*rpc.StatusUpdate{&up}
}

var slugPatterns = map[string]*regexp.Regexp{
	"development":    regexp.MustCompile(`UV\d*$`),
	"fix":            regexp.MustCompile(`KORR\d*$`),
	"correction":     regexp.MustCompile(`RÄ\d*$`),
	"retransmission": regexp.MustCompile(`OMS\d*$`),
}

func populateSlugMeta(meta doc.DataMap, blocks []doc.Block) doc.DataMap {
	var slug string

	for i := range blocks {
		if blocks[i].Type != "tt/slugline" {
			continue
		}

		slug = blocks[i].Value
	}

	if slug == "" {
		return meta
	}

	for cause, pattern := range slugPatterns {
		if !pattern.MatchString(slug) {
			continue
		}

		if meta == nil {
			meta = make(doc.DataMap)
		}

		meta["cause"] = cause

		return meta
	}

	return meta
}
