package docformat

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	navigadoc "github.com/navigacontentlab/navigadoc/doc"
	"github.com/navigacontentlab/revisor"
)

//go:embed constraints/*.json
var BuiltInConstraints embed.FS

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

type IngestOptions struct {
	DefaultLanguage string
	Identity        IdentityStore
	LogPos          LogPosStore
	OCLog           OCLogGetter
	GetDocument     GetDocumentFunc
	Objects         ObjectGetter
	OCProps         PropertyGetter
	DocStore        DocStore
	Blocklist       *Blocklist
	Validator       *revisor.Validator
	Done            chan OCLogEvent
}

type ValidationError struct {
	Document Document
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

type HTTPError struct {
	Status     string
	StatusCode int
	Header     http.Header
	Body       bytes.Buffer
}

func (e HTTPError) Error() string {
	return e.Status
}

func IsHTTPErrorWithStatus(err error, status int) bool {
	var httpErr HTTPError

	if !errors.As(err, &httpErr) {
		return false
	}

	return httpErr.StatusCode == status
}

func HTTPErrorFromResponse(res *http.Response) error {
	e := HTTPError{
		Status:     res.Status,
		StatusCode: res.StatusCode,
		Header:     res.Header,
	}

	_, _ = io.Copy(&e.Body, res.Body)

	return e
}

type Ingester struct {
	opt IngestOptions

	AsyncError func(_ context.Context, err error)
}

func NewIngester(opt IngestOptions) *Ingester {
	return &Ingester{
		opt: opt,
		AsyncError: func(_ context.Context, err error) {
			log.Println(err.Error())
		},
	}
}

type includeCheckerFunc func(ctx context.Context, evt OCLogEvent) (bool, string, error)

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
			return ctx.Err()
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
) (bool, string, error) {
	if evt.EventType == "DELETE" {
		return true, evt.EventType, nil
	}

	props, err := in.opt.OCProps.GetProperties(ctx, evt.UUID, evt.Content.Version,
		[]string{"TTArticleType"})
	if IsHTTPErrorWithStatus(err, http.StatusNotFound) {
		return true, "DELETE", nil
	} else if IsHTTPErrorWithStatus(err, http.StatusInternalServerError) {
		// TODO: this is not sound logic outside of a test, there might
		// be 500-errors that have nothing to do with corrupted data.

		in.opt.Blocklist.Add(evt.UUID, fmt.Errorf(
			"article import check failed: %w", err))

		return false, "", nil
	} else if err != nil {
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
	case "ADD", "UPDATE":
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
	case "DELETE":
		err := in.delete(ctx, evt)
		if err != nil {
			return fmt.Errorf(
				"failed to delete %q: %w", evt.UUID, err)
		}
	}

	return nil
}

func (in *Ingester) delete(ctx context.Context, evt OCLogEvent) error {
	info, err := in.opt.Identity.GetCurrentVersion(evt.UUID)
	if err != nil {
		return fmt.Errorf("failed to get current version info: %w", err)
	}

	err = in.opt.DocStore.Delete(ctx, info.OriginalUUID)
	if err != nil {
		return fmt.Errorf(
			"failed to delete document with original UUID: %w", err)
	}

	err = in.opt.DocStore.Delete(ctx, evt.UUID)
	if err != nil {
		return fmt.Errorf(
			"failed to delete document with event UUID: %w", err)
	}

	return nil
}

type ConvertedDoc struct {
	Document     Document
	ReplacesUUID []string
	Updater      IdentityReference
	Creator      IdentityReference
	Units        []IdentityReference
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

	doc := cd.Document

	if info.UUID != doc.UUID {
		if strings.Contains(doc.URI, evt.UUID) {
			doc.URI = strings.ReplaceAll(
				doc.URI, evt.UUID, info.UUID,
			)
		}

		doc.UUID = info.UUID
	}

	err = fixUUIDs(&doc)
	if err != nil {
		in.opt.Blocklist.Add(evt.UUID, fmt.Errorf(
			"failed to fix UUIDs: %w", err))
		return nil
	}

	status, err := in.checkStatus(ctx, cd.Status)
	if err != nil {
		return fmt.Errorf("failed to check for status updates: %w", err)
	}

	acl := []ACLEntry{
		{
			Name:        cd.Creator.Name,
			URI:         cd.Creator.URI,
			Permissions: []string{"r", "w"},
		},
	}

	for _, unit := range cd.Units {
		acl = append(acl, ACLEntry{
			Name:        unit.Name,
			URI:         unit.URI,
			Permissions: []string{"r", "w"},
		})
	}

	err = in.validateDocument(doc)
	if err != nil {
		return err
	}

	_, err = in.opt.DocStore.Update(ctx, &UpdateRequest{
		Created: evt.Created,
		Updater: cd.Updater,
		Meta: []UpdateMeta{
			{Key: "oc-source", Value: evt.UUID},
			{Key: "oc-version", Value: strconv.Itoa(evt.Content.Version)},
			{Key: "oc-event", Value: strconv.Itoa(evt.ID)},
		},
		Document: doc,
		Status:   status,
		ACL:      acl,
	})
	if err != nil {
		return fmt.Errorf("failed to store update: %w", err)
	}

	return nil
}

func (in *Ingester) validateDocument(doc Document) error {
	data, err := json.Marshal(&doc)
	if err != nil {
		return fmt.Errorf(
			"failed to marshal document for validation: %w", err)
	}

	var nDoc navigadoc.Document

	err = json.Unmarshal(data, &nDoc)
	if err != nil {
		return fmt.Errorf(
			"failed to unmarshal document for validation: %w", err)
	}

	errors := in.opt.Validator.ValidateDocument(&nDoc)
	if len(errors) > 0 {
		return &ValidationError{
			Document: doc,
			Errors:   errors,
		}
	}

	return nil
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
			out.Updater = IdentityReference{
				Name: link.Title,
				URI:  link.URI,
			}
		case "creator":
			out.Creator = IdentityReference{
				Name: link.Title,
				URI:  link.URI,
			}

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

func sharedWithUnit(link navigadoc.Block) (IdentityReference, bool) {
	for _, u := range link.Links {
		if u.Type != "x-imid/unit" ||
			u.Rel != "shared-with" {
			continue
		}

		return IdentityReference{
			Name: u.Title,
			URI: strings.Replace(u.URI,
				"imid://unit/", "core://unit/", 1),
		}, true
	}

	return IdentityReference{}, false
}

func unitReference(link navigadoc.Block) (IdentityReference, bool) {
	for _, l := range link.Links {
		if l.Type != "x-imid/organisation" ||
			l.Rel != "affiliation" {
			continue
		}

		for _, u := range l.Links {
			if u.Type != "x-imid/unit" ||
				u.Rel != "affiliation" {
				continue
			}

			return IdentityReference{
				Name: u.Title,
				URI: strings.Replace(u.URI,
					"imid://unit/", "core://unit/", 1),
			}, true
		}
	}

	return IdentityReference{}, false
}

func (in *Ingester) checkStatus(
	ctx context.Context, status string,
) ([]StatusUpdate, error) {
	if status == "draft" {
		return nil, nil
	}

	return []StatusUpdate{
		{Name: status},
	}, nil
}
