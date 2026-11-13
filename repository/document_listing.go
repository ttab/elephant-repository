package repository

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	rpcdoc "github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/rpc"
	"github.com/ttab/newsdoc"
)

const (
	// ListDocumentsDefaultLimit is the page size used when a
	// ListDocuments request doesn't set a limit.
	ListDocumentsDefaultLimit = 100
	// ListDocumentsMaxLimit caps the page size of a ListDocuments request.
	ListDocumentsMaxLimit = 500
	// DefaultListDocumentsMaxBytes is the default cap on the size of the
	// document data that is loaded for a ListDocuments page. A page always
	// holds at least one document, even if it alone is larger.
	DefaultListDocumentsMaxBytes = 4 << 20
	// ListDocumentsMaxSelectors caps the number of selectors in a
	// ListDocuments request.
	ListDocumentsMaxSelectors = 20

	// listDocumentsLoadChunk is how many documents are loaded at a time
	// when filling a page, so that a page cut short by the byte cap
	// doesn't load much more than it returns.
	listDocumentsLoadChunk = 50

	listCursorPrefix = "v1"
)

type listingMetrics struct {
	pageDocuments prometheus.Histogram
	pageBytes     prometheus.Histogram
	byteCapped    prometheus.Counter
}

func newListingMetrics(reg prometheus.Registerer) (*listingMetrics, error) {
	var m listingMetrics

	prom := elephantine.NewMetricsHelper(reg)

	prom.Histogram(&m.pageDocuments, prometheus.HistogramOpts{
		Name: "repository_list_documents_page_documents",
		Help: "Documents returned per ListDocuments page; pages well " +
			"below the requested limit mean the byte cap or ACL " +
			"filtering is cutting them short.",
		Buckets: []float64{0, 1, 10, 25, 50, 100, 250, 500},
	})

	prom.Histogram(&m.pageBytes, prometheus.HistogramOpts{
		Name: "repository_list_documents_page_bytes",
		Help: "Bytes of document data loaded per ListDocuments page; " +
			"growth means clients are crawling larger documents, " +
			"and with it the read load the listing puts on the database.",
		Buckets: prometheus.ExponentialBuckets(1024, 4, 9),
	})

	prom.Counter(&m.byteCapped, prometheus.CounterOpts{
		Name: "repository_list_documents_byte_capped_total",
		Help: "ListDocuments pages cut short by the byte cap; a high " +
			"rate means clients ask for more than fits in a page " +
			"and make more round trips than their limit suggests.",
	})

	if err := prom.Err(); err != nil {
		return nil, fmt.Errorf("register listing metrics: %w", err)
	}

	return &m, nil
}

// listCursor is the position in a listing: the index of the selector being
// worked through and the inclusive lower bound for the next UUID in it.
type listCursor struct {
	Selector int
	From     uuid.UUID
}

func (c listCursor) Encode() string {
	raw := listCursorPrefix + ":" + strconv.Itoa(c.Selector) +
		":" + c.From.String()

	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeListCursor(s string) (listCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return listCursor{}, fmt.Errorf("invalid encoding: %w", err)
	}

	parts := strings.Split(string(raw), ":")
	if len(parts) != 3 || parts[0] != listCursorPrefix {
		return listCursor{}, fmt.Errorf("unknown cursor format")
	}

	idx, err := strconv.Atoi(parts[1])
	if err != nil || idx < 0 {
		return listCursor{}, fmt.Errorf("invalid selector index")
	}

	from, err := uuid.Parse(parts[2])
	if err != nil {
		return listCursor{}, fmt.Errorf("invalid position: %w", err)
	}

	return listCursor{
		Selector: idx,
		From:     from,
	}, nil
}

// nextUUID returns the UUID that follows id in byte order, which is the order
// Postgres sorts UUIDs in. Returns false if id is the last possible UUID.
func nextUUID(id uuid.UUID) (uuid.UUID, bool) {
	for i := len(id) - 1; i >= 0; i-- {
		id[i]++

		if id[i] != 0 {
			return id, true
		}
	}

	return uuid.Nil, false
}

// ListDocuments implements repository.Documents.
func (a *DocumentsService) ListDocuments(
	ctx context.Context, req *repository.ListDocumentsRequest,
) (*repository.ListDocumentsResponse, error) {
	auth, err := RequireAnyScope(ctx,
		ScopeDocumentRead, ScopeDocumentReadAll,
		ScopeDocumentAdmin,
	)
	if err != nil {
		return nil, err
	}

	if len(req.Selectors) == 0 {
		return nil, rpc.RequiredArgument("selectors")
	}

	if len(req.Selectors) > ListDocumentsMaxSelectors {
		return nil, rpc.InvalidArgument("selectors", fmt.Sprintf(
			"cannot list more than %d selectors",
			ListDocumentsMaxSelectors))
	}

	var current bool

	switch req.Version {
	case repository.ListVersion_LIST_VERSION_SELECTED:
	case repository.ListVersion_LIST_VERSION_CURRENT:
		current = true
	default:
		return nil, rpc.InvalidArgument("version",
			"unknown version mode")
	}

	selectors := make([]DocumentListSelector, len(req.Selectors))

	for i, s := range req.Selectors {
		sel, err := a.listSelectorFromRPC(i, s, current)
		if err != nil {
			return nil, err
		}

		selectors[i] = sel
	}

	limit := req.Limit

	switch {
	case limit < 0:
		return nil, rpc.InvalidArgument("limit",
			"cannot be a negative number")
	case limit == 0:
		limit = ListDocumentsDefaultLimit
	case limit > ListDocumentsMaxLimit:
		limit = ListDocumentsMaxLimit
	}

	extractors, err := parseSubsetExpressions(req.Subset)
	if err != nil {
		return nil, err
	}

	var cursor listCursor

	if req.After != "" {
		c, err := decodeListCursor(req.After)
		if err != nil {
			return nil, rpc.InvalidArgument("after", err.Error())
		}

		if c.Selector >= len(selectors) {
			return nil, rpc.InvalidArgument("after",
				"the cursor doesn't match the selectors")
		}

		cursor = c
	}

	page := listPage{
		svc:        a,
		selectors:  selectors,
		current:    current,
		limit:      limit,
		extractors: extractors,
		aclBypass: auth.Claims.HasAnyScope(
			ScopeDocumentReadAll, ScopeDocumentAdmin),
		identity: append([]string{auth.Claims.Subject},
			auth.Claims.Units...),
	}

	err = page.Fill(ctx, cursor)
	if err != nil {
		return nil, err
	}

	if req.IncludeMeta && len(page.items) > 0 {
		err := a.addListMeta(ctx, page.items, page.ids)
		if err != nil {
			return nil, err
		}
	}

	a.listing.pageDocuments.Observe(float64(len(page.items)))
	a.listing.pageBytes.Observe(float64(page.bytes))

	if page.capped {
		a.listing.byteCapped.Inc()
	}

	res := repository.ListDocumentsResponse{
		Items: page.items,
	}

	if page.next != nil {
		res.Next = page.next.Encode()
	}

	return &res, nil
}

func (a *DocumentsService) listSelectorFromRPC(
	idx int, s *repository.ListDocumentsSelector, current bool,
) (DocumentListSelector, error) {
	field := func(name string) string {
		return fmt.Sprintf("selectors.%d.%s", idx, name)
	}

	if s == nil || s.Type == "" {
		return DocumentListSelector{}, rpc.RequiredArgument(field("type"))
	}

	if s.Status != "" && s.WorkflowCheckpoint != "" {
		return DocumentListSelector{}, rpc.InvalidArgument(
			fmt.Sprintf("selectors.%d", idx),
			"status and workflow_checkpoint cannot be combined")
	}

	// A negative checkpoint has no status head of its own, and so no
	// version to select.
	if s.WorkflowCheckpoint != "" && !current {
		wf, ok := a.workflows.GetDocumentWorkflow(s.Type)
		if ok && wf.Configuration.NegativeCheckpoint != "" &&
			wf.Configuration.NegativeCheckpoint == s.WorkflowCheckpoint {
			return DocumentListSelector{}, rpc.InvalidArgument(
				field("workflow_checkpoint"), fmt.Sprintf(
					"%q is the negative checkpoint for %q and "+
						"has no version to select, use the "+
						"LIST_VERSION_CURRENT version mode",
					s.WorkflowCheckpoint, s.Type))
		}
	}

	return DocumentListSelector{
		Type:       s.Type,
		Status:     s.Status,
		Checkpoint: s.WorkflowCheckpoint,
	}, nil
}

// addListMeta adds the document meta to the items, ids holds the document
// UUID of each item.
func (a *DocumentsService) addListMeta(
	ctx context.Context,
	items []*repository.ListDocumentsItem,
	ids []uuid.UUID,
) error {
	unique := make([]uuid.UUID, 0, len(ids))
	seen := make(map[uuid.UUID]bool, len(ids))

	for _, id := range ids {
		if seen[id] {
			continue
		}

		seen[id] = true

		unique = append(unique, id)
	}

	meta, err := a.store.BulkGetDocumentMeta(ctx, unique)
	if err != nil {
		return rpc.Internalf("get document meta: %v", err)
	}

	for i, item := range items {
		m, ok := meta[ids[i]]
		if !ok {
			continue
		}

		item.Meta = DocumentMetaToRPC(m)
	}

	return nil
}

// listPage collects one page of a ListDocuments listing.
type listPage struct {
	svc        *DocumentsService
	selectors  []DocumentListSelector
	current    bool
	limit      int64
	extractors []*newsdoc.ValueExtractor
	aclBypass  bool
	identity   []string

	items   []*repository.ListDocumentsItem
	ids     []uuid.UUID
	scanned int64
	bytes   int
	capped  bool
	// next is the cursor for the next page, nil when the listing is
	// complete.
	next *listCursor
}

// Fill works through the selectors from the cursor until the page is full,
// and sets the cursor for the next page unless the listing is complete.
//
// A page is full when limit rows have been scanned, so documents that the
// caller isn't allowed to read still count against it, which keeps the work
// done per call bounded. It's also full when the next document would take
// the loaded document data past the service's byte cap.
func (p *listPage) Fill(
	ctx context.Context, cursor listCursor,
) error {
	for cursor.Selector < len(p.selectors) {
		remaining := p.limit - p.scanned
		if remaining <= 0 {
			p.next = &cursor

			return nil
		}

		sel := p.selectors[cursor.Selector]

		// The listing reads with the service's own database access,
		// but nothing is returned until the caller's read permission
		// has been checked for each document below, unless they hold
		// a scope that bypasses ACLs.
		rows, err := p.svc.store.ListDocuments(ctx, ListDocumentsQuery{
			Selector: sel,
			Current:  p.current,
			From:     cursor.From,
			Limit:    remaining,
		})
		if err != nil {
			return rpc.Internalf("list documents: %v", err)
		}

		p.scanned += int64(len(rows))

		err = p.addDocuments(ctx, cursor.Selector, sel, rows)
		if err != nil {
			return err
		}

		if p.capped {
			return nil
		}

		// A short result means that the selector is exhausted.
		if int64(len(rows)) < remaining {
			cursor = listCursor{Selector: cursor.Selector + 1}

			continue
		}

		next, ok := nextUUID(rows[len(rows)-1].UUID)
		if !ok {
			cursor = listCursor{Selector: cursor.Selector + 1}

			continue
		}

		cursor.From = next
	}

	return nil
}

// addDocuments filters the rows down to the documents the caller can read,
// loads them and adds them to the page. If the byte cap is hit it marks the
// page as capped, with the next page starting at the first document that
// didn't fit.
func (p *listPage) addDocuments(
	ctx context.Context,
	selIdx int,
	sel DocumentListSelector,
	rows []ListedDocument,
) error {
	if len(rows) == 0 {
		return nil
	}

	if !p.aclBypass {
		ids := make([]uuid.UUID, len(rows))

		for i := range rows {
			ids[i] = rows[i].UUID
		}

		permitted, err := p.svc.store.BulkCheckPermissions(ctx,
			BulkCheckPermissionRequest{
				UUIDs:       ids,
				GranteeURIs: p.identity,
				Permissions: []Permission{ReadPermission},
			})
		if err != nil {
			return rpc.Internalf("check ACL access: %v", err)
		}

		allowed := make(map[uuid.UUID]bool, len(permitted))

		for _, id := range permitted {
			allowed[id] = true
		}

		accepted := make([]ListedDocument, 0, len(rows))

		for _, r := range rows {
			if allowed[r.UUID] {
				accepted = append(accepted, r)
			}
		}

		rows = accepted
	}

	for start := 0; start < len(rows); start += listDocumentsLoadChunk {
		chunk := rows[start:min(start+listDocumentsLoadChunk, len(rows))]

		refs := make([]BulkGetReference, len(chunk))

		for i, r := range chunk {
			refs[i] = BulkGetReference(r)
		}

		docs, err := p.svc.store.BulkGetDocuments(ctx, refs)
		if err != nil {
			return rpc.Internalf("load documents: %v", err)
		}

		loaded := make(map[uuid.UUID]*BulkGetItem, len(docs))

		for i := range docs {
			loaded[docs[i].UUID] = &docs[i]
		}

		for _, r := range chunk {
			// A document that was deleted after it was listed is
			// skipped.
			doc, ok := loaded[r.UUID]
			if !ok {
				continue
			}

			if len(p.items) > 0 && p.bytes+doc.Size > p.svc.listMaxBytes {
				p.capped = true
				p.next = &listCursor{
					Selector: selIdx,
					From:     r.UUID,
				}

				return nil
			}

			p.bytes += doc.Size

			item := repository.ListDocumentsItem{
				Uuid:     r.UUID.String(),
				Type:     sel.Type,
				Version:  doc.Version,
				Selector: int64(selIdx),
			}

			if len(p.extractors) > 0 {
				item.Subset = collectSubset(doc.Document, p.extractors)
			} else {
				item.Document = rpcdoc.DocumentToRPC(doc.Document)
			}

			p.items = append(p.items, &item)
			p.ids = append(p.ids, r.UUID)
		}
	}

	return nil
}
