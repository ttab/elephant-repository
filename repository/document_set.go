package repository

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"sync"

	"github.com/google/uuid"
	rpc_newsdoc "github.com/ttab/elephant-api/newsdoc"
	rsock "github.com/ttab/elephant-api/repositorysocket"
	"github.com/ttab/newsdoc"
	"golang.org/x/sync/errgroup"
)

type DocumentSetEmitter interface {
	DocumentBatch(ctx context.Context, msg *rsock.DocumentBatch)
	Update(ctx context.Context, msg *rsock.DocumentUpdate)
	InclusionBatch(ctx context.Context, msg *rsock.InclusionBatch)
	Remove(ctx context.Context, msg *rsock.DocumentRemoved)
	Error(ctx context.Context, msg *rsock.Error)
}

func newDocumentSet(
	docType string,
	includeACL bool,
	name string,
	timespan *Timespan,
	labels []string,
	includeExtractors []*newsdoc.ValueExtractor,
	subsetExtractors []*newsdoc.ValueExtractor,
	inclusionSubsets map[string][]*newsdoc.ValueExtractor,
	identity []string,
	cache *DocCache,
	store DocStore,
	emitter DocumentSetEmitter,
) *documentSet {
	slices.Sort(labels)

	ds := documentSet{
		docType:           docType,
		includeACL:        includeACL,
		name:              name,
		timespan:          timespan,
		labels:            slices.Compact(labels),
		set:               make(map[uuid.UUID]*setDocument),
		included:          make(map[uuid.UUID]*incDocument),
		includeExtractors: includeExtractors,
		subsetExtractors:  subsetExtractors,
		inclusionSubsets:  inclusionSubsets,
		process:           make(chan DocumentStreamItem, 128),
		oosErr:            make(chan struct{}),
		identity:          identity,
		cache:             cache,
		store:             store,
		emitter:           emitter,
	}

	return &ds
}

// documentSet tracks documents that are included in a set. See SocketSession.
type documentSet struct {
	docType    string
	includeACL bool
	name       string
	timespan   *Timespan
	labels     []string

	identMutex sync.RWMutex
	identity   []string

	cache   *DocCache
	store   DocStore
	emitter DocumentSetEmitter

	set      map[uuid.UUID]*setDocument
	included map[uuid.UUID]*incDocument

	includeExtractors []*newsdoc.ValueExtractor
	subsetExtractors  []*newsdoc.ValueExtractor
	inclusionSubsets  map[string][]*newsdoc.ValueExtractor

	process chan DocumentStreamItem

	oosErr  chan struct{}
	oosOnce sync.Once
}

func (ds *documentSet) IdentityUpdated(uris []string) {
	ds.identMutex.Lock()
	ds.identity = uris
	ds.identMutex.Unlock()
}

func (ds *documentSet) getIdentity() []string {
	ds.identMutex.RLock()
	identity := ds.identity
	ds.identMutex.RUnlock()

	return identity
}

func (ds *documentSet) handleDocStreamItem(items []DocumentStreamItem) {
	for _, item := range items {
		if item.Event.Event == TypeACLUpdate && !ds.includeACL {
			continue
		}

		select {
		case <-ds.oosErr:
			return
		case ds.process <- item:
		default:
			// If the processing buffer is full we will lose
			// events and get out of sync with the actual state of
			// things. This will cause the processing loop to emit
			// an "oos" error and exit on the next iteration.
			ds.oosOnce.Do(func() {
				close(ds.oosErr)
			})

			return
		}
	}
}

func (ds *documentSet) Initialise(
	ctx context.Context,
	stream *DocumentStream,
) error {
	stream.Subscribe(ctx, ds.handleDocStreamItem)

	method := matchByType

	if ds.timespan != nil {
		method = matchByTimeRange
	}

	var items []DocumentItem

	switch method {
	case matchByTimeRange:
		hits, err := ds.store.ListDocumentsInTimeRange(
			ctx, ds.docType, *ds.timespan, ds.labels)
		if err != nil {
			return fmt.Errorf(
				"get documents for type and time range: %w", err)
		}

		items = hits
	case matchByType:
		hits, err := ds.store.ListDocumentsOfType(
			ctx, ds.docType, nil, ds.labels)
		if err != nil {
			return fmt.Errorf(
				"get documents by type and labels: %w", err)
		}

		items = hits
	default:
		return fmt.Errorf("unexpected match method: %#v", method)
	}

	permReq := BulkCheckPermissionRequest{
		UUIDs:       make([]uuid.UUID, len(items)),
		GranteeURIs: ds.getIdentity(),
		Permissions: []Permission{ReadPermission},
	}

	versions := make(map[uuid.UUID]int64, len(items))

	for i := range items {
		versions[items[i].UUID] = items[i].CurrentVersion
		permReq.UUIDs[i] = items[i].UUID
	}

	allowedDocs, err := ds.store.BulkCheckPermissions(ctx, permReq)
	if err != nil {
		return fmt.Errorf("perform pernissions check: %w", err)
	}

	if len(allowedDocs) == 0 {
		return nil
	}

	getRefs := make([]BulkGetReference, len(allowedDocs))

	matches := make(map[uuid.UUID]*DocumentStreamData)

	for i := range allowedDocs {
		matches[allowedDocs[i]] = &DocumentStreamData{}

		getRefs[i] = BulkGetReference{
			UUID:    allowedDocs[i],
			Version: versions[allowedDocs[i]],
		}
	}

	var (
		meta      map[uuid.UUID]*DocumentMeta
		documents iter.Seq[BulkGetItem]
	)

	grp, gCtx := errgroup.WithContext(ctx)

	grp.Go(func() error {
		iter, err := ds.cache.GetDocuments(gCtx, getRefs)
		if err != nil {
			return fmt.Errorf("get documents: %w", err)
		}

		documents = iter

		return nil
	})

	grp.Go(func() error {
		m, err := ds.store.BulkGetDocumentMeta(gCtx, allowedDocs)
		if err != nil {
			return fmt.Errorf("get document meta: %w", err)
		}

		meta = m

		return nil
	})

	err = grp.Wait()
	if err != nil {
		return fmt.Errorf("get match data: %w", err)
	}

	// TODO: This is probably where we would apply filters.

	for docID, m := range meta {
		match := matches[docID]
		if match == nil {
			continue
		}

		match.Meta = *m
	}

	for doc := range documents {
		docID, err := uuid.Parse(doc.Document.UUID)
		if err != nil {
			return fmt.Errorf(
				"invalid document UUID: %w", err)
		}

		match, ok := matches[docID]
		if !ok {
			continue
		}

		match.Document = doc.Document
		match.DocumentVersion = doc.Version
	}

	var (
		batch       *rsock.DocumentBatch
		inclChanges []inclusionChange
	)

	batch = &rsock.DocumentBatch{}

	for docUUID, m := range matches {
		state := &rsock.DocumentState{
			Uuid: docUUID.String(),
			Meta: DocumentMetaToRPC(&m.Meta),
		}

		if len(ds.subsetExtractors) > 0 {
			state.Subset = collectSubset(m.Document, ds.subsetExtractors)
		} else {
			state.Document = rpc_newsdoc.DocumentToRPC(m.Document)
		}

		batch.Documents = append(batch.Documents, state)

		incCh := ds.AddDocument(docUUID, m.Document)

		inclChanges = append(inclChanges, incCh...)

		if len(batch.Documents) == 20 {
			err := ds.emitBatch(ctx, batch, inclChanges)
			if err != nil {
				return fmt.Errorf("emit document batch: %w", err)
			}

			batch = &rsock.DocumentBatch{}
			inclChanges = nil
		}
	}

	// We always emit a final batch, even if it happens to be zero length,
	// so not checking len(Documents) before emit.
	batch.FinalBatch = true

	err = ds.emitBatch(ctx, batch, inclChanges)
	if err != nil {
		return fmt.Errorf("emit final document batch: %w", err)
	}

	return nil
}

func (ds *documentSet) emitBatch(
	ctx context.Context,
	batch *rsock.DocumentBatch,
	inclusionChanges []inclusionChange,
) error {
	batch.SetName = ds.name

	ds.emitter.DocumentBatch(ctx, batch)

	err := ds.handleInclusionChanges(ctx, inclusionChanges)
	if err != nil {
		return fmt.Errorf("failed to handle inclusions: %w", err)
	}

	return nil
}

func (ds *documentSet) ProcessingLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ds.oosErr:
			ds.emitErrorf(ctx, "oos", "processing buffer overflow")

			return
		case item := <-ds.process:
			// Included documents are not part of the core set (and
			// are therefore not Added), but they are part of an
			// extended "inclusion set", and emitted as a service to
			// the client.
			isIncluded, includeDoc := ds.isIncluded(item.Event.UUID)

			// Only process events for documents that have been
			// included or have the core set type.
			if item.Event.Type != ds.docType && !isIncluded {
				continue
			}

			isDelete := item.Event.Event == TypeDeleteDocument
			isDocVersion := item.Event.Event == TypeDocumentVersion
			hasAccess := true

			// Don't do access checks for delete events.
			if !isDelete {
				acc, err := ds.store.CheckPermissions(ctx,
					CheckPermissionRequest{
						UUID:        item.Event.UUID,
						GranteeURIs: ds.getIdentity(),
						Permissions: []Permission{ReadPermission},
					})
				if err != nil {
					ds.emitErrorf(ctx, "processing",
						"failed to check permissions: %v", err)

					return
				}

				hasAccess = acc == PermissionCheckAllowed
			}

			// Being a bit verbose with the booleans here to
			// document the reasoning and avoid complex boolean
			// expressions in the actual flow control statements.

			// Deletes and documents that we dont't have access to
			// always exit the set.
			shouldBeInSet := !isDelete && hasAccess &&
				item.Event.Type == ds.docType &&
				ds.EvaluateDocument(
					item.Event.Type,
					item.Timespans,
					item.Event.Labels)
			isInSet := ds.set[item.Event.UUID] != nil
			// The update caused the document to exit the set.
			isRemove := !shouldBeInSet && isInSet
			// Is an add if it's a new document version or if the
			// document is new to the set.
			isAdd := !isRemove && shouldBeInSet && (isDocVersion || !isInSet)
			// Should the document state be included.
			withState := isAdd || (includeDoc && isDocVersion)
			// Is an update affecting a document that either should
			// be in the set, or is included by a document.
			isUpdate := shouldBeInSet || isIncluded

			var inclusionChanges []inclusionChange

			switch {
			case isAdd:
				inclusionChanges = ds.AddDocument(item.Event.UUID, item.Data.Document)
			case isRemove:
				inclusionChanges = ds.RemoveDocument(item.Event.UUID)
			}

			switch {
			case isUpdate:
				ds.emitItem(ctx, item, withState, shouldBeInSet)
			case isRemove:
				ds.emitRemove(ctx, item.Event.UUID)
			}

			// This is where we emit information about included
			// document when they enter or leave the extended
			// inclusion set. Inclusion changes are not tied to the
			// event triggering the inclusion change. But subsequent
			// changes to the included document will be treated as
			// normal isUpdate:s.
			err := ds.handleInclusionChanges(ctx, inclusionChanges)
			if err != nil {
				ds.emitErrorf(ctx, "processing",
					"failed to handle inclusions: %v", err)

				return
			}
		}
	}
}

func (ds *documentSet) isIncluded(docUUID uuid.UUID) (bool, bool) {
	inc := ds.included[docUUID]
	if inc == nil {
		return false, false
	}

	return inc.IncludeCount > 0, inc.GetDocCount > 0
}

func (ds *documentSet) emitErrorf(
	ctx context.Context,
	code string, format string, a ...any,
) {
	message := fmt.Sprintf(format, a...)

	ds.emitter.Error(ctx, &rsock.Error{
		ErrorCode:    code,
		ErrorMessage: message,
	})
}

func (ds *documentSet) emitItem(
	ctx context.Context,
	item DocumentStreamItem,
	withState bool,
	inSet bool,
) {
	update := rsock.DocumentUpdate{
		SetName:  ds.name,
		Event:    EventToRPC(item.Event),
		Included: !inSet,
	}

	if withState {
		update.Meta = DocumentMetaToRPC(&item.Data.Meta)

		extractors := ds.subsetExtractors
		if !inSet {
			extractors = ds.inclusionSubsets[item.Event.Type]
		}

		if len(extractors) > 0 {
			update.Subset = collectSubset(
				item.Data.Document, extractors)
		} else {
			update.Document = rpc_newsdoc.DocumentToRPC(
				item.Data.Document)
		}
	}

	ds.emitter.Update(ctx, &update)
}

func (ds *documentSet) emitRemove(
	ctx context.Context,
	docUUID uuid.UUID,
) {
	ds.emitter.Remove(ctx, &rsock.DocumentRemoved{
		SetName:      ds.name,
		DocumentUuid: docUUID.String(),
	})
}

func (ds *documentSet) handleInclusionChanges(
	ctx context.Context,
	changes []inclusionChange,
) error {
	// No changes, bail early.
	if len(changes) == 0 {
		return nil
	}

	var (
		metaUUIDS []uuid.UUID
		bulkPerm  = BulkCheckPermissionRequest{
			GranteeURIs: ds.getIdentity(),
			Permissions: []Permission{ReadPermission},
		}
		canRead  = make(map[uuid.UUID]bool, len(changes))
		loadDocs = make(map[uuid.UUID]*newsdoc.Document)
	)

	for _, change := range changes {
		if change.Change == changeRemoved {
			ds.emitRemove(ctx, change.UUID)

			continue
		}

		bulkPerm.UUIDs = append(bulkPerm.UUIDs, change.UUID)
	}

	access, err := ds.store.BulkCheckPermissions(ctx, bulkPerm)
	if err != nil {
		return fmt.Errorf("permissions check: %w", err)
	}

	for _, docUUID := range access {
		canRead[docUUID] = true
	}

	for _, change := range changes {
		if !canRead[change.UUID] {
			continue
		}

		metaUUIDS = append(metaUUIDS, change.UUID)

		if change.LoadDoc {
			// Looks a bit odd, but we're checking if the map entry
			// exists when building the bulk load request, and
			// populate it with a non nil value after load.
			loadDocs[change.UUID] = nil
		}
	}

	// No access to the added documents.
	if len(metaUUIDS) == 0 {
		return nil
	}

	// Load meta info for all added documents.
	meta, err := ds.store.BulkGetDocumentMeta(ctx, metaUUIDS)
	if err != nil {
		return fmt.Errorf("get document meta information: %w", err)
	}

	var bulkReq []BulkGetReference

	// Build the bulk document loading request.
	for docUUID, m := range meta {
		if _, load := loadDocs[docUUID]; !load {
			continue
		}

		bulkReq = append(bulkReq, BulkGetReference{
			UUID:    docUUID,
			Version: m.CurrentVersion,
		})
	}

	if len(bulkReq) > 0 {
		docs, err := ds.cache.GetDocuments(ctx, bulkReq)
		if err != nil {
			return fmt.Errorf("get documents: %w", err)
		}

		// Populate the loadDocs map with the actual loaded documents.
		for doc := range docs {
			loadDocs[doc.UUID] = &doc.Document
		}
	}

	batch := &rsock.InclusionBatch{
		SetName: ds.name,
	}

	// Build the inclusion batch from the meta information and loaded
	// documents.
	for _, change := range changes {
		if change.Change == changeRemoved {
			continue
		}

		docMeta := meta[change.UUID]

		state := &rsock.DocumentState{
			Uuid: change.UUID.String(),
			Meta: DocumentMetaToRPC(docMeta),
		}

		doc := loadDocs[change.UUID]
		if doc != nil {
			extractors := ds.inclusionSubsets[docMeta.Type]

			if len(extractors) > 0 {
				state.Subset = collectSubset(*doc, extractors)
			} else {
				state.Document = rpc_newsdoc.DocumentToRPC(*doc)
			}
		}

		batch.Documents = append(batch.Documents, &rsock.InclusionDocument{
			Uuid:  change.UUID.String(),
			State: state,
		})
	}

	ds.emitter.InclusionBatch(ctx, batch)

	return nil
}

// EvaluateDocument checks if the document matches the timespan and labels
// defined for the set.
func (ds *documentSet) EvaluateDocument(
	docType string,
	timespans []Timespan,
	labels []string,
) bool {
	if docType != ds.docType {
		return false
	}

	// If the document has fewer labels that what is required by the set,
	// the document labels can never have the required labels.
	if len(labels) < len(ds.labels) {
		return false
	}

	// If we have a set timespan for the set one of the document timestamps
	// must overlap it.
	if ds.timespan != nil {
		var tsMatch bool

		for i := range timespans {
			if !ds.timespan.Overlaps(timespans[i], 0) {
				continue
			}

			tsMatch = true

			break
		}

		if !tsMatch {
			return false
		}
	}

	// Check that the document label set contains all the required labels.
	for i := range ds.labels {
		if !slices.Contains(labels, ds.labels[i]) {
			return false
		}
	}

	return true
}

// setDocument is the state information for a primary document in the set.
type setDocument struct {
	IncludeRefs map[uuid.UUID]bool
}

// IncludedDocument tells us whether a set document included the document with
// the given UUID, the second boolean return value is true if the actual
// document for the included document should be loaded.
func (sd *setDocument) IncludedDocument(docUUID uuid.UUID) (bool, bool) {
	if sd == nil {
		return false, false
	}

	loadDoc, included := sd.IncludeRefs[docUUID]

	return included, loadDoc
}

// incDocument is the state information for a document that was included through
// a primary document in the set.
type incDocument struct {
	UUID uuid.UUID
	// IncludeCount is a reference counter for the document. An include
	// count of zero means that the document no longer should be included in
	// the set.
	IncludeCount int
	// GetDocCount is a counter for the number of include specs that were
	// annotated with doc.
	GetDocCount int
}

const (
	changeRemoved = 0
	changeAdd     = 1
)

type inclusionChange struct {
	UUID    uuid.UUID
	Change  int
	LoadDoc bool
}

// AddDocument that already has been evaluated as a match. Returns a list of
// documents that changed inclusion status based on this document.
func (ds *documentSet) AddDocument(
	docUUID uuid.UUID,
	doc newsdoc.Document,
) []inclusionChange {
	setDoc := setDocument{
		IncludeRefs: make(map[uuid.UUID]bool),
	}

	// Run extractors and collect the results into setDoc.IncludeRefs.
	for _, ex := range ds.includeExtractors {
		items := ex.Collect(doc)

		for _, item := range items {
			value, ok := item["uuid"]
			if !ok {
				continue
			}

			incUUID, err := uuid.Parse(value.Value)
			if err != nil {
				continue
			}

			setDoc.IncludeRefs[incUUID] = value.Annotation == "doc" || setDoc.IncludeRefs[incUUID]
		}
	}

	// Get hold of the previous set document information for this document.
	previous, wasInSet := ds.set[docUUID]

	// Update the set document with the new state.
	ds.set[docUUID] = &setDoc

	var changed []inclusionChange

	// Update the document set reference counters with positive changes.
	for incUUID, loadDoc := range setDoc.IncludeRefs {
		incRef, known := ds.included[incUUID]
		if !known {
			incRef = &incDocument{
				UUID: incUUID,
			}

			ds.included[incUUID] = incRef

			changed = append(changed, inclusionChange{
				UUID:   incUUID,
				Change: changeAdd,
			})
		}

		prevInc, prevLoad := previous.IncludedDocument(incUUID)

		if !prevInc {
			incRef.IncludeCount++
		}

		if loadDoc && !prevLoad {
			incRef.GetDocCount++
		}
	}

	// Set LoadDoc after all references have been tallied.
	for i := range changed {
		incRef := ds.included[changed[i].UUID]

		changed[i].LoadDoc = incRef.GetDocCount > 0
	}

	// If the document previously was in the set.
	if wasInSet {
		// Update the document set reference counters with negative
		// changes.
		for incUUID, didLoadDoc := range previous.IncludeRefs {
			incRef := ds.included[incUUID]

			included, loadDoc := setDoc.IncludedDocument(incUUID)

			if !included {
				incRef.IncludeCount--
			}

			if didLoadDoc && !loadDoc {
				incRef.GetDocCount--
			}

			if incRef.IncludeCount == 0 {
				delete(ds.included, incUUID)

				changed = append(changed, inclusionChange{
					UUID:   incUUID,
					Change: changeRemoved,
				})
			}
		}
	}

	return changed
}

// RemoveDocumet that didn't match the set. Returns a list of documents that
// changed inclusion status based on this document.
func (ds *documentSet) RemoveDocument(docUUID uuid.UUID) []inclusionChange {
	// Get hold of the current set document information for this document.
	current, wasInSet := ds.set[docUUID]
	if !wasInSet {
		return nil
	}

	var changed []inclusionChange

	// Update the document set reference counters with negative
	// changes.
	for incUUID, didLoadDoc := range current.IncludeRefs {
		incRef := ds.included[incUUID]

		incRef.IncludeCount--

		if didLoadDoc {
			incRef.GetDocCount--
		}

		if incRef.IncludeCount == 0 {
			delete(ds.included, incUUID)

			changed = append(changed, inclusionChange{
				UUID:   incUUID,
				Change: changeRemoved,
			})
		}
	}

	return changed
}

type SocketResponder interface {
	Respond(
		ctx context.Context,
		handle *CallHandle,
		resp *rsock.Response,
		final bool,
	)
}

func newDocumentSetHandle(
	ctx context.Context,
	call *CallHandle,
	responder SocketResponder,
) *documentSetHandle {
	ctx, cancel := context.WithCancel(ctx)

	return &documentSetHandle{
		call:      call,
		ctx:       ctx,
		cancel:    cancel,
		responder: responder,
	}
}

var _ DocumentSetEmitter = &documentSetHandle{}

type documentSetHandle struct {
	call      *CallHandle
	ctx       context.Context
	cancel    func()
	responder SocketResponder

	Set *documentSet
}

func (d *documentSetHandle) Close() {
	d.cancel()
}

// DocumentBatch implements DocumentSetEmitter.
func (d *documentSetHandle) DocumentBatch(ctx context.Context, msg *rsock.DocumentBatch) {
	d.responder.Respond(ctx, d.call, &rsock.Response{
		DocumentBatch: msg,
	}, false)
}

// Error implements DocumentSetEmitter.
func (d *documentSetHandle) Error(ctx context.Context, msg *rsock.Error) {
	d.responder.Respond(ctx, d.call, &rsock.Response{
		Error: msg,
	}, false)

	d.Close()
}

// InclusionBatch implements DocumentSetEmitter.
func (d *documentSetHandle) InclusionBatch(ctx context.Context, msg *rsock.InclusionBatch) {
	d.responder.Respond(ctx, d.call, &rsock.Response{
		InclusionBatch: msg,
	}, false)
}

// Remove implements DocumentSetEmitter.
func (d *documentSetHandle) Remove(ctx context.Context, msg *rsock.DocumentRemoved) {
	d.responder.Respond(ctx, d.call, &rsock.Response{
		Removed: msg,
	}, false)
}

// Update implements DocumentSetEmitter.
func (d *documentSetHandle) Update(ctx context.Context, msg *rsock.DocumentUpdate) {
	d.responder.Respond(ctx, d.call, &rsock.Response{
		DocumentUpdate: msg,
	}, false)
}
