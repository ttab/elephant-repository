package repository

import (
	"context"
	"fmt"
	"slices"

	"github.com/google/uuid"
	rpc_newsdoc "github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repositorysocket"
	rsock "github.com/ttab/elephant-api/repositorysocket"
	"github.com/ttab/newsdoc"
)

type DocumentSetEmitter interface {
	Update(ctx context.Context, msg *rsock.DocumentUpdate)
	InclusionBatch(ctx context.Context, msg *rsock.InclusionBatch)
	Remove(ctx context.Context, msg *rsock.DocumentRemoved)
}

func newDocumentSet(
	name string,
	timespan *Timespan,
	labels []string,
	includeExtractors []*newsdoc.ValueExtractor,
	cache *DocCache,
	store DocStore,
	emitter DocumentSetEmitter,
) *documentSet {
	slices.Sort(labels)

	ds := documentSet{
		name:              name,
		timespan:          timespan,
		labels:            slices.Compact(labels),
		set:               make(map[uuid.UUID]*setDocument),
		included:          make(map[uuid.UUID]*incDocument),
		includeExtractors: includeExtractors,
		process:           make(chan DocumentStreamItem, 128),
		cache:             cache,
		store:             store,
		emitter:           emitter,
	}

	return &ds
}

// documentSet tracks documents that are included in a set. See SocketSession.
type documentSet struct {
	name     string
	timespan *Timespan
	labels   []string

	cache   *DocCache
	store   DocStore
	emitter DocumentSetEmitter

	set      map[uuid.UUID]*setDocument
	included map[uuid.UUID]*incDocument

	includeExtractors []*newsdoc.ValueExtractor

	process chan DocumentStreamItem
	oosErr  bool
}

func (ds *documentSet) HandleDocStreamItem(item DocumentStreamItem) {
	select {
	case ds.process <- item:
	default:
		ds.oosErr = true
	}
}

func (ds *documentSet) ProcessingLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-ds.process:
			isDelete := item.Event.Event == TypeDeleteDocument
			isDocVersion := item.Event.Event == TypeDocumentVersion
			matchesSet := ds.EvaluateDocument(item.Timespans, item.Event.Labels)
			// Deletes always exit the set.
			shouldBeInSet := !isDelete && matchesSet
			isInSet := ds.set[item.Event.UUID] != nil
			isRemove := !shouldBeInSet && isInSet
			// Is an add if it's a new document version or if the
			// document is new to the set.
			isAdd := shouldBeInSet && (isDocVersion || !isInSet)
			isIncluded := ds.included[item.Event.UUID].IncludeCount > 0
			// Update to a document that either should be in the
			// set, or is included by a document.
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
				ds.emitItem(ctx, item, isAdd)
			case isRemove:
				ds.emitRemove(ctx, item.Event.UUID)
			}

			ds.handleInclusionChanges(ctx, inclusionChanges)
		}
	}
}

func (ds *documentSet) emitItem(
	ctx context.Context,
	item DocumentStreamItem,
	withState bool,
) {
	update := rsock.DocumentUpdate{
		SetName: ds.name,
		Event:   EventToRPC(item.Event),
	}

	if withState {
		update.Document = rpc_newsdoc.DocumentToRPC(item.Data.Document)
		update.Meta = DocumentMetaToRPC(&item.Data.Meta)
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
	var (
		metaUUIDS []uuid.UUID
		loadDocs  = make(map[uuid.UUID]*newsdoc.Document)
	)

	for _, change := range changes {
		if change.Change == changeRemoved {
			ds.emitRemove(ctx, change.UUID)

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

	batch := repositorysocket.InclusionBatch{
		SetName: ds.name,
	}

	// Build the inclusion batch from the meta information and loaded
	// documents.
	for _, change := range changes {
		if change.Change == changeRemoved {
			continue
		}

		state := repositorysocket.DocumentState{
			Meta: DocumentMetaToRPC(meta[change.UUID]),
		}

		doc := loadDocs[change.UUID]
		if doc != nil {
			state.Document = rpc_newsdoc.DocumentToRPC(*doc)
		}

		batch.Documents = append(batch.Documents, &state)
	}

	ds.emitter.InclusionBatch(ctx, &batch)

	return nil
}

// EvaluateDocument checks if the document matches the timespan and labels
// defined for the set.
func (ds *documentSet) EvaluateDocument(timespans []Timespan, labels []string) bool {
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
