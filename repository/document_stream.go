package repository

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephantine"
	"github.com/ttab/newsdoc"
	"golang.org/x/sync/errgroup"
)

type DocumentStreamData struct {
	Events          []Event
	DocumentVersion int64
	LastEvent       int64
	Document        newsdoc.Document
	Meta            DocumentMeta
}

type DocumentStreamItem struct {
	Event     Event
	Timespans []Timespan
	Data      *DocumentStreamData
}

// DocumentStreamHandlerFunc handles entries in the document stream. A handler
// should never block as handlers are processed serially.
type DocumentStreamHandlerFunc func(items []DocumentStreamItem)

const docStreamBufSize = 100

// docStreamBuf is a fixed-size circular buffer of DocumentStreamItems.
type docStreamBuf struct {
	items [docStreamBufSize]DocumentStreamItem
	head  int
	len   int
}

// put appends an item to the buffer, overwriting the oldest when full.
func (b *docStreamBuf) put(item DocumentStreamItem) {
	b.items[b.head] = item
	b.head = (b.head + 1) % docStreamBufSize

	if b.len < docStreamBufSize {
		b.len++
	}
}

// itemsFrom returns items with Event.ID > from. Returns (nil, false) if from
// is older than the earliest item in the buffer. Returns (nil, true) if the
// buffer is empty or from >= latest Event.ID.
func (b *docStreamBuf) itemsFrom(from int64) ([]DocumentStreamItem, bool) {
	if b.len == 0 {
		return nil, true
	}

	// Start is the index of the oldest item.
	start := (b.head - b.len + docStreamBufSize) % docStreamBufSize
	oldest := b.items[start].Event.ID
	newest := b.items[(b.head-1+docStreamBufSize)%docStreamBufSize].Event.ID

	if from < oldest {
		return nil, false
	}

	if from >= newest {
		return nil, true
	}

	var result []DocumentStreamItem

	for i := range b.len {
		idx := (start + i) % docStreamBufSize

		if b.items[idx].Event.ID > from {
			result = append(result, b.items[idx])
		}
	}

	return result, true
}

func NewDocumentStream(
	ctx context.Context,
	log *slog.Logger,
	registerer prometheus.Registerer,
	store DocStore,
) (*DocumentStream, error) {
	// Buffer of one to make sure that we always run another iteration of
	// log consume if we get one or more events during processing.
	ch := make(chan int64, 1)

	store.OnEventlog(ctx, ch)

	s := DocumentStream{
		ctx:       ctx,
		log:       log,
		consumers: make(map[int64]DocumentStreamHandlerFunc),
		lastID:    -1, // Start from last event.
		eventChan: ch,
		store:     store,
	}

	prom := elephantine.NewMetricsHelper(registerer)

	prom.CounterVec(&s.mEmitEvents, prometheus.CounterOpts{
		Name: "repository_docstream_emit_total",
		Help: "Counts when we 'start' to emit events, and 'ok' on success, 'error' on failure.",
	}, []string{"status"})

	prom.Gauge(&s.mPosition, prometheus.GaugeOpts{
		Name: "repository_docstream_position",
		Help: "The current eventlog position of the docstream.",
	})

	prom.Gauge(&s.mSubscribers, prometheus.GaugeOpts{
		Name: "repository_docstream_subscribers",
		Help: "Current docstream subscribers.",
	})

	err := prom.Err()
	if err != nil {
		return nil, fmt.Errorf("register metrics: %w", err)
	}

	go s.handleEvents()

	return &s, nil
}

type DocumentStream struct {
	ctx       context.Context
	log       *slog.Logger
	m         sync.RWMutex
	serial    int64
	consumers map[int64]DocumentStreamHandlerFunc
	buf       docStreamBuf

	mEmitEvents  *prometheus.CounterVec
	mPosition    prometheus.Gauge
	mSubscribers prometheus.Gauge

	// No mutex needed for these as they're only touched from the event
	// handling loop.
	lastID    int64
	eventChan chan int64
	store     DocStore
}

func (s *DocumentStream) consumerCount() int {
	s.m.RLock()
	count := len(s.consumers)
	s.m.RUnlock()

	return count
}

func (s *DocumentStream) handleEvents() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case id := <-s.eventChan:
			if s.consumerCount() == 0 {
				// Bump the lastID so that we don't do
				// unnecessary processing to the time when
				// nobody was connected.
				s.lastID = max(s.lastID, id)
				s.mPosition.Set(float64(s.lastID))

				continue
			}

			err := elephantine.CallWithRecover(s.ctx,
				func(ctx context.Context) error {
					return s.emitEvents(ctx, id)
				})
			if err != nil {
				s.log.Error("failed to emit document stream items",
					elephantine.LogKeyError, err)
			}
		}
	}
}

func (s *DocumentStream) emitEvents(
	ctx context.Context, observed int64,
) (outErr error) {
	if s.lastID == -1 {
		s.lastID = observed - 1
	}

	s.mEmitEvents.WithLabelValues("start").Inc()

	defer func() {
		if outErr != nil {
			s.mEmitEvents.WithLabelValues("error").Inc()

			return
		}

		s.mEmitEvents.WithLabelValues("ok").Inc()
	}()

	// Set a generous timeout, really shouldn't be anywhere close to being
	// this slow.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)

	defer cancel()

	events, err := s.store.GetEventlog(ctx, s.lastID, 50)
	if err != nil {
		return fmt.Errorf("fetch eventlog after %d: %w", s.lastID, err)
	}

	// Ignore events generated during restores.
	events = slices.DeleteFunc(events, func(e Event) bool {
		return e.SystemState == SystemStateRestoring
	})

	if len(events) == 0 {
		return nil
	}

	data := make(map[uuid.UUID]*DocumentStreamData)

	for _, evt := range events {
		item, ok := data[evt.UUID]
		if !ok {
			item = &DocumentStreamData{}
			data[evt.UUID] = item
		}

		item.Events = append(item.Events, evt)
		item.LastEvent = evt.ID
	}

	docIDs := slices.Collect(maps.Keys(data))

	var bulkGet []BulkGetReference

	for docUUID := range data {
		// We always fetch the latest version of the document.
		bulkGet = append(bulkGet, BulkGetReference{
			UUID: docUUID,
		})
	}

	grp, gCtx := errgroup.WithContext(ctx)

	grp.Go(func() error {
		metaInfo, err := s.store.BulkGetDocumentMeta(gCtx, docIDs)
		if err != nil {
			return fmt.Errorf("get meta information for events: %w", err)
		}

		for docUUID, item := range data {
			meta := metaInfo[docUUID]

			if meta != nil {
				item.Meta = *meta
			}
		}

		return nil
	})

	grp.Go(func() error {
		docRes, err := s.store.BulkGetDocuments(gCtx, bulkGet)
		if err != nil {
			return fmt.Errorf("get documents for events: %w", err)
		}

		for _, docItem := range docRes {
			item, ok := data[docItem.UUID]
			if !ok {
				continue
			}

			item.Document = docItem.Document
			item.DocumentVersion = docItem.Version
		}

		return nil
	})

	err = grp.Wait()
	if err != nil {
		return fmt.Errorf("load data: %w", err)
	}

	items := make([]DocumentStreamItem, 0, len(events))

	for _, evt := range events {
		items = append(items, DocumentStreamItem{
			Event:     evt,
			Timespans: TimespansFromTuples(evt.Timespans),
			Data:      data[evt.UUID],
		})
	}

	s.m.Lock()

	for i := range items {
		s.buf.put(items[i])

		s.lastID = items[i].Event.ID

		s.mPosition.Set(float64(s.lastID))
	}

	for _, h := range s.consumers {
		h(items)
	}

	s.m.Unlock()

	return nil
}

// Subscribe registers a handler for new document stream items. The handler
// will be automatically unregistered when ctx or the stream context is
// cancelled.
func (s *DocumentStream) Subscribe(
	ctx context.Context, handler DocumentStreamHandlerFunc,
) {
	s.m.Lock()

	s.serial++

	id := s.serial

	s.consumers[id] = handler
	s.mSubscribers.Set(float64(len(s.consumers)))

	s.m.Unlock()

	go s.unsubscribeOnCancel(ctx, id)
}

// SubscribeFrom registers a handler and replays buffered items with
// Event.ID > from before delivering live events. Returns false if the
// requested position is no longer available in the buffer. The handler will be
// automatically unregistered when ctx or the stream context is cancelled.
func (s *DocumentStream) SubscribeFrom(
	ctx context.Context, from int64, handler DocumentStreamHandlerFunc,
) bool {
	s.m.Lock()

	replay, ok := s.buf.itemsFrom(from)
	if !ok {
		s.m.Unlock()

		return false
	}

	if len(replay) > 0 {
		handler(replay)
	}

	s.serial++

	id := s.serial

	s.consumers[id] = handler
	s.mSubscribers.Set(float64(len(s.consumers)))

	s.m.Unlock()

	go s.unsubscribeOnCancel(ctx, id)

	return true
}

func (s *DocumentStream) unsubscribeOnCancel(ctx context.Context, id int64) {
	select {
	case <-ctx.Done():
	case <-s.ctx.Done():
	}

	s.m.Lock()

	delete(s.consumers, id)
	s.mSubscribers.Set(float64(len(s.consumers)))

	s.m.Unlock()
}
