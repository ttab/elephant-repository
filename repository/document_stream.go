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

// DocumentStreamHandler handles a entry in the document stream. A handler
// should never block as as handlers are processed serially.
type DocumentStreamHandler func(item DocumentStreamItem)

func NewDocumentStream(
	ctx context.Context,
	log *slog.Logger,
	store DocStore,
) *DocumentStream {
	// Buffer of one to make sure that we always run another iteration of
	// log consume if we get one or more events during processing.
	ch := make(chan int64, 1)

	store.OnEventlog(ctx, ch)

	s := DocumentStream{
		ctx:       ctx,
		log:       log,
		consumers: make(map[int64]DocumentStreamHandler),
		lastID:    -1, // Start from last event.
		eventChan: ch,
		store:     store,
	}

	go s.handleEvents()

	return &s
}

type DocumentStream struct {
	ctx       context.Context
	log       *slog.Logger
	m         sync.RWMutex
	serial    int64
	consumers map[int64]DocumentStreamHandler

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

func (s *DocumentStream) emitEvents(ctx context.Context, observed int64) error {
	if s.lastID == -1 {
		s.lastID = observed - 1
	}

	expectedItems := observed - s.lastID

	// Set a generous timeout, really shouldn't be anywhere close to being
	// this slow.
	ctx, cancel := context.WithTimeout(ctx,
		2*time.Duration(expectedItems)*time.Second)
	defer cancel()

	events, err := s.store.GetEventlog(ctx, s.lastID, 50)
	if err != nil {
		return fmt.Errorf("fetch eventlog: %w", err)
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

	s.m.RLock()

	for _, evt := range events {
		item := DocumentStreamItem{
			Event:     evt,
			Timespans: TimespansFromTuples(evt.Timespans),
			Data:      data[evt.UUID],
		}

		for _, h := range s.consumers {
			h(item)
		}
	}

	s.m.RUnlock()

	return nil
}

// Subscribe calls handler with new items until the subscription context is
// cancelled or the document stream is stopped.
func (s *DocumentStream) Subscribe(ctx context.Context, handler DocumentStreamHandler) {
	s.m.Lock()

	s.serial++

	id := s.serial
	s.consumers[id] = handler

	s.m.Unlock()

	select {
	case <-ctx.Done():
	case <-s.ctx.Done():
	}

	s.m.Lock()
	delete(s.consumers, id)
	s.m.Unlock()
}
