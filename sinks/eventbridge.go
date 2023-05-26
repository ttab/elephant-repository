package sinks

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/ttab/elephant/repository"
	"github.com/ttab/elephantine"
	"golang.org/x/exp/slog"
)

type EventBridgeEventPutter interface {
	PutEvents(
		ctx context.Context, params *eventbridge.PutEventsInput,
		optFns ...func(*eventbridge.Options),
	) (*eventbridge.PutEventsOutput, error)
}

const EventBridgeSizeLimit = 256 * 1024

type EventBridgeOptions struct {
	Logger       *slog.Logger
	EventBusName string
}

func NewEventBridge(
	client EventBridgeEventPutter, opts EventBridgeOptions,
) *EventBridge {
	return &EventBridge{
		logger: opts.Logger,
		client: client,
	}
}

type EventBridge struct {
	logger   *slog.Logger
	client   EventBridgeEventPutter
	eventBus string
}

// SinkName implements EventSink.
func (*EventBridge) SinkName() string {
	return "aws-eventbridge"
}

// SendEvents implements EventSink.
func (eb *EventBridge) SendEvents(
	ctx context.Context, evts []EventDetail,
	skipMetric IncrementSkipMetricFunc,
) (int, error) {
	var (
		processed int
		abortErr  error
	)

	remaining := evts

	for abortErr == nil {
		batch, nextIdx, err := eb.eventBridgeBatch(remaining, skipMetric)
		if err != nil {
			abortErr = fmt.Errorf("batching failed: %w", err)
		}

		if len(batch) == 0 {
			break
		}

		out, err := eb.client.PutEvents(ctx, &eventbridge.PutEventsInput{
			Entries: batch.AsEntries(),
		})
		if err != nil {
			return processed, fmt.Errorf("put request failed: %w", err)
		}

		for i, res := range out.Entries {
			if res.ErrorCode != nil {
				msg := "unknown error"

				if res.ErrorMessage != nil {
					msg = *res.ErrorMessage
				}

				eb.logger.Error("event rejected",
					elephantine.LogKeyEventID, batch[i].EventID,
					elephantine.LogKeyError, fmt.Sprintf(
						"%s: %s",
						*res.ErrorCode, msg))

				skipMetric(*batch[i].DetailType, "rejected")
			}
		}

		processed += nextIdx

		if len(remaining) == nextIdx {
			break
		}

		remaining = remaining[nextIdx:]
	}

	if abortErr != nil {
		return processed, abortErr
	}

	return processed, nil
}

type ebEntry struct {
	types.PutEventsRequestEntry

	EventID int64
}

type batchedEvents []ebEntry

func (be batchedEvents) AsEntries() []types.PutEventsRequestEntry {
	entries := make([]types.PutEventsRequestEntry, len(be))

	for i := range be {
		entries[i] = be[i].PutEventsRequestEntry
	}

	return entries
}

func (eb *EventBridge) eventBridgeBatch(
	evts []EventDetail, incr IncrementSkipMetricFunc,
) (batchedEvents, int, error) {
	var (
		entries   []ebEntry
		totalSize int
	)

	for i, evt := range evts {
		if evt.Event.Event == repository.TypeEventIgnored {
			continue
		}

		detailData, err := json.Marshal(evt)
		if err != nil {
			return entries, i, fmt.Errorf(
				"failed to marshal event detail: %w", err)
		}

		e := types.PutEventsRequestEntry{
			Time:       aws.Time(time.Now()),
			Source:     aws.String("elephant"),
			DetailType: aws.String(string(evt.Event.Event)),
			Detail:     aws.String(string(detailData)),
		}

		if eb.eventBus != "" {
			e.EventBusName = aws.String(eb.eventBus)
		}

		size := 14 // Includes the timestamp

		size += len(*e.Source)
		size += len(*e.DetailType)
		size += len(detailData)

		if size > EventBridgeSizeLimit {
			eb.logger.Error("skipping oversized event",
				elephantine.LogKeyEventID, evt.Event.ID,
				elephantine.LogKeyEventType, evt.Event.Type,
			)

			incr(evt.Event.Type, "message_size")

			continue
		}

		totalSize += size

		if totalSize > EventBridgeSizeLimit {
			return entries, i, nil
		}

		entries = append(entries, ebEntry{
			EventID:               evt.Event.ID,
			PutEventsRequestEntry: e,
		})
	}

	return entries, len(evts), nil
}

var _ EventSink = &EventBridge{}
