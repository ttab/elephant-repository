package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
)

type EventType string

const (
	TypeEventIgnored    EventType = ""
	TypeDocumentVersion EventType = "document"
	TypeNewStatus       EventType = "status"
	TypeACLUpdate       EventType = "acl"
	TypeDeleteDocument  EventType = "delete_document"
	TypeRestoreFinished EventType = "restore_finished"
)

type Event struct {
	ID           int64      `json:"id"`
	Event        EventType  `json:"event"`
	UUID         uuid.UUID  `json:"uuid"`
	Timestamp    time.Time  `json:"timestamp"`
	Updater      string     `json:"updater"`
	Type         string     `json:"type"`
	Language     string     `json:"language"`
	OldLanguage  string     `json:"old_language,omitempty"`
	MainDocument *uuid.UUID `json:"main_document,omitempty"`
	Version      int64      `json:"version,omitempty"`
	StatusID     int64      `json:"status_id,omitempty"`
	Status       string     `json:"status,omitempty"`
	ACL          []ACLEntry `json:"acl,omitempty"`
	SystemState  string     `json:"system_state,omitempty"`
}

type replicationErrorCode string

const (
	errorCodeReplicationAlreadyActive replicationErrorCode = "already-active"
)

type pgReplicationError struct {
	Code replicationErrorCode
}

func (err pgReplicationError) Error() string {
	return fmt.Sprintf("error code: %q", err.Code)
}

func isReplicationErrorCode(err error, code replicationErrorCode) bool {
	var re pgReplicationError

	if !errors.As(err, &re) {
		return false
	}

	return re.Code == code
}

type PGReplication struct {
	pool     *pgxpool.Pool
	dbURI    string
	logger   *slog.Logger
	slotName string

	cancel  func()
	started chan bool
	stopped chan struct{}

	restarts prometheus.Counter
	timeouts prometheus.Counter
	events   *prometheus.CounterVec
}

func NewPGReplication(
	logger *slog.Logger,
	pool *pgxpool.Pool,
	dbURI string,
	slotName string,
	metricsRegisterer prometheus.Registerer,
) (*PGReplication, error) {
	if metricsRegisterer == nil {
		metricsRegisterer = prometheus.DefaultRegisterer
	}

	restarts := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "elephant_replicator_restarts_total",
			Help: "Number of times the replicator has restarted.",
		},
	)
	if err := metricsRegisterer.Register(restarts); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	timeouts := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "elephant_replicator_wal_timeouts__total",
			Help: "Number of times we have timed out waiting for ReceiveMessage.",
		},
	)
	if err := metricsRegisterer.Register(timeouts); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	events := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_replicator_events_total",
			Help: "Number of received replicator events.",
		},
		[]string{"type", "relation"},
	)
	if err := metricsRegisterer.Register(events); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	return &PGReplication{
		pool:     pool,
		logger:   logger,
		dbURI:    dbURI,
		slotName: slotName,
		restarts: restarts,
		timeouts: timeouts,
		events:   events,
		started:  make(chan bool, 1),
		stopped:  make(chan struct{}),
	}, nil
}

func (pr *PGReplication) Run(ctx context.Context) {
	const restartWaitSeconds = 10

	rCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	pr.cancel = cancel

	defer close(pr.stopped)

	for {
		err := pr.startReplication(rCtx)

		switch {
		case isReplicationErrorCode(err, errorCodeReplicationAlreadyActive):
			// Ignoring this, it's not actually an error, just check
			// again later.
		case errors.Is(err, context.Canceled):
			return
		case err != nil:
			pr.restarts.Inc()

			pr.logger.ErrorContext(
				rCtx, "replication error, restarting",
				elephantine.LogKeyError, err,
				elephantine.LogKeyDelay, slog.DurationValue(restartWaitSeconds),
			)
		}

		select {
		case <-time.After(restartWaitSeconds * time.Second):
		case <-rCtx.Done():
			return
		}
	}
}

// Started emits true as a signal every time replication has started.
func (pr *PGReplication) Started() <-chan bool {
	return pr.started
}

func (pr *PGReplication) Stop() {
	if pr.cancel == nil {
		return
	}

	pr.cancel()

	select {
	case <-time.After(10 * time.Second):
	case <-pr.stopped:
	}
}

func (pr *PGReplication) startReplication(
	ctx context.Context,
) error {
	nconn, err := pr.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to get a connection from the connection pool: %w", err)
	}

	defer nconn.Release()

	slotInfo, err := pr.getSlotInfo(ctx, nconn, pr.slotName)
	if err != nil {
		return fmt.Errorf("failed to get slot information: %w", err)
	}

	if slotInfo.Active {
		// Informing the caller that replication already is running.
		return pgReplicationError{
			Code: errorCodeReplicationAlreadyActive,
		}
	}

	nconn.Release()

	replConnString, err := pg.SetConnStringVariables(
		pr.dbURI, url.Values{
			"replication": []string{"database"},
		})
	if err != nil {
		return fmt.Errorf("failed to create connection string: %w", err)
	}

	conn, err := pgconn.Connect(ctx, replConnString)
	if err != nil {
		return fmt.Errorf(
			"failed to create replication connection: %w", err)
	}

	defer conn.Close(context.Background())

	if !slotInfo.Exists {
		_, err = pglogrepl.CreateReplicationSlot(ctx,
			conn, pr.slotName, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{
				Mode: pglogrepl.LogicalReplication,
			})
		if err != nil {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}
	}

	err = pglogrepl.StartReplication(
		ctx, conn, pr.slotName, 0,
		pglogrepl.StartReplicationOptions{PluginArgs: []string{
			"proto_version '1'", "publication_names 'eventlog'",
		}})
	if isPgErrorCode(err, SQLCodeObjectInUse) {
		// Another instance has grabbed the replication slot between our
		// initial check and now. Informing the caller that replication
		// already is running.
		return pgReplicationError{
			Code: errorCodeReplicationAlreadyActive,
		}
	} else if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	pr.logger.Info("replication started")
	select {
	case pr.started <- true:
	default:
	}

	err = pr.replicationLoop(ctx, conn)
	if err != nil {
		return err
	}

	return nil
}

// https://www.postgresql.org/docs/current/errcodes-appendix.html#ERRCODES-TABLE
const (
	SQLCodeObjectInUse = "55006"
)

// isPgErrorCode checks if an error has a specific error code.
// TODO: Move to elephantine.
func isPgErrorCode(err error, code string) bool {
	if err == nil {
		return false
	}

	var pgerr *pgconn.PgError

	ok := errors.As(err, &pgerr)
	if !ok {
		return false
	}

	return pgerr.Code == code
}

func (pr *PGReplication) replicationLoop(
	ctx context.Context,
	conn *pgconn.PgConn,
) error {
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	dec := NewTupleDecoder()

	var clientXLogPos pglogrepl.LSN

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(
				ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: clientXLogPos,
				})
			if err != nil {
				return fmt.Errorf(
					"failed to send standby status update: %w", err)
			}

			nextStandbyMessageDeadline = time.Now().Add(
				standbyMessageTimeout)
		}

		rcvCtx, cancel := context.WithDeadline(
			ctx, nextStandbyMessageDeadline)
		rawMsg, err := conn.ReceiveMessage(rcvCtx)

		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				pr.timeouts.Inc()

				continue
			}

			return fmt.Errorf("failed to receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("received wal error (%s %s): %s",
				errMsg.Code,
				errMsg.Severity,
				errMsg.Message)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			pr.logger.Error("received unexpected message",
				elephantine.LogKeyMessage, fmt.Sprintf("%T", rawMsg))

			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(
				msg.Data[1:])
			if err != nil {
				return fmt.Errorf(
					"failed to parse keepalive message: %w", err)
			}

			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf(
					"failed to parse replication message: %w", err)
			}

			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf(
					"failed to parse logical replication message: %w", err)
			}

			err = pr.handleReplicationMessage(dec, logicalMsg)
			if err != nil {
				return fmt.Errorf(
					"failed to handle replication message: %w", err)
			}

			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
}

const replicaIdentityFull = 'O'

type replMessage struct {
	Table     string
	NewValues map[string]interface{}
	OldValues map[string]interface{}
}

func (pr *PGReplication) handleReplicationMessage(
	dec *TupleDecoder,
	msg pglogrepl.Message,
) error {
	switch logicalMsg := msg.(type) {
	case *pglogrepl.RelationMessage:
		pr.events.WithLabelValues(
			"relation", logicalMsg.RelationName,
		).Inc()

		dec.RegisterRelation(logicalMsg)
	case *pglogrepl.InsertMessage:
		rel, values, err := dec.DecodeValues(
			logicalMsg.RelationID, logicalMsg.Tuple)
		if err != nil {
			return fmt.Errorf(
				"failed to decode values: %w", err)
		}

		pr.events.WithLabelValues(
			"insert", rel.RelationName,
		).Inc()

		if rel.Namespace != "public" {
			return nil
		}

		return pr.handleMessage(replMessage{
			Table:     rel.RelationName,
			NewValues: values,
		})

	case *pglogrepl.UpdateMessage:
		rel, ok := dec.GetRelation(logicalMsg.RelationID)
		if !ok {
			return fmt.Errorf("got message for unknown relation")
		}

		pr.events.WithLabelValues(
			"update", rel.RelationName,
		).Inc()

		if rel.Namespace != "public" {
			return nil
		}

		switch rel.RelationName {
		case "document":
		case "status_heads":
		default:
			// Ignore updates for everything else.
			return nil
		}

		_, values, err := dec.DecodeValues(
			logicalMsg.RelationID, logicalMsg.NewTuple)
		if err != nil {
			return fmt.Errorf(
				"failed to decode values: %w", err)
		}

		msg := replMessage{
			Table:     rel.RelationName,
			NewValues: values,
		}

		if logicalMsg.OldTupleType != replicaIdentityFull {
			return fmt.Errorf(
				"replica identity for the relation %q is not set to full",
				rel.RelationName)
		}

		_, oldValues, err := dec.DecodeValues(
			logicalMsg.RelationID, logicalMsg.OldTuple)
		if err != nil {
			return fmt.Errorf(
				"failed to decode old values: %w", err)
		}

		msg.OldValues = oldValues

		oldSystState, hadSysState := oldValues["system_state"].(string)
		_, hasSysState := values["system_state"].(string)

		// System state is set on document, status_heads, acl_audit. But
		// the only move out of a system state that we want to track is
		// that of document moving out of SystemStateRestoring. Ignore
		// everything else.
		if hadSysState && !hasSysState && !(oldSystState == SystemStateRestoring &&
			rel.RelationName == "document") {
			return nil
		}

		return pr.handleMessage(msg)

	default:
		return nil
	}

	return nil
}

func (pr *PGReplication) handleMessage(msg replMessage) error {
	var evt Event

	switch msg.Table {
	case "document":
		e, err := parseDocumentMessage(pr.logger, msg)
		if err != nil {
			return fmt.Errorf(
				"failed to parse document table message: %w", err)
		}

		evt = e
	case "status_heads":
		e, err := parseStatusHeadsMessage(pr.logger, msg)
		if err != nil {
			return fmt.Errorf(
				"failed to parse status_heads table message: %w", err)
		}

		evt = e
	case "delete_record":
		e, err := parseDeleteMessage(msg)
		if err != nil {
			return fmt.Errorf(
				"failed to parse delete_record table message: %w", err)
		}

		evt = e
	case "acl_audit":
		e, err := parseACLMessage(msg)
		if err != nil {
			return fmt.Errorf(
				"failed to parse acl_audit table message: %w", err)
		}

		evt = e
	default:
		return nil
	}

	if evt.Event == TypeEventIgnored {
		return nil
	}

	return pr.recordEvent(evt)
}

func parseDeleteMessage(msg replMessage) (Event, error) {
	evt := Event{
		Event: TypeDeleteDocument,
	}

	err := readMessageColumns(msg, &evt)
	if err != nil {
		return Event{}, err
	}

	created, ok := msg.NewValues["created"].(time.Time)
	if !ok {
		return Event{}, fmt.Errorf("failed to extract created time")
	}

	evt.Timestamp = created

	creator, ok := msg.NewValues["creator_uri"].(string)
	if !ok {
		return Event{}, fmt.Errorf("failed to extract creator_uri")
	}

	evt.Updater = creator

	return evt, nil
}

func parseACLMessage(msg replMessage) (Event, error) {
	evt := Event{
		Event: TypeACLUpdate,
	}

	err := readMessageColumns(msg, &evt)
	if err != nil {
		return Event{}, err
	}

	updated, ok := msg.NewValues["updated"].(time.Time)
	if !ok {
		return Event{}, fmt.Errorf("failed to extract updated time")
	}

	evt.Timestamp = updated

	updater, ok := msg.NewValues["updater_uri"].(string)
	if !ok {
		return Event{}, fmt.Errorf("failed to extract updater_uri")
	}

	evt.Updater = updater

	stateSlice, ok := msg.NewValues["state"].([]interface{})
	if !ok {
		return Event{}, fmt.Errorf("failed to extract state slice")
	}

	for i := range stateSlice {
		aclMap, ok := stateSlice[i].(map[string]interface{})
		if !ok {
			return Event{}, fmt.Errorf("failed to extract ACL entry map")
		}

		aclURI, ok := aclMap["uri"].(string)
		if !ok {
			return Event{}, fmt.Errorf("failed to extract ACL uri")
		}

		permSlice, ok := aclMap["permissions"].([]interface{})
		if !ok {
			return Event{}, fmt.Errorf(
				"failed to extract ACL permission slice")
		}

		perms := make([]string, len(permSlice))

		for j := range permSlice {
			p, ok := permSlice[j].(string)
			if !ok {
				return Event{}, fmt.Errorf("failed to extract ACL permission")
			}

			perms[j] = p
		}

		evt.ACL = append(evt.ACL, ACLEntry{
			URI:         aclURI,
			Permissions: perms,
		})
	}

	return evt, nil
}

func parseStatusHeadsMessage(log *slog.Logger, msg replMessage) (Event, error) {
	evt := Event{
		Event: TypeNewStatus,
	}

	err := readMessageColumns(msg, &evt)
	if err != nil {
		return Event{}, err
	}

	id, ok := msg.NewValues["current_id"].(int64)
	if !ok {
		return Event{}, fmt.Errorf("failed to extract current id")
	}

	evt.StatusID = id

	if msg.OldValues != nil {
		oldID, ok := msg.OldValues["current_id"].(int64)
		if !ok {
			return Event{}, fmt.Errorf("failed to extract old id")
		}

		// New IDs have to be higher, bail, something odd is going on.
		if id <= oldID {
			log.Warn("received a status replication update that didn't increment the status ID",
				elephantine.LogKeyDocumentUUID, msg.NewValues["uuid"],
				elephantine.LogKeyDocumentStatus, msg.NewValues["name"],
				elephantine.LogKeyDocumentStatusID, id,
			)

			return Event{}, nil
		}
	}

	updated, ok := msg.NewValues["updated"].(time.Time)
	if !ok {
		return Event{}, fmt.Errorf("failed to extract updated time")
	}

	evt.Timestamp = updated

	updater, ok := msg.NewValues["updater_uri"].(string)
	if !ok {
		return Event{}, fmt.Errorf("failed to extract updater_uri")
	}

	evt.Updater = updater

	docVersion, ok := msg.NewValues["version"].(int64)
	if ok {
		evt.Version = docVersion
	}

	name, ok := msg.NewValues["name"].(string)
	if !ok {
		return Event{}, fmt.Errorf("failed to extract name")
	}

	evt.Status = name

	return evt, nil
}

func readMessageColumns(msg replMessage, evt *Event) error {
	docUUID, ok := msg.NewValues["uuid"].([16]uint8)
	if !ok {
		return fmt.Errorf("failed to extract uuid")
	}

	evt.UUID = docUUID

	docType, ok := msg.NewValues["type"].(string)
	if ok {
		evt.Type = docType
	}

	docLanguage, ok := msg.NewValues["language"].(string)
	if ok {
		evt.Language = docLanguage
	}

	oldLanguage, ok := msg.OldValues["language"].(string)
	if ok && oldLanguage != docLanguage {
		evt.OldLanguage = oldLanguage
	}

	docMain, ok := msg.NewValues["main_doc"].([16]uint8)
	if ok {
		mu := uuid.UUID(docMain)

		evt.MainDocument = &mu
	}

	systemState, ok := msg.NewValues["system_state"].(string)
	if ok {
		evt.SystemState = systemState
	}

	return nil
}

func parseDocumentMessage(log *slog.Logger, msg replMessage) (Event, error) {
	evt := Event{
		Event: TypeDocumentVersion,
	}

	err := readMessageColumns(msg, &evt)
	if err != nil {
		return Event{}, err
	}

	if msg.OldValues != nil {
		oldSystState, hadSysState := msg.OldValues["system_state"].(string)
		_, hasSysState := msg.NewValues["system_state"].(string)

		// Leaving a system state is an update where we only change the
		// system state. We collect all values for the event as usual,
		// but change the event to a "restore_finished" event.
		if hadSysState && !hasSysState {
			switch oldSystState {
			case SystemStateRestoring:
				evt.Event = TypeRestoreFinished
			default:
				return Event{}, fmt.Errorf(
					"left unknown system state %q", oldSystState)
			}
		}
	}

	sysLock, _ := msg.NewValues["system_state"].(string)

	if sysLock == SystemStateDeleting {
		// Deletes are tracked through delete_record.
		return Event{Event: TypeEventIgnored}, nil
	}

	version, ok := msg.NewValues["current_version"].(int64)
	if !ok {
		return Event{}, fmt.Errorf("failed to extract current version")
	}

	// Just a placeholder used to start the restore, should not be reflected
	// in the eventlog.
	if sysLock == SystemStateRestoring && version == 0 {
		return Event{Event: TypeEventIgnored}, nil
	}

	evt.Version = version

	if evt.Event != TypeRestoreFinished && msg.OldValues != nil {
		oldVersion, ok := msg.OldValues["current_version"].(int64)
		if !ok {
			return Event{}, fmt.Errorf("failed to extract old version")
		}

		// New versions have to be higher, bail, something odd
		// is going on.
		if version <= oldVersion {
			log.Warn("received a document replication update that didn't increment the version",
				elephantine.LogKeyDocumentUUID, msg.NewValues["uuid"],
				elephantine.LogKeyDocumentVersion, version,
			)

			return Event{}, nil
		}
	}

	updated, ok := msg.NewValues["updated"].(time.Time)
	if !ok {
		return Event{}, fmt.Errorf("failed to extract updated time")
	}

	evt.Timestamp = updated

	updater, ok := msg.NewValues["updater_uri"].(string)
	if !ok {
		return Event{}, fmt.Errorf("failed to extract updater_uri")
	}

	evt.Updater = updater

	docType, ok := msg.NewValues["type"].(string)
	if !ok {
		return Event{}, fmt.Errorf("failed to extract updater_uri")
	}

	evt.Type = docType

	docUUID, ok := msg.NewValues["uuid"].([16]uint8)
	if !ok {
		return Event{}, fmt.Errorf("failed to extract uuid")
	}

	evt.UUID = docUUID

	return evt, nil
}

func (pr *PGReplication) recordEvent(evt Event) error {
	ctx := context.Background()

	tx, err := pr.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	defer pg.SafeRollback(ctx, pr.logger, tx, "eventlog insert")

	var mainDocUUID pgtype.UUID

	if evt.MainDocument != nil {
		mainDocUUID.Bytes = *evt.MainDocument
		mainDocUUID.Valid = true
	}

	row := postgres.InsertIntoEventLogParams{
		Event:       string(evt.Event),
		UUID:        evt.UUID,
		Timestamp:   pg.Time(evt.Timestamp),
		Updater:     pg.TextOrNull(evt.Updater),
		Type:        pg.TextOrNull(evt.Type),
		Version:     pg.BigintOrNull(evt.Version),
		Status:      pg.TextOrNull(evt.Status),
		StatusID:    pg.BigintOrNull(evt.StatusID),
		MainDoc:     mainDocUUID,
		Language:    pg.TextOrNull(evt.Language),
		OldLanguage: pg.TextOrNull(evt.OldLanguage),
		SystemState: pg.TextOrNull(evt.SystemState),
	}

	if evt.ACL != nil {
		data, err := json.Marshal(evt.ACL)
		if err != nil {
			return fmt.Errorf("failed to marshal ACL: %w", err)
		}

		row.Acl = data
	}

	q := postgres.New(tx)

	id, err := q.InsertIntoEventLog(ctx, row)
	if err != nil {
		return fmt.Errorf("failed to insert eventlog entry: %w", err)
	}

	err = notifyEventlog(ctx, pr.logger, q, id)
	if err != nil {
		return fmt.Errorf("failed to send event log notification: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

type TupleDecoder struct {
	relations map[uint32]*pglogrepl.RelationMessage
	typeMap   *pgtype.Map
}

func NewTupleDecoder() *TupleDecoder {
	return &TupleDecoder{
		relations: make(map[uint32]*pglogrepl.RelationMessage),
		typeMap:   pgtype.NewMap(),
	}
}

func (td *TupleDecoder) RegisterRelation(rel *pglogrepl.RelationMessage) {
	td.relations[rel.RelationID] = rel
}

func (td *TupleDecoder) GetRelation(id uint32) (*pglogrepl.RelationMessage, bool) {
	r, ok := td.relations[id]

	return r, ok
}

func (td *TupleDecoder) DecodeValues(
	relation uint32, tuple *pglogrepl.TupleData,
) (*pglogrepl.RelationMessage, map[string]interface{}, error) {
	rel, ok := td.relations[relation]
	if !ok {
		return nil, nil, fmt.Errorf(
			"unknown relation ID %d", relation)
	}

	values := make(map[string]interface{})

	for idx, col := range tuple.Columns {
		colName := rel.Columns[idx].Name

		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
		case 't': // text
			val, err := decodeTextColumnData(
				td.typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, nil, fmt.Errorf(
					"error decoding %s column data: %w",
					colName, err)
			}

			values[colName] = val
		}
	}

	return rel, values, nil
}

func decodeTextColumnData(
	mi *pgtype.Map, data []byte, dataType uint32,
) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		v, err := dt.Codec.DecodeValue(
			mi, dataType, pgtype.TextFormatCode, data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode value: %w", err)
		}

		return v, nil
	}

	return string(data), nil
}

type slotInfo struct {
	Plugin string
	Active bool
	Exists bool
}

func (pr *PGReplication) getSlotInfo(
	ctx context.Context, conn *pgxpool.Conn, name string,
) (*slotInfo, error) {
	row := conn.QueryRow(ctx, `
SELECT plugin, active
FROM (SELECT * FROM pg_replication_slots) AS slots
WHERE slot_name=$1`, name)

	info := slotInfo{
		Exists: true,
	}

	err := row.Scan(&info.Plugin, &info.Active)
	if errors.Is(err, pgx.ErrNoRows) {
		return &slotInfo{Exists: false}, nil
	} else if err != nil {
		return nil, fmt.Errorf(
			"failed to check for current replication slot: %w", err)
	}

	return &info, nil
}
