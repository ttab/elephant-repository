package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
	"github.com/ttab/elephant/postgres"
)

type EventType string

const (
	TypeDocumentVersion EventType = "document"
	TypeNewStatus       EventType = "status"
	TypeACLUpdate       EventType = "acl"
	TypeDeleteDocument  EventType = "delete_document"
	TypeDeleteStatus    EventType = "delete_status"
)

type Event struct {
	Type      EventType         `json:"type"`
	UUID      string            `json:"uuid"`
	Timestamp time.Time         `json:"timestamp"`
	Updater   string            `json:"updater"`
	Version   int               `json:"version"`
	Status    string            `json:"status"`
	Meta      map[string]string `json:"meta"`
}

type PGReplication struct {
	pool   *pgxpool.Pool
	dbURI  string
	logger *logrus.Logger
}

func NewPGReplication(
	logger *logrus.Logger,
	pool *pgxpool.Pool,
	dbURI string,
) *PGReplication {
	return &PGReplication{
		pool:   pool,
		logger: logger,
		dbURI:  dbURI,
	}
}

func (pr *PGReplication) Run(ctx context.Context) {
	const restartWaitSeconds = 10

	for {
		err := pr.startReplication(ctx)
		if err != nil {
			pr.logger.WithContext(ctx).WithError(err).Errorf(
				"replication error, restarting in %d seconds",
				restartWaitSeconds,
			)
		}

		select {
		case <-time.After(restartWaitSeconds * time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func (pr *PGReplication) startReplication(
	ctx context.Context,
) error {
	lockTx, err := pr.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin locking transaction: %w", err)
	}

	defer lockTx.Rollback(context.Background())

	lockQueries := postgres.New(lockTx)

	err = lockQueries.AcquireTXLock(ctx, LockLogicalReplication)
	if err != nil {
		return fmt.Errorf("failed to acquire replication lock: %w", err)
	}

	conn, err := pgconn.Connect(ctx, pr.dbURI+"?replication=database")
	if err != nil {
		return fmt.Errorf(
			"failed to create replication connection: %w", err)
	}

	defer conn.Close(context.Background())

	slotInfo, err := pr.getSlotInfo(ctx, lockTx, "eventlogslot")
	if err != nil {
		return fmt.Errorf("failed to get slot information: %w", err)
	}

	if !slotInfo.Exists {
		_, err = pglogrepl.CreateReplicationSlot(ctx,
			conn, "eventlogslot", "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{
				Mode: pglogrepl.LogicalReplication,
			})
		if err != nil {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}
	}

	err = pglogrepl.StartReplication(
		ctx, conn, "eventlogslot", 0,
		pglogrepl.StartReplicationOptions{PluginArgs: []string{
			"proto_version '1'", "publication_names 'eventlog'",
		}})
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	err = pr.replicationLoop(ctx, conn)
	if err != nil {
		return err
	}

	return nil
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
				continue
			}

			return fmt.Errorf("failed to recieve message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("recieved wal error (%s %s): %s",
				errMsg.Code,
				errMsg.Severity,
				errMsg.Message)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message: %T\n", rawMsg)
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

func (pr *PGReplication) handleReplicationMessage(
	dec *TupleDecoder,
	msg pglogrepl.Message,
) error {
	switch logicalMsg := msg.(type) {
	case *pglogrepl.RelationMessage:
		dec.RegisterRelation(logicalMsg)
	case *pglogrepl.BeginMessage:
	case *pglogrepl.CommitMessage:
	case *pglogrepl.InsertMessage:
		rel, values, err := dec.DecodeValues(
			logicalMsg.RelationID, logicalMsg.Tuple)
		if err != nil {
			return fmt.Errorf(
				"failed to decode values: %w", err)
		}

		log.Printf("INSERT INTO %s.%s", rel.Namespace, rel.RelationName)
		spew.Dump(values)

	case *pglogrepl.UpdateMessage:
		rel, values, err := dec.DecodeValues(
			logicalMsg.RelationID, logicalMsg.NewTuple)
		if err != nil {
			return fmt.Errorf(
				"failed to decode values: %w", err)
		}

		log.Printf("UPDATE %s.%s", rel.Namespace, rel.RelationName)
		spew.Dump(values)
	case *pglogrepl.DeleteMessage:
		// ...
	case *pglogrepl.TruncateMessage:
	case *pglogrepl.TypeMessage:
	case *pglogrepl.OriginMessage:
	default:
		log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
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
		case 't': //text
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

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

type slotInfo struct {
	Plugin string
	Active bool
	Exists bool
}

func (pr *PGReplication) getSlotInfo(
	ctx context.Context, conn pgx.Tx, name string,
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
