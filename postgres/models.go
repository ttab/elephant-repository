// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.16.0

package postgres

import (
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

type Acl struct {
	Uuid        uuid.UUID
	Uri         string
	Created     pgtype.Timestamptz
	CreatorUri  string
	Permissions pgtype.Array[string]
}

type DeleteRecord struct {
	ID         int64
	Uuid       uuid.UUID
	Uri        string
	Version    int64
	Created    pgtype.Timestamptz
	CreatorUri string
	Meta       []byte
}

type Document struct {
	Uuid           uuid.UUID
	Uri            string
	Created        pgtype.Timestamptz
	CreatorUri     string
	Updated        pgtype.Timestamptz
	UpdaterUri     string
	CurrentVersion int64
	Deleting       bool
}

type DocumentLink struct {
	FromDocument uuid.UUID
	Version      int64
	ToDocument   uuid.UUID
	Rel          pgtype.Text
	Type         pgtype.Text
}

type DocumentStatus struct {
	Uuid       uuid.UUID
	Name       string
	ID         int64
	Version    int64
	Created    pgtype.Timestamptz
	CreatorUri string
	Meta       []byte
	Archived   bool
	Signature  pgtype.Text
}

type DocumentVersion struct {
	Uuid         uuid.UUID
	Uri          string
	Version      int64
	Title        pgtype.Text
	Type         string
	Language     pgtype.Text
	Created      pgtype.Timestamptz
	CreatorUri   string
	Meta         []byte
	DocumentData []byte
	Archived     bool
	Signature    pgtype.Text
}

type SchemaVersion struct {
	Version int32
}

type SigningKey struct {
	Kid  string
	Spec []byte
}

type StatusHead struct {
	Uuid       uuid.UUID
	Name       string
	ID         int64
	Updated    pgtype.Timestamptz
	UpdaterUri string
}