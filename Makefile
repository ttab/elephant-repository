pg_conn?=postgres://repository:pass@localhost/repository
rollback_to?=0

UID := $(shell id -u)
GID := $(shell id -g)

SQL_TOOLS := ghcr.io/ttab/elephant-sqltools:v0.1.0

SQLC := docker run --rm \
	-v "${PWD}:/usr/src" -u $(UID):$(GID) \
	$(SQL_TOOLS) sqlc
TERN := docker run --rm \
	-v "${PWD}:/usr/src" \
	--network host \
	$(SQL_TOOLS) tern

.PHONY: db-rollback
db-rollback:
	$(TERN) migrate --migrations schema \
		--conn-string $(pg_conn) --destination $(rollback_to)

.PHONY: db-migrate
db-migrate:
	$(TERN) migrate --migrations schema \
		--conn-string $(pg_conn)
	rm -f postgres/schema.sql
	make postgres/schema.sql

.PHONY: generate-sql
generate-sql: postgres/schema.sql postgres/query.sql.go

postgres/schema.sql postgres/schema_version.sql:
	./dump-postgres-schema.sh

postgres/query.sql.go: postgres/schema.sql postgres/query.sql
	$(SQLC) generate
