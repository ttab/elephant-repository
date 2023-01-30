service_name = repository
proto_file = rpc/$(service_name)/service.proto
generated_files = rpc/$(service_name)/service.pb.go rpc/$(service_name)/service.twirp.go

pg_conn?=postgres://repository:pass@localhost/repository

.PHONY: proto
proto: $(generated_files)

$(generated_files): $(proto_file) Dockerfile.generator Makefile
	docker build . -f Dockerfile.generator -t docformat-generator:latest \
		--build-arg protoc_version=3.21.9-r0
	docker run --rm -v "${PWD}:/usr/src" -u ${UID}:${GID} \
		docformat-generator:latest \
		protoc --go_out=. --twirp_out=. $(proto_file)

# Looks like we'll have to use a snapshot version of sqlc until pgx/v5 support
# lands in v1.17.0. See https://github.com/kyleconroy/sqlc/issues/1823
bin/sqlc: go.mod
	GOBIN=${PWD}/bin go install github.com/kyleconroy/sqlc/cmd/sqlc

bin/tern: go.mod
	GOBIN=${PWD}/bin go install github.com/jackc/tern

.PHONY: db-teardown
db-teardown: bin/tern
	./bin/tern migrate --migrations schema \
		--conn-string $(pg_conn) --destination 0
	rm postgres/schema.sql

.PHONY: migrate
db-migrate: bin/tern
	./bin/tern migrate --migrations schema \
		--conn-string $(pg_conn)
	rm postgres/schema.sql
	make postgres/schema.sql

.PHONY: generate-sql
generate-sql: db-migrate postgres/query.sql.go

postgres/schema.sql:
	cd schema && tern migrate \
		--conn-string $(pg_conn)
	./dump-postgres-schema.sh

postgres/query.sql.go: bin/sqlc postgres/schema.sql postgres/query.sql
	./bin/sqlc --experimental generate
