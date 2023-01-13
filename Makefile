service_name = repository
proto_file = rpc/$(service_name)/service.proto
generated_files = rpc/$(service_name)/service.pb.go rpc/$(service_name)/service.twirp.go

.PHONY: proto
proto: $(generated_files)

$(generated_files): $(proto_file) Dockerfile.generator Makefile
	docker build . -f Dockerfile.generator -t docformat-generator:latest \
		--build-arg protoc_version=3.21.9-r0
	docker run --rm -v "${PWD}:/usr/src" -u ${UID}:${GID} \
		docformat-generator:latest \
		protoc --go_out=. --twirp_out=. $(proto_file)

