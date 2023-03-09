FROM golang:1.20.2-alpine3.17 AS build

WORKDIR /usr/src

ADD go.mod go.sum ./
RUN go mod download && go mod verify

RUN go build github.com/twitchtv/twirp/protoc-gen-twirp
RUN go build google.golang.org/protobuf/cmd/protoc-gen-go
RUN go build github.com/navigacontentlab/twopdocs/cmd/protoc-gen-openapi3

FROM alpine:3.17.1

ARG protoc_version

RUN apk add --no-cache protoc=$protoc_version

COPY --from=build /usr/src/protoc-gen-twirp /usr/local/bin/
COPY --from=build /usr/src/protoc-gen-go /usr/local/bin/
COPY --from=build /usr/src/protoc-gen-openapi3 /usr/local/bin/

WORKDIR /usr/src
