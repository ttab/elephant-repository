FROM golang:1.20.2-alpine3.17 AS build

WORKDIR /usr/src

ADD go.mod go.sum ./
RUN go mod download && go mod verify

ADD . ./

RUN go build -o /usr/local/bin/repository ./cmd/repository

FROM alpine:3.17.2

COPY --from=build /usr/local/bin/repository /usr/local/bin/

ENTRYPOINT ["repository"]