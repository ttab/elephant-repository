FROM registry.a.tt.se/docker/golang:1.20.2-alpine3.17 AS build

WORKDIR /usr/src

ADD go.mod go.sum ./
RUN go mod download && go mod verify

ADD . ./

RUN go build -o /usr/local/bin/repository ./cmd/repository

FROM registry.a.tt.se/docker/alpine:3.17.2

COPY --from=build /usr/local/bin/repository /usr/local/bin/

RUN apk upgrade --no-cache \
    && apk add tzdata

# API server
EXPOSE 1080

# Debug/profiling server
EXPOSE 1081

ENTRYPOINT ["repository"]
