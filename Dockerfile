FROM --platform=$BUILDPLATFORM golang:1.22.4-alpine3.20 AS build

WORKDIR /usr/src

ADD go.mod go.sum ./
RUN go mod download && go mod verify

ADD . ./

ARG TARGETOS TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -o /build/repository ./cmd/repository

FROM alpine:3.20

COPY --from=build /build/repository /usr/local/bin/repository

RUN apk upgrade --no-cache \
    && apk add tzdata

# API server
EXPOSE 1080

# Debug/profiling server
EXPOSE 1081

ENTRYPOINT ["repository", "run"]
