//go:build tools
// +build tools

package tools

import (
	_ "github.com/jackc/tern/v2"
	_ "github.com/kyleconroy/sqlc/cmd/sqlc"
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"
)
