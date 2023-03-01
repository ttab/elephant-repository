package test

import (
	"context"

	"golang.org/x/exp/slog"
)

type Logger interface {
	Log(args ...any)
}

func NewLogHandler(t Logger, level slog.Level) slog.Handler {
	h := LogHandler{
		t: t,
	}

	h.handler = slog.HandlerOptions{
		Level: level,
	}.NewTextHandler(&h)

	return &h
}

type LogHandler struct {
	t       Logger
	handler *slog.TextHandler
}

func (h *LogHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.handler.Handle(ctx, r) //nolint:wrapcheck
}

func (h *LogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h.handler.WithAttrs(attrs)
}

func (h *LogHandler) WithGroup(name string) slog.Handler {
	return h.handler.WithGroup(name)
}

func (h *LogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h *LogHandler) Write(data []byte) (int, error) {
	h.t.Log(string(data))

	return len(data), nil
}
