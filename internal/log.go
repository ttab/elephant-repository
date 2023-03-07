package internal

import (
	"io"
	"os"

	"golang.org/x/exp/slog"
)

// Log attribute keys used throughout the application.
const (
	LogKeyLogLevel     = "log_level"
	LogKeyCountMetric  = "count_metric"
	LogKeyDocumentUUID = "document_uuid"
	LogKeyTransaction  = "transaction"
	LogKeyOCSource     = "oc_source"
	LogKeyOCVersion    = "oc_version"
	LogKeyOCEvent      = "oc_event"
	LogKeyChannel      = "channel"
	LogKeyMessage      = "message"
	LogKeyDelay        = "delay"
	LogKeyBucket       = "bucket"
	LogKeyObjectKey    = "object_key"
	LogKeyComponent    = "component"
	LogKeyCount        = "count"
)

// SetUpLogger creates a default JSON logger and sets it as the global logger.
func SetUpLogger(logLevel string, w io.Writer) *slog.Logger {
	logger := slog.New(slog.NewJSONHandler(w))

	level := slog.LevelWarn

	if logLevel != "" {
		err := level.UnmarshalText([]byte(logLevel))
		if err != nil {
			level = slog.LevelWarn

			logger.Error("invalid log level", err,
				LogKeyLogLevel, logLevel)
		}
	}

	logger = slog.New(slog.HandlerOptions{
		Level: &level,
	}.NewJSONHandler(os.Stdout))

	slog.SetDefault(logger)

	return logger
}
