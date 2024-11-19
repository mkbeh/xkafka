package kslog

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KgoAdapter struct {
	sl *slog.Logger
}

func NewKgoAdapter(sl *slog.Logger) *KgoAdapter {
	return &KgoAdapter{sl}
}

// Level is for the kgo.Logger interface.
func (l *KgoAdapter) Level() kgo.LogLevel {
	ctx := context.Background()
	switch {
	case l.sl.Enabled(ctx, slog.LevelDebug):
		return kgo.LogLevelDebug
	case l.sl.Enabled(ctx, slog.LevelInfo):
		return kgo.LogLevelInfo
	case l.sl.Enabled(ctx, slog.LevelWarn):
		return kgo.LogLevelWarn
	case l.sl.Enabled(ctx, slog.LevelError):
		return kgo.LogLevelError
	default:
		return kgo.LogLevelNone
	}
}

func (l *KgoAdapter) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	l.sl.Log(context.Background(), kgoToSlogLevel(level), msg, keyvals...)
}

func kgoToSlogLevel(level kgo.LogLevel) slog.Level {
	switch level {
	case kgo.LogLevelError:
		return slog.LevelError
	case kgo.LogLevelWarn:
		return slog.LevelWarn
	case kgo.LogLevelInfo:
		return slog.LevelInfo
	case kgo.LogLevelDebug:
		return slog.LevelDebug
	default:
		// Using the default level for slog
		return slog.LevelInfo
	}
}

// all key constants must be defined with the "Key" suffix.

// Constants and constructors to standardize the keys used in logs.

const (
	// errorKey - to pass error to log.
	errorKey string = "error"
	// componentKey - to identify the component that is logging (e.g.: "kafka-consumer")
	componentKey = "component"
)

// Error for passing error to log.
func Error(err error) slog.Attr { return slog.Any(errorKey, err) }

// Component to identify the component that is logging (e.g.: "kafka-consumer").
// here any distinct level of your application abstraction can be used.
// it's not mandatory associated with kind of an external connection,
// e.g.: "location-event-processor" is also applicable for the field.
func Component(component string) slog.Attr { return slog.String(componentKey, component) }

// Record transforms an []byte into a slog.Attr.
func Record(record []byte) slog.Attr { return slog.String("record", string(record)) }

// Records transforms a []byte into a slog.Attr.
func Records(records []byte) slog.Attr { return slog.String("records", string(records)) }

// Count transforms an int into a slog.Attr.
func Count(count int) slog.Attr { return slog.Int("count", count) }

// ConsumerLabels transforms a map[string]string into slog.Attr.
func ConsumerLabels(labels map[string]string) slog.Attr {
	attrs := make([]slog.Attr, 0)
	for k, v := range labels {
		attrs = append(attrs, slog.String(k, v))
	}
	return slog.Attr{
		Key:   "consumer_labels",
		Value: slog.GroupValue(attrs...),
	}
}
