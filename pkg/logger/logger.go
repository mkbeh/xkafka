package logger

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
