package logging

import (
	"io"
	"log/slog"
	"strings"
)

type Logger struct {
	inner *slog.Logger
}

func NewJSONLogger(w io.Writer, level slog.Level) *Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
	})
	return &Logger{inner: slog.New(handler)}
}

func ParseLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func (l *Logger) Info(event string, args ...any) {
	l.inner.Info(event, append([]any{"event", event}, args...)...)
}

func (l *Logger) Error(event string, args ...any) {
	l.inner.Error(event, append([]any{"event", event}, args...)...)
}
