package kafka

import (
	"log/slog"

	"github.com/mkbeh/kafka/pkg/kslog"
)

const (
	producerComponentValue = "kafka_producer"
	consumerComponentValue = "kafka_consumer"
)

func wrapLogger(l *slog.Logger, component string) *slog.Logger {
	if l == nil {
		l = slog.Default()
	}
	return l.With(kslog.Component(component))
}
