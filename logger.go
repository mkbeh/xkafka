package kafka

import (
	"log/slog"

	kslog "github.com/mkbeh/kafka/pkg/logger"
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
