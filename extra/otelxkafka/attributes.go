package otelxkafka

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kerr"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

const (
	fetchErrorRecoverableKey attribute.Key = "xkafka.fetch.error.recoverable"
	shareGroupKey            attribute.Key = "xkafka.share.group.name"
	shareAckOutcomeKey       attribute.Key = "xkafka.share.ack.outcome"
	transactionOutcomeKey    attribute.Key = "xkafka.transaction.outcome"
	transactionTypeKey       attribute.Key = "xkafka.transaction.type"

	sendOperationName         = "send"
	processOperationName      = "process"
	offsetCommitOperationName = "commit"
	shareAckOperationName     = "ack"
)

func newClientAttributes(name string, labels map[string]string) attribute.Set {
	var attributes []attribute.KeyValue

	if name != "" {
		attributes = append(attributes, semconv.MessagingClientID(name))
	}

	for key, value := range labels {
		attributes = append(attributes, attribute.String(key, value))
	}

	return attribute.NewSet(attributes...)
}

func errorType(err error) attribute.KeyValue {
	var kafkaErr *kerr.Error
	if errors.As(err, &kafkaErr) && kafkaErr != nil && kafkaErr.Message != "" {
		return semconv.ErrorTypeKey.String(kafkaErr.Message)
	}

	return semconv.ErrorType(err)
}
