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

	processOperationName      = "process"
	offsetCommitOperationName = "commit"
	shareAckOperationName     = "ack"
)

func newClientAttributes(name string, labels map[string]string) attribute.Set {
	attributes := make([]attribute.KeyValue, 0, len(labels)+1)

	for key, value := range labels {
		if isReservedAttribute(key) {
			continue
		}

		attributes = append(attributes, attribute.String(key, value))
	}

	if name != "" {
		attributes = append(attributes, semconv.MessagingClientID(name))
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

func isReservedAttribute(key string) bool {
	switch key {
	case string(semconv.MessagingClientIDKey),
		string(semconv.MessagingConsumerGroupNameKey),
		string(semconv.MessagingDestinationNameKey),
		string(semconv.MessagingDestinationPartitionIDKey),
		string(semconv.MessagingOperationNameKey),
		string(semconv.MessagingOperationTypeKey),
		string(semconv.MessagingSystemKey),
		string(semconv.ErrorTypeKey),
		string(semconv.MessagingBatchMessageCountKey),
		string(semconv.MessagingKafkaOffsetKey),
		string(fetchErrorRecoverableKey),
		string(shareGroupKey),
		string(shareAckOutcomeKey),
		string(transactionOutcomeKey),
		string(transactionTypeKey):
		return true
	}

	return false
}
