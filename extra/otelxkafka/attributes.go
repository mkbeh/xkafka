package otelxkafka

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kerr"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

const (
	clientIDAttribute               = "messaging.client.id"
	consumerGroupAttribute          = "messaging.consumer.group.name"
	destinationNameAttribute        = "messaging.destination.name"
	destinationPartitionIDAttribute = "messaging.destination.partition.id"
	operationNameAttribute          = "messaging.operation.name"
	operationTypeAttribute          = "messaging.operation.type"
	messagingSystemAttribute        = "messaging.system"
	errorTypeAttribute              = "error.type"
	recordCountAttribute            = "messaging.batch.message_count"
	kafkaOffsetAttribute            = "messaging.kafka.offset"

	fetchErrorRecoverableAttribute = "xkafka.fetch.error.recoverable"
	shareGroupAttribute            = "xkafka.share.group.name"
	shareAckOutcomeAttribute       = "xkafka.share.ack.outcome"
	transactionOutcomeAttribute    = "xkafka.transaction.outcome"
	transactionTypeAttribute       = "xkafka.transaction.type"

	messagingSystemKafka = "kafka"

	handleOperationName       = "handle"
	offsetCommitOperationName = "commit"
	shareAckOperationName     = "ack"

	processOperationType = "process"
	settleOperationType  = "settle"
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
		return attribute.String(errorTypeAttribute, kafkaErr.Message)
	}

	return semconv.ErrorType(err)
}

func isReservedAttribute(key string) bool {
	switch key {
	case clientIDAttribute,
		consumerGroupAttribute,
		destinationNameAttribute,
		destinationPartitionIDAttribute,
		operationNameAttribute,
		operationTypeAttribute,
		messagingSystemAttribute,
		errorTypeAttribute,
		recordCountAttribute,
		kafkaOffsetAttribute,
		fetchErrorRecoverableAttribute,
		shareGroupAttribute,
		shareAckOutcomeAttribute,
		transactionOutcomeAttribute,
		transactionTypeAttribute:
		return true
	}

	return false
}
