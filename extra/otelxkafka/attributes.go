package otelxkafka

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
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

func commonRecordDestination(records []*kgo.Record) (topic string, partition int32) {
	if len(records) == 0 || records[0] == nil {
		return "", -1
	}

	topic = records[0].Topic
	partition = records[0].Partition

	for _, record := range records[1:] {
		if record == nil || record.Topic != topic {
			return "", -1
		}

		if partition >= 0 && record.Partition != partition {
			partition = -1
		}
	}

	return topic, partition
}

func errorType(err error) attribute.KeyValue {
	var kafkaErr *kerr.Error
	if errors.As(err, &kafkaErr) && kafkaErr != nil && kafkaErr.Message != "" {
		return semconv.ErrorTypeKey.String(kafkaErr.Message)
	}

	return semconv.ErrorType(err)
}
