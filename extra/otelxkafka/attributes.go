package otelxkafka

import (
	"errors"
	"slices"

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

// newAttributeSets builds client and consumer attributes.
// Consumer attributes extend client attributes with the configured group.
func newAttributeSets(
	clientID string,
	consumerGroup string,
	shareGroup string,
	labels map[string]string,
) (clientAttrs, consumerAttrs []attribute.KeyValue) {
	var attrs []attribute.KeyValue

	if clientID != "" {
		attrs = append(attrs, semconv.MessagingClientID(clientID))
	}
	for key, value := range labels {
		attrs = append(attrs, attribute.String(key, value))
	}

	clientAttrs = normalizeAttributes(attrs...)

	var groupAttr attribute.KeyValue
	switch {
	case consumerGroup != "":
		groupAttr = semconv.MessagingConsumerGroupName(consumerGroup)
	case shareGroup != "":
		groupAttr = shareGroupKey.String(shareGroup)
	default:
		return clientAttrs, clientAttrs
	}

	// Prevent consumerAttrs from sharing writable capacity with clientAttrs.
	consumerAttrs = append(
		clientAttrs[:len(clientAttrs):len(clientAttrs)],
		groupAttr,
	)

	return clientAttrs, normalizeAttributes(consumerAttrs...)
}

// normalizeAttributes returns normalized attributes with no spare slice capacity.
func normalizeAttributes(attrs ...attribute.KeyValue) []attribute.KeyValue {
	set := attribute.NewSet(attrs...)
	return slices.Clip(set.ToSlice())
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
