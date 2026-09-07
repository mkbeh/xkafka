package otelxkafka

import (
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"go.opentelemetry.io/otel/attribute"
)

func TestNewClientAttributes(t *testing.T) {
	set := newClientAttributes("orders", map[string]string{
		"service":                       "orders-api",
		clientIDAttribute:               "override",
		consumerGroupAttribute:          "override",
		destinationNameAttribute:        "override",
		destinationPartitionIDAttribute: "override",
		operationNameAttribute:          "override",
		operationTypeAttribute:          "override",
		messagingSystemAttribute:        "override",
		errorTypeAttribute:              "override",
		recordCountAttribute:            "override",
		kafkaOffsetAttribute:            "override",
		fetchErrorRecoverableAttribute:  "override",
		shareGroupAttribute:             "override",
		shareAckOutcomeAttribute:        "override",
		transactionOutcomeAttribute:     "override",
		transactionTypeAttribute:        "override",
	})

	if value, ok := set.Value(attribute.Key(clientIDAttribute)); !ok || value.AsString() != "orders" {
		t.Fatalf("messaging.client.id = %q, %v; want orders, true", value.AsString(), ok)
	}
	if value, ok := set.Value(attribute.Key("service")); !ok || value.AsString() != "orders-api" {
		t.Fatalf("service = %q, %v; want orders-api, true", value.AsString(), ok)
	}

	for _, key := range []string{
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
		transactionTypeAttribute,
	} {
		if _, ok := set.Value(attribute.Key(key)); ok {
			t.Fatalf("reserved %s attribute was retained", key)
		}
	}
}

func TestErrorTypeKafkaError(t *testing.T) {
	got := errorType(fmt.Errorf("wrapped: %w", kerr.UnknownTopicOrPartition))

	if got.Key != attribute.Key(errorTypeAttribute) {
		t.Fatalf("error type key = %q, want %q", got.Key, errorTypeAttribute)
	}
	if got.Value.AsString() != kerr.UnknownTopicOrPartition.Message {
		t.Fatalf("error type = %q, want %q", got.Value.AsString(), kerr.UnknownTopicOrPartition.Message)
	}
}
