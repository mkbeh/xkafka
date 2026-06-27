// Package kprom provides Prometheus metrics.
//
// Low-level Kafka client metrics are provided by the official franz-go kprom plugin.
// This includes broker connection, read/write, request, produce, and fetch metrics.
//
// In addition to franz-go client metrics, this package tracks wrapper-level metrics:
//
// Producer metrics:
//
//	#ns_kafka_produce_errors_total{topic="#{topic}"}
//
// Transaction metrics:
//
//	#ns_kafka_transactions_total{outcome="#{commit|error}"}
//	#ns_kafka_transaction_duration_seconds
//
// Consumer metrics:
//
//	#ns_kafka_consume_handle_duration_seconds{topic="#{topic}",consumer_group="#{consumer_group}"}
//	#ns_kafka_consume_errors_total{topic="#{topic}",consumer_group="#{consumer_group}"}
//
// The consume_handle_duration_seconds and transaction_duration_seconds metrics are
// Prometheus histograms, so they also export _bucket, _sum, and _count series.
//
// Fetch metrics from the official franz-go kprom plugin are collected at the Kafka
// client fetch layer. They can differ from handler-level processing metrics and may
// be absent for share group consumers. Use consume_handle_duration_seconds_count to
// estimate the number of records observed by xkafka handlers.
//
// Const labels:
//   - client_id=#{client_id}
//   - client_kind=#{consumer|producer}

package kprom

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	franzkprom "github.com/twmb/franz-go/plugin/kprom"
)

type Metrics struct {
	hooks    *franzkprom.Metrics
	producer *ProducerMetrics
	consumer *ConsumerMetrics
}

func NewMetrics(namespace, subsystem string, labels prometheus.Labels) *Metrics {
	labels = cloneLabels(labels)
	constLabels := baseConstLabels(labels)

	return &Metrics{
		hooks:    newHooks(namespace, subsystem, constLabels),
		producer: newProducerMetrics(namespace, subsystem, constLabels),
		consumer: newConsumerMetrics(namespace, subsystem, labels, constLabels),
	}
}

func (m *Metrics) Hooks() kgo.Hook {
	return m.hooks
}

func (m *Metrics) Producer() *ProducerMetrics {
	return m.producer
}

func (m *Metrics) Consumer() *ConsumerMetrics {
	return m.consumer
}

func newHooks(namespace, subsystem string, constLabels prometheus.Labels) *franzkprom.Metrics {
	return franzkprom.NewMetrics(
		namespace,
		franzkprom.Subsystem(subsystem),

		// Register official franz-go metrics into the default Prometheus registry
		// used by the application /metrics endpoint.
		franzkprom.Registerer(prometheus.DefaultRegisterer),
		franzkprom.Gatherer(prometheus.DefaultGatherer),

		// Keep wrapper-level const labels on official franz-go metrics too.
		franzkprom.WithStaticLabel(constLabels),

		franzkprom.BrokerLabels(
			franzkprom.BrokerNodeID,
		),

		franzkprom.FetchAndProduceDetail(
			franzkprom.ByNode,
			franzkprom.ByTopic,
			franzkprom.Batches,
			franzkprom.Records,
			franzkprom.CompressedBytes,
			franzkprom.UncompressedBytes,
			franzkprom.ConsistentNaming,
		),

		franzkprom.Histograms(
			franzkprom.ReadWait,
			franzkprom.ReadTime,
			franzkprom.WriteWait,
			franzkprom.WriteTime,
			franzkprom.RequestDurationE2E,
			franzkprom.RequestThrottled,
		),

		franzkprom.Buckets(defaultDurationBuckets()),
	)
}
