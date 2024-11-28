// Package kprom provides prometheus plug-in metrics for a kgo client.
//
// This package tracks the following metrics under the following names,
// all metrics being counter vecs:

//	#ns_kafka_connects_total{node_id="#{node}"}
//	#ns_kafka_connect_errors_total{node_id="#{node}"}
//	#ns_kafka_disconnects_total{node_id="#{node}"}
//	#ns_kafka_write_errors_total{node_id="#{node}"}
//	#ns_kafka_write_bytes_total{node_id="#{node}"}
//	#ns_kafka_write_wait_latencies{node_id="#{node}"}
//	#ns_kafka_write_latencies{node_id="#{node}"}
//	#ns_kafka_read_errors_total{node_id="#{node}"}
//	#ns_kafka_read_bytes_total{node_id="#{node}"}
//	#ns_kafka_read_wait_latencies{node_id="#{node}"}
//	#ns_kafka_read_latencies{node_id="#{node}"}
//	#ns_kafka_throttle_latencies{node_id="#{node}"}
//	#ns_kafka_produce_bytes_uncompressed_total{node_id="#{node}",topic="#{topic}"}
//	#ns_kafka_produce_bytes_compressed_total{node_id="#{node}",topic="#{topic}"}
//	#ns_kafka_produce_records_total{node_id="#{node}",topic="#{topic}"}
//	#ns_kafka_fetch_bytes_uncompressed_total{node_id="#{node}",topic="#{topic}",group="#{group}"}
//	#ns_kafka_fetch_bytes_compressed_total{node_id="#{node}",topic="#{topic}",group="#{group}"}
//	#ns_kafka_fetch_records_total{node_id="#{node}",topic="#{topic}",group="#{group}"}
//	#ns_kafka_consume_handle_timing{topic="#{topic}",group="#{group}"}
//	#ns_kafka_consume_errors_total{topic="#{topic}",group="#{group}"}
//
// Const labels:
//   - client_id=#{client_id}
//   - client_kind=#{consumer/producer}
//
// This can be used in a client like so:
//
//	m := kprom.NewMetrics("namespace", "subsystem", <labels>)
//	cl, err := kgo.NewClient(
//	        kgo.WithHooks(m),
//	        // ...other opts
//	)
package kprom

import (
	"net"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	nodeLabel   = "node_id"
	topicLabel  = "topic"
	groupLabel  = "consumer_group"
	clientLabel = "client_id"
	kindLabel   = "client_kind"
)

type Metrics struct {
	labels prometheus.Labels

	connects    *prometheus.CounterVec
	connectErrs *prometheus.CounterVec
	disconnects *prometheus.CounterVec

	writeErrs    *prometheus.CounterVec
	writeBytes   *prometheus.CounterVec
	writeWaits   *prometheus.HistogramVec
	writeTimings *prometheus.HistogramVec

	readErrs    *prometheus.CounterVec
	readBytes   *prometheus.CounterVec
	readWaits   *prometheus.HistogramVec
	readTimings *prometheus.HistogramVec

	throttles *prometheus.HistogramVec

	produceBatchesUncompressed *prometheus.CounterVec
	produceBatchesCompressed   *prometheus.CounterVec
	produceRecordsTotal        *prometheus.CounterVec

	fetchBatchesUncompressed *prometheus.CounterVec
	fetchBatchesCompressed   *prometheus.CounterVec
	fetchRecordsTotal        *prometheus.CounterVec
	consumerProcessTiming    *prometheus.SummaryVec
	consumerHandleErrors     *prometheus.CounterVec
}

// To see all hooks that are available, ctrl+f "Hook" in the package
// documentation! The code below implements most hooks available at the time of
// this writing for demonstration.

var ( // interface checks to ensure we implement the hooks properly
	_ kgo.HookBrokerConnect       = new(Metrics)
	_ kgo.HookBrokerDisconnect    = new(Metrics)
	_ kgo.HookBrokerWrite         = new(Metrics)
	_ kgo.HookBrokerRead          = new(Metrics)
	_ kgo.HookBrokerThrottle      = new(Metrics)
	_ kgo.HookProduceBatchWritten = new(Metrics)
	_ kgo.HookFetchBatchRead      = new(Metrics)
)

func (m *Metrics) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		m.connectErrs.WithLabelValues(node).Inc()
		return
	}
	m.connects.WithLabelValues(node).Inc()
}

func (m *Metrics) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	node := strconv.Itoa(int(meta.NodeID))
	m.disconnects.WithLabelValues(node).Inc()
}

func (m *Metrics) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, writeWait, timeToWrite time.Duration, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		m.writeErrs.WithLabelValues(node).Inc()
		return
	}
	m.writeBytes.WithLabelValues(node).Add(float64(bytesWritten))
	m.writeWaits.WithLabelValues(node).Observe(writeWait.Seconds())
	m.writeTimings.WithLabelValues(node).Observe(timeToWrite.Seconds())
}

func (m *Metrics) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, readWait, timeToRead time.Duration, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		m.readErrs.WithLabelValues(node).Inc()
		return
	}
	m.readBytes.WithLabelValues(node).Add(float64(bytesRead))
	m.readWaits.WithLabelValues(node).Observe(readWait.Seconds())
	m.readTimings.WithLabelValues(node).Observe(timeToRead.Seconds())
}

func (m *Metrics) OnBrokerThrottle(meta kgo.BrokerMetadata, throttleInterval time.Duration, _ bool) {
	node := strconv.Itoa(int(meta.NodeID))
	m.throttles.WithLabelValues(node).Observe(throttleInterval.Seconds())
}

func (m *Metrics) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, _ int32, metrics kgo.ProduceBatchMetrics) {
	node := strconv.Itoa(int(meta.NodeID))
	m.produceBatchesUncompressed.WithLabelValues(node, topic).Add(float64(metrics.UncompressedBytes))
	m.produceBatchesCompressed.WithLabelValues(node, topic).Add(float64(metrics.CompressedBytes))
	m.produceRecordsTotal.WithLabelValues(node, topic).Add(float64(metrics.NumRecords))
}

func (m *Metrics) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, _ int32, metrics kgo.FetchBatchMetrics) {
	node := strconv.Itoa(int(meta.NodeID))
	m.fetchBatchesUncompressed.WithLabelValues(node, topic, m.labels[groupLabel]).Add(float64(metrics.UncompressedBytes))
	m.fetchBatchesCompressed.WithLabelValues(node, topic, m.labels[groupLabel]).Add(float64(metrics.CompressedBytes))
	m.fetchRecordsTotal.WithLabelValues(node, topic, m.labels[groupLabel]).Add(float64(metrics.NumRecords))
}

// CollectHandleProcessTiming collects handle process timing. Manual use.
func (m *Metrics) CollectHandleProcessTiming(startTime time.Time, topic string) {
	m.consumerProcessTiming.WithLabelValues(topic, m.labels[groupLabel]).Observe(time.Since(startTime).Seconds())
}

// CollectHandleErrors collects consumer handle errors. Manual use.
func (m *Metrics) CollectHandleErrors(topic string) {
	m.consumerHandleErrors.WithLabelValues(topic, m.labels[groupLabel]).Inc()
}

func NewMetrics(namespace, subsystem string, labels prometheus.Labels) (m *Metrics) {
	constLabels := prometheus.Labels{
		clientLabel: labels[clientLabel],
		kindLabel:   labels[kindLabel],
	}

	return &Metrics{
		labels: labels,

		// connects and disconnects

		connects: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_connects_total",
			Help:        "Total number of connections opened, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		connectErrs: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_connect_errors_total",
			Help:        "Total number of connection errors, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		disconnects: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_disconnects_total",
			Help:        "Total number of connections closed, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		// write

		writeErrs: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_write_errors_total",
			Help:        "Total number of write errors, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		writeBytes: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_write_bytes_total",
			Help:        "Total number of bytes written, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		writeWaits: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_write_wait_latencies",
			Help:        "Latency of time spent waiting to write to Kafka, in seconds by broker",
			Buckets:     prometheus.ExponentialBuckets(0.0001, 1.5, 20),
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		writeTimings: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_write_latencies",
			Help:        "Latency of time spent writing to Kafka, in seconds by broker",
			Buckets:     prometheus.ExponentialBuckets(0.0001, 1.5, 20),
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		// read

		readErrs: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_read_errors_total",
			Help:        "Total number of read errors, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		readBytes: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_read_bytes_total",
			Help:        "Total number of bytes read, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		readWaits: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_read_wait_latencies",
			Help:        "Latency of time spent waiting to read from Kafka, in seconds by broker",
			Buckets:     prometheus.ExponentialBuckets(0.0001, 1.5, 20),
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		readTimings: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_read_latencies",
			Help:        "Latency of time spent reading from Kafka, in seconds by broker",
			Buckets:     prometheus.ExponentialBuckets(0.0001, 1.5, 20),
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		// throttles

		throttles: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_throttle_latencies",
			Help:        "Latency of Kafka request throttles, in seconds by broker",
			Buckets:     prometheus.ExponentialBuckets(0.0001, 1.5, 20),
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		// produces & consumes

		produceBatchesUncompressed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_produce_bytes_uncompressed_total",
			Help:        "Total number of uncompressed bytes produced, by broker and topic",
			ConstLabels: constLabels,
		}, []string{nodeLabel, topicLabel}),

		produceBatchesCompressed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_produce_bytes_compressed_total",
			Help:        "Total number of compressed bytes actually produced, by topic and partition",
			ConstLabels: constLabels,
		}, []string{nodeLabel, topicLabel}),

		produceRecordsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_produce_records_total",
			Help:        "Total number of records produced",
			ConstLabels: constLabels,
		}, []string{nodeLabel, topicLabel}),

		fetchBatchesUncompressed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_fetch_bytes_uncompressed_total",
			Help:        "Total number of uncompressed bytes fetched, by topic and partition",
			ConstLabels: constLabels,
		}, []string{nodeLabel, topicLabel, groupLabel}),

		fetchBatchesCompressed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_fetch_bytes_compressed_total",
			Help:        "Total number of compressed bytes actually fetched, by topic and partition",
			ConstLabels: constLabels,
		}, []string{nodeLabel, topicLabel, groupLabel}),

		fetchRecordsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "kafka_fetch_records_total",
			Help:        "Total number of records fetched",
			ConstLabels: constLabels,
		}, []string{nodeLabel, topicLabel, groupLabel}),

		consumerProcessTiming: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "consume_handle_timing",
			Help:        "Handling duration of consumer split by quantiles (max age = 1m)",
			MaxAge:      time.Minute,
			Objectives:  map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
			ConstLabels: constLabels,
		}, []string{topicLabel, groupLabel}),

		consumerHandleErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "consume_errors_total",
			Help:        "Number of messages failed to consume",
			ConstLabels: constLabels,
		}, []string{topicLabel, groupLabel}),
	}
}
