// Package kprom provides prometheus plug-in metrics for a kgo client.
//
// This package tracks the following metrics under the following names,
// all metrics being counter vecs:

//	#ds_kafka_connects_total{node_id="#{node}"}
//	#ds_kafka_connect_errors_total{node_id="#{node}"}
//	#ds_kafka_disconnects_total{node_id="#{node}"}
//	#ds_kafka_write_errors_total{node_id="#{node}"}
//	#ds_kafka_write_bytes_total{node_id="#{node}"}
//	#ds_kafka_write_wait_latencies{node_id="#{node}"}
//	#ds_kafka_write_latencies{node_id="#{node}"}
//	#ds_kafka_read_errors_total{node_id="#{node}"}
//	#ds_kafka_read_bytes_total{node_id="#{node}"}
//	#ds_kafka_read_wait_latencies{node_id="#{node}"}
//	#ds_kafka_read_latencies{node_id="#{node}"}
//	#ds_kafka_throttle_latencies{node_id="#{node}"}
//	#ds_kafka_produce_bytes_uncompressed_total{node_id="#{node}",topic="#{topic}"}
//	#ds_kafka_produce_bytes_compressed_total{node_id="#{node}",topic="#{topic}"}
//	#ds_kafka_produce_records_total{node_id="#{node}",topic="#{topic}"}
//	#ds_kafka_fetch_bytes_uncompressed_total{node_id="#{node}",topic="#{topic}",group="#{group}"}
//	#ds_kafka_fetch_bytes_compressed_total{node_id="#{node}",topic="#{topic}",group="#{group}"}
//	#ds_kafka_fetch_records_total{node_id="#{node}",topic="#{topic}",group="#{group}"}
//
// The above metrics can be expanded considerably with options in this package,
// allowing timings, uncompressed and compressed bytes, and different labels.
//
// This can be used in a client like so:
//
//	m := kprom.NewMetrics("my-client-id", "client-kind", "consumer-group")
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
	ProducerKind = "producer"
	ConsumerKind = "consumer"
)

type Metrics struct {
	labels map[string]string
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
		connectErrs.WithLabelValues(node).Inc()
		return
	}
	connects.WithLabelValues(node).Inc()
}

func (m *Metrics) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	node := strconv.Itoa(int(meta.NodeID))
	disconnects.WithLabelValues(node).Inc()
}

func (m *Metrics) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, writeWait, timeToWrite time.Duration, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		writeErrs.WithLabelValues(node).Inc()
		return
	}
	writeBytes.WithLabelValues(node).Add(float64(bytesWritten))
	writeWaits.WithLabelValues(node).Observe(writeWait.Seconds())
	writeTimings.WithLabelValues(node).Observe(timeToWrite.Seconds())
}

func (m *Metrics) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, readWait, timeToRead time.Duration, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		readErrs.WithLabelValues(node).Inc()
		return
	}
	readBytes.WithLabelValues(node).Add(float64(bytesRead))
	readWaits.WithLabelValues(node).Observe(readWait.Seconds())
	readTimings.WithLabelValues(node).Observe(timeToRead.Seconds())
}

func (m *Metrics) OnBrokerThrottle(meta kgo.BrokerMetadata, throttleInterval time.Duration, _ bool) {
	node := strconv.Itoa(int(meta.NodeID))
	throttles.WithLabelValues(node).Observe(throttleInterval.Seconds())
}

func (m *Metrics) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, _ int32, metrics kgo.ProduceBatchMetrics) {
	node := strconv.Itoa(int(meta.NodeID))
	produceBatchesUncompressed.WithLabelValues(node, topic).Add(float64(metrics.UncompressedBytes))
	produceBatchesCompressed.WithLabelValues(node, topic).Add(float64(metrics.CompressedBytes))
	produceRecordsTotal.WithLabelValues(node, topic).Add(float64(metrics.NumRecords))
}

func (m *Metrics) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, _ int32, metrics kgo.FetchBatchMetrics) {
	node := strconv.Itoa(int(meta.NodeID))
	fetchBatchesUncompressed.WithLabelValues(node, topic, m.labels[groupLabel]).Add(float64(metrics.UncompressedBytes))
	fetchBatchesCompressed.WithLabelValues(node, topic, m.labels[groupLabel]).Add(float64(metrics.CompressedBytes))
	fetchRecordsTotal.WithLabelValues(node, topic, m.labels[groupLabel]).Add(float64(metrics.NumRecords))
}

func NewMetrics(clientID, clientKind, groupID string) (m *Metrics) {
	return &Metrics{
		labels: map[string]string{
			clientLabel: clientID,
			kindLabel:   clientKind,
			groupLabel:  groupID,
		},
	}
}

const (
	nodeLabel   = "node_id"
	topicLabel  = "topic"
	groupLabel  = "consumer_group"
	kindLabel   = "client_kind"
	clientLabel = "client_id"
)

var (
	connects = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_connects_total",
		Help:      "Total number of connections opened, by broker",
	}, []string{nodeLabel})

	connectErrs = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_connect_errors_total",
		Help:      "Total number of connection errors, by broker",
	}, []string{nodeLabel})

	disconnects = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_disconnects_total",
		Help:      "Total number of connections closed, by broker",
	}, []string{nodeLabel})

	// write

	writeErrs = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_write_errors_total",
		Help:      "Total number of write errors, by broker",
	}, []string{nodeLabel})

	writeBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_write_bytes_total",
		Help:      "Total number of bytes written, by broker",
	}, []string{nodeLabel})

	writeWaits = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ds",
		Name:      "kafka_write_wait_latencies",
		Help:      "Latency of time spent waiting to write to Kafka, in seconds by broker",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 1.5, 20),
	}, []string{nodeLabel})

	writeTimings = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ds",
		Name:      "kafka_write_latencies",
		Help:      "Latency of time spent writing to Kafka, in seconds by broker",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 1.5, 20),
	}, []string{nodeLabel})

	// read

	readErrs = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_read_errors_total",
		Help:      "Total number of read errors, by broker",
	}, []string{nodeLabel})

	readBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_read_bytes_total",
		Help:      "Total number of bytes read, by broker",
	}, []string{nodeLabel})

	readWaits = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ds",
		Name:      "kafka_read_wait_latencies",
		Help:      "Latency of time spent waiting to read from Kafka, in seconds by broker",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 1.5, 20),
	}, []string{nodeLabel})

	readTimings = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ds",
		Name:      "kafka_read_latencies",
		Help:      "Latency of time spent reading from Kafka, in seconds by broker",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 1.5, 20),
	}, []string{nodeLabel})

	// throttles

	throttles = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ds",
		Name:      "kafka_throttle_latencies",
		Help:      "Latency of Kafka request throttles, in seconds by broker",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 1.5, 20),
	}, []string{nodeLabel})

	// produces & consumes

	produceBatchesUncompressed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_produce_bytes_uncompressed_total",
		Help:      "Total number of uncompressed bytes produced, by broker and topic",
	}, []string{nodeLabel, topicLabel})

	produceBatchesCompressed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_produce_bytes_compressed_total",
		Help:      "Total number of compressed bytes actually produced, by topic and partition",
	}, []string{nodeLabel, topicLabel})

	produceRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_produce_records_total",
		Help:      "Total number of records produced",
	}, []string{nodeLabel, topicLabel})

	fetchBatchesUncompressed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_fetch_bytes_uncompressed_total",
		Help:      "Total number of uncompressed bytes fetched, by topic and partition",
	}, []string{nodeLabel, topicLabel, groupLabel})

	fetchBatchesCompressed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_fetch_bytes_compressed_total",
		Help:      "Total number of compressed bytes actually fetched, by topic and partition",
	}, []string{nodeLabel, topicLabel, groupLabel})

	fetchRecordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_fetch_records_total",
		Help:      "Total number of records fetched",
	}, []string{nodeLabel, topicLabel, groupLabel})
)
