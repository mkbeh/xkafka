package kprom

import (
	"net"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Hooks struct {
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
}

var (
	_ kgo.HookBrokerConnect       = new(Hooks)
	_ kgo.HookBrokerDisconnect    = new(Hooks)
	_ kgo.HookBrokerWrite         = new(Hooks)
	_ kgo.HookBrokerRead          = new(Hooks)
	_ kgo.HookBrokerThrottle      = new(Hooks)
	_ kgo.HookProduceBatchWritten = new(Hooks)
	_ kgo.HookFetchBatchRead      = new(Hooks)
)

func (h *Hooks) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		h.connectErrs.WithLabelValues(node).Inc()
		return
	}

	h.connects.WithLabelValues(node).Inc()
}

func (h *Hooks) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	node := strconv.Itoa(int(meta.NodeID))
	h.disconnects.WithLabelValues(node).Inc()
}

func (h *Hooks) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, writeWait, timeToWrite time.Duration, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		h.writeErrs.WithLabelValues(node).Inc()
		return
	}

	h.writeBytes.WithLabelValues(node).Add(float64(bytesWritten))
	h.writeWaits.WithLabelValues(node).Observe(writeWait.Seconds())
	h.writeTimings.WithLabelValues(node).Observe(timeToWrite.Seconds())
}

func (h *Hooks) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, readWait, timeToRead time.Duration, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		h.readErrs.WithLabelValues(node).Inc()
		return
	}

	h.readBytes.WithLabelValues(node).Add(float64(bytesRead))
	h.readWaits.WithLabelValues(node).Observe(readWait.Seconds())
	h.readTimings.WithLabelValues(node).Observe(timeToRead.Seconds())
}

func (h *Hooks) OnBrokerThrottle(meta kgo.BrokerMetadata, throttleInterval time.Duration, _ bool) {
	node := strconv.Itoa(int(meta.NodeID))
	h.throttles.WithLabelValues(node).Observe(throttleInterval.Seconds())
}

func (h *Hooks) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, _ int32, metrics kgo.ProduceBatchMetrics) {
	node := strconv.Itoa(int(meta.NodeID))

	h.produceBatchesUncompressed.WithLabelValues(node, topic).Add(float64(metrics.UncompressedBytes))
	h.produceBatchesCompressed.WithLabelValues(node, topic).Add(float64(metrics.CompressedBytes))
	h.produceRecordsTotal.WithLabelValues(node, topic).Add(float64(metrics.NumRecords))
}

func (h *Hooks) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, _ int32, metrics kgo.FetchBatchMetrics) {
	node := strconv.Itoa(int(meta.NodeID))
	group := labelValue(h.labels, groupLabel)

	h.fetchBatchesUncompressed.WithLabelValues(node, topic, group).Add(float64(metrics.UncompressedBytes))
	h.fetchBatchesCompressed.WithLabelValues(node, topic, group).Add(float64(metrics.CompressedBytes))
	h.fetchRecordsTotal.WithLabelValues(node, topic, group).Add(float64(metrics.NumRecords))
}

func newHooks(
	namespace string,
	subsystem string,
	labels prometheus.Labels,
	constLabels prometheus.Labels,
) *Hooks {
	return &Hooks{
		labels: labels,

		// connects and disconnects

		connects: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "connects_total",
			Help:        "Total number of connections opened, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		connectErrs: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "connect_errors_total",
			Help:        "Total number of connection errors, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		disconnects: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "disconnects_total",
			Help:        "Total number of connections closed, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		// write

		writeErrs: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "write_errors_total",
			Help:        "Total number of write errors, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		writeBytes: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "write_bytes_total",
			Help:        "Total number of bytes written, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		writeWaits: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "write_wait_latencies",
			Help:        "Latency of time spent waiting to write to Kafka, in seconds by broker",
			Buckets:     prometheus.ExponentialBuckets(0.0001, 1.5, 20),
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		writeTimings: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "write_latencies",
			Help:        "Latency of time spent writing to Kafka, in seconds by broker",
			Buckets:     prometheus.ExponentialBuckets(0.0001, 1.5, 20),
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		// read

		readErrs: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "read_errors_total",
			Help:        "Total number of read errors, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		readBytes: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "read_bytes_total",
			Help:        "Total number of bytes read, by broker",
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		readWaits: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "read_wait_latencies",
			Help:        "Latency of time spent waiting to read from Kafka, in seconds by broker",
			Buckets:     prometheus.ExponentialBuckets(0.0001, 1.5, 20),
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		readTimings: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "read_latencies",
			Help:        "Latency of time spent reading from Kafka, in seconds by broker",
			Buckets:     prometheus.ExponentialBuckets(0.0001, 1.5, 20),
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		// throttles

		throttles: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "throttle_latencies",
			Help:        "Latency of Kafka request throttles, in seconds by broker",
			Buckets:     prometheus.ExponentialBuckets(0.0001, 1.5, 20),
			ConstLabels: constLabels,
		}, []string{nodeLabel}),

		// produce hook metrics

		produceBatchesUncompressed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "produce_bytes_uncompressed_total",
			Help:        "Total number of uncompressed bytes produced, by broker and topic",
			ConstLabels: constLabels,
		}, []string{nodeLabel, topicLabel}),

		produceBatchesCompressed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "produce_bytes_compressed_total",
			Help:        "Total number of compressed bytes actually produced, by topic and partition",
			ConstLabels: constLabels,
		}, []string{nodeLabel, topicLabel}),

		produceRecordsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "produce_records_total",
			Help:        "Total number of records produced",
			ConstLabels: constLabels,
		}, []string{nodeLabel, topicLabel}),

		// fetch hook metrics

		fetchBatchesUncompressed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "fetch_bytes_uncompressed_total",
			Help:        "Total number of uncompressed bytes fetched, by topic and partition",
			ConstLabels: constLabels,
		}, []string{nodeLabel, topicLabel, groupLabel}),

		fetchBatchesCompressed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "fetch_bytes_compressed_total",
			Help:        "Total number of compressed bytes actually fetched, by topic and partition",
			ConstLabels: constLabels,
		}, []string{nodeLabel, topicLabel, groupLabel}),

		fetchRecordsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "fetch_records_total",
			Help:        "Total number of records fetched",
			ConstLabels: constLabels,
		}, []string{nodeLabel, topicLabel, groupLabel}),
	}
}
