package kprom

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ConsumerMetrics struct {
	labels prometheus.Labels

	handleDuration *prometheus.HistogramVec
	handleErrors   *prometheus.CounterVec
}

func newConsumerMetrics(
	namespace string,
	subsystem string,
	labels prometheus.Labels,
	constLabels prometheus.Labels,
) *ConsumerMetrics {
	return &ConsumerMetrics{
		labels: labels,

		handleDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "consume_handle_duration_seconds",
			Help:        "Kafka consumer handler duration in seconds",
			Buckets:     defaultDurationBuckets(),
			ConstLabels: constLabels,
		}, []string{topicLabel, groupLabel}),

		handleErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "consume_errors_total",
			Help:        "Number of messages failed to consume",
			ConstLabels: constLabels,
		}, []string{topicLabel, groupLabel}),
	}
}

func (m *ConsumerMetrics) CollectHandleProcessTiming(startTime time.Time, topic string) {
	m.handleDuration.WithLabelValues(topic, labelValue(m.labels, groupLabel)).
		Observe(time.Since(startTime).Seconds())
}

func (m *ConsumerMetrics) CollectHandleErrors(topic string) {
	m.handleErrors.WithLabelValues(topic, labelValue(m.labels, groupLabel)).Inc()
}
