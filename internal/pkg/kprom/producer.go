package kprom

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type TransactionOutcome string

const (
	TransactionOutcomeCommit TransactionOutcome = "commit"
	TransactionOutcomeError  TransactionOutcome = "error"
)

type ProducerMetrics struct {
	produceErrorsTotal *prometheus.CounterVec

	transactionsTotal   *prometheus.CounterVec
	transactionDuration *prometheus.HistogramVec
}

func newProducerMetrics(namespace, subsystem string, constLabels prometheus.Labels) *ProducerMetrics {
	return &ProducerMetrics{
		produceErrorsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "produce_errors_total",
			Help:        "Total number of producer record errors by topic",
			ConstLabels: constLabels,
		}, []string{topicLabel}),

		transactionsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "transactions_total",
			Help:        "Total number of Kafka transactions by result",
			ConstLabels: constLabels,
		}, []string{outcomeLabel}),

		transactionDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "transaction_duration_seconds",
			Help:        "Kafka transaction duration in seconds",
			Buckets:     defaultDurationBuckets(),
			ConstLabels: constLabels,
		}, []string{}),
	}
}

func (m *ProducerMetrics) CollectProduceError(topic string) {
	m.produceErrorsTotal.WithLabelValues(topic).Inc()
}

func (m *ProducerMetrics) CollectTransaction(startTime time.Time, outcome TransactionOutcome) {
	m.transactionsTotal.WithLabelValues(string(outcome)).Inc()
	m.transactionDuration.WithLabelValues().Observe(time.Since(startTime).Seconds())
}
