package kafka

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	topicLabel = "topic"
	groupLabel = "consumer_group"
)

var (
	consumerProcessTiming = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Namespace:  "ds",
		Name:       "kafka_consume_handle_timing",
		Help:       "Handling duration of consumer split by quantiles (max age = 1m)",
		MaxAge:     time.Minute,
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
	}, []string{groupLabel, topicLabel})

	consumerErrs = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ds",
		Name:      "kafka_consume_errors",
		Help:      "Number of messages failed to consume",
	}, []string{groupLabel, topicLabel})
)
