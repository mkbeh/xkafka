package kprom

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	hooks    *Hooks
	producer *ProducerMetrics
	consumer *ConsumerMetrics
}

func NewMetrics(namespace, subsystem string, labels prometheus.Labels) *Metrics {
	labels = cloneLabels(labels)
	constLabels := baseConstLabels(labels)

	return &Metrics{
		hooks:    newHooks(namespace, subsystem, labels, constLabels),
		producer: newProducerMetrics(namespace, subsystem, constLabels),
		consumer: newConsumerMetrics(namespace, subsystem, labels, constLabels),
	}
}

func (m *Metrics) Hooks() *Hooks {
	return m.hooks
}

func (m *Metrics) Producer() *ProducerMetrics {
	return m.producer
}

func (m *Metrics) Consumer() *ConsumerMetrics {
	return m.consumer
}
