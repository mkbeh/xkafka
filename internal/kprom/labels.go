package kprom

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	topicLabel   = "topic"
	groupLabel   = "consumer_group"
	clientLabel  = "client_id"
	outcomeLabel = "outcome"
)

func baseConstLabels(labels prometheus.Labels) prometheus.Labels {
	return prometheus.Labels{
		clientLabel: labels[clientLabel],
	}
}

func cloneLabels(labels prometheus.Labels) prometheus.Labels {
	cloned := make(prometheus.Labels, len(labels))
	for k, v := range labels {
		cloned[k] = v
	}
	return cloned
}

func labelValue(labels prometheus.Labels, key string) string {
	if labels == nil {
		return ""
	}

	return labels[key]
}

func defaultDurationBuckets() []float64 {
	return []float64{
		0.001, // 1ms
		0.002,
		0.005,
		0.01,
		0.025,
		0.05,
		0.1,
		0.25,
		0.5,
		1,
		2.5,
		5,
		10,
		30,
		60,
	}
}
