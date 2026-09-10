package otelxkafka

import "maps"

// ClientOpt configures client attributes shared by Meter and Tracer.
type ClientOpt interface {
	MeterOpt
	TracerOpt
}

type clientConfig struct {
	clientID      string
	labels        map[string]string
	consumerGroup string
	shareGroup    string
}

type clientOptFunc func(*clientConfig)

func (o clientOptFunc) applyMeter(cfg *meterConfig) {
	o(&cfg.client)
}

func (o clientOptFunc) applyTracer(cfg *tracerConfig) {
	o(&cfg.client)
}

// ClientID configures the messaging client ID attribute.
func ClientID(clientID string) ClientOpt {
	return clientOptFunc(func(cfg *clientConfig) {
		cfg.clientID = clientID
	})
}

// ConsumerGroup configures the messaging consumer group attribute.
func ConsumerGroup(group string) ClientOpt {
	return clientOptFunc(func(cfg *clientConfig) {
		cfg.consumerGroup = group
		cfg.shareGroup = ""
	})
}

// ShareGroup configures the xkafka Share Group attribute.
func ShareGroup(group string) ClientOpt {
	return clientOptFunc(func(cfg *clientConfig) {
		cfg.shareGroup = group
		cfg.consumerGroup = ""
	})
}

// Labels configures custom OpenTelemetry attributes.
// Repeated calls merge labels, with later values replacing duplicate keys.
func Labels(labels map[string]string) ClientOpt {
	values := maps.Clone(labels)

	return clientOptFunc(func(cfg *clientConfig) {
		if len(values) == 0 {
			return
		}

		if cfg.labels == nil {
			cfg.labels = maps.Clone(values)
			return
		}

		maps.Copy(cfg.labels, values)
	})
}
