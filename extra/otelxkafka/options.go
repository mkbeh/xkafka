package otelxkafka

import "maps"

// ClientOpt configures OpenTelemetry attributes shared by Meter and Tracer.
//
// Client options can be passed to both [NewMeter] and [NewTracer].
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

// ClientID sets the messaging client ID attribute.
//
// An empty value omits the attribute.
func ClientID(clientID string) ClientOpt {
	return clientOptFunc(func(cfg *clientConfig) {
		cfg.clientID = clientID
	})
}

// ConsumerGroup sets the messaging consumer group attribute.
//
// Configuring ConsumerGroup clears any Share Group. An empty value omits the
// consumer group attribute. If [ConsumerGroup] and [ShareGroup] are both used,
// the option applied last takes precedence.
func ConsumerGroup(group string) ClientOpt {
	return clientOptFunc(func(cfg *clientConfig) {
		cfg.consumerGroup = group
		cfg.shareGroup = ""
	})
}

// ShareGroup sets the xkafka Share Group attribute.
//
// Configuring ShareGroup clears any consumer group. An empty value omits the
// Share Group attribute. If [ConsumerGroup] and [ShareGroup] are both used, the
// option applied last takes precedence.
func ShareGroup(group string) ClientOpt {
	return clientOptFunc(func(cfg *clientConfig) {
		cfg.shareGroup = group
		cfg.consumerGroup = ""
	})
}

// Labels adds custom OpenTelemetry attributes shared by Meter and Tracer.
//
// The provided map is copied when Labels is called. Repeated calls merge
// attributes, with later values replacing duplicate keys.
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
