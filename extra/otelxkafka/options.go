package otelxkafka

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Option configures OpenTelemetry metrics.
//
// The interface is sealed so options can only be created by this package.
type Option interface {
	apply(*settings)
}

type optionFunc func(*settings)

func (option optionFunc) apply(settings *settings) {
	option(settings)
}

type settings struct {
	meterProvider metric.MeterProvider
}

// New creates an OpenTelemetry metrics integration.
//
// When no MeterProvider is configured, the global OpenTelemetry MeterProvider
// is used. The returned value is safe for concurrent use and reuse across
// multiple xkafka clients and group transaction sessions.
func New(options ...Option) (*Metrics, error) {
	settings := settings{
		meterProvider: otel.GetMeterProvider(),
	}

	for _, option := range options {
		if option != nil {
			option.apply(&settings)
		}
	}

	meter := settings.meterProvider.Meter(instrumentationName)
	instruments, err := newClientMetricInstruments(meter)
	if err != nil {
		return nil, err
	}

	return &Metrics{
		meter:       meter,
		instruments: instruments,
	}, nil
}

// WithMeterProvider configures the MeterProvider used for metrics.
//
// A nil provider leaves the global OpenTelemetry MeterProvider in use. The
// caller retains ownership of a non-nil provider.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return optionFunc(func(settings *settings) {
		if provider != nil {
			settings.meterProvider = provider
		}
	})
}
