package otelxkafka

import "github.com/mkbeh/xkafka"

const instrumentationName = "github.com/mkbeh/xkafka/extra/otelxkafka"

// Kotel combines OpenTelemetry meter and tracer hooks for xkafka.
type Kotel struct {
	meter  *Meter
	tracer *Tracer
}

// Opt configures Kotel.
type Opt interface {
	apply(*Kotel)
}

type optFunc func(*Kotel)

func (o optFunc) apply(k *Kotel) {
	o(k)
}

// WithTracer configures Kotel with a Tracer.
func WithTracer(tracer *Tracer) Opt {
	return optFunc(func(k *Kotel) {
		if tracer != nil {
			k.tracer = tracer
		}
	})
}

// WithMeter configures Kotel with a Meter.
func WithMeter(meter *Meter) Opt {
	return optFunc(func(k *Kotel) {
		if meter != nil {
			k.meter = meter
		}
	})
}

// NewKotel creates a Kotel and applies opts to it.
func NewKotel(opts ...Opt) *Kotel {
	k := &Kotel{}

	for _, opt := range opts {
		if opt != nil {
			opt.apply(k)
		}
	}

	return k
}

// Hooks returns xkafka hooks for the configured telemetry components.
//
// Each call returns client-scoped hook instances, allowing one Kotel to be
// reused across multiple xkafka clients and group transaction sessions.
func (k *Kotel) Hooks() []xkafka.Hook {
	var hooks []xkafka.Hook

	if k.tracer != nil {
		hooks = append(hooks, k.tracer.clone())
	}
	if k.meter != nil {
		hooks = append(hooks, k.meter.clone())
	}

	return hooks
}
