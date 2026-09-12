package otelxkafka

import "github.com/mkbeh/xkafka"

// Kotel combines OpenTelemetry instrumentation components into xkafka hooks.
//
// Configure it with [WithMeter], [WithTracer], or both, then pass [Kotel.Hooks]
// to [xkafka.WithHooks].
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

// WithTracer sets the Tracer used by Kotel.
//
// A nil tracer is ignored.
func WithTracer(tracer *Tracer) Opt {
	return optFunc(func(k *Kotel) {
		if tracer != nil {
			k.tracer = tracer
		}
	})
}

// WithMeter sets the Meter used by Kotel.
//
// A nil meter is ignored.
func WithMeter(meter *Meter) Opt {
	return optFunc(func(k *Kotel) {
		if meter != nil {
			k.meter = meter
		}
	})
}

// NewKotel creates a Kotel configured with opts.
func NewKotel(opts ...Opt) *Kotel {
	k := &Kotel{}

	for _, opt := range opts {
		opt.apply(k)
	}

	return k
}

// Hooks returns the configured telemetry components as xkafka hooks.
//
// If no telemetry components are configured, Hooks returns no hooks.
func (k *Kotel) Hooks() []xkafka.Hook {
	var hooks []xkafka.Hook

	if k.tracer != nil {
		hooks = append(hooks, k.tracer)
	}
	if k.meter != nil {
		hooks = append(hooks, k.meter)
	}

	return hooks
}
