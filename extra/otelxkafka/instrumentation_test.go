package otelxkafka

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

func TestInstrumentationScope(t *testing.T) {
	t.Parallel()

	assertScope := func(t *testing.T, name, version, schemaURL string) {
		t.Helper()

		if name != testScopeName {
			t.Fatalf("scope name = %q, want %q", name, testScopeName)
		}
		if version != Version() {
			t.Fatalf("scope version = %q, want %q", version, Version())
		}
		if schemaURL != semconv.SchemaURL {
			t.Fatalf("scope schema URL = %q, want %q", schemaURL, semconv.SchemaURL)
		}
	}

	t.Run("meter", func(t *testing.T) {
		meter, reader := newTestMeter(t)
		meter.OnHandleEnd(t.Context(), []*kgo.Record{{Topic: "a"}}, time.Millisecond, nil)

		scope := collectScopeMetrics(t, reader).Scope
		assertScope(t, scope.Name, scope.Version, scope.SchemaURL)
	})

	t.Run("tracer", func(t *testing.T) {
		tracer, recorder := newTestTracer(t)
		records := []*kgo.Record{{Topic: "a"}}
		ctx := tracer.OnProduceStart(t.Context(), records)
		tracer.OnProduceEnd(ctx, records, time.Millisecond, nil)

		scope := onlyEndedSpan(t, recorder).InstrumentationScope()
		assertScope(t, scope.Name, scope.Version, scope.SchemaURL)
	})
}
