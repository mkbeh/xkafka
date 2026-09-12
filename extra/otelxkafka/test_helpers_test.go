package otelxkafka

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const (
	testScopeName        = "github.com/mkbeh/xkafka/extra/otelxkafka"
	testGenericErrorType = "_OTHER"
)

func newTestMeter(t *testing.T, opts ...MeterOpt) (*Meter, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		if err := provider.Shutdown(context.WithoutCancel(t.Context())); err != nil {
			t.Error(err)
		}
	})

	return NewMeter(append(opts, MeterProvider(provider))...), reader
}

func collectScopeMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ScopeMetrics {
	t.Helper()

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	if len(data.ScopeMetrics) != 1 {
		t.Fatalf("scope metrics = %d, want 1", len(data.ScopeMetrics))
	}

	return data.ScopeMetrics[0]
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()

	scope := collectScopeMetrics(t, reader)
	metrics := make(map[string]metricdata.Metrics, len(scope.Metrics))
	for _, instrument := range scope.Metrics {
		metrics[instrument.Name] = instrument
	}

	return metrics
}

func sumData(t *testing.T, data metricdata.Metrics) metricdata.Sum[int64] {
	t.Helper()

	sum, ok := data.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("metric %q: got %T, want Sum[int64]", data.Name, data.Data)
	}

	return sum
}

func onlySumPoint(t *testing.T, sum metricdata.Sum[int64]) metricdata.DataPoint[int64] {
	t.Helper()

	if len(sum.DataPoints) != 1 {
		t.Fatalf("data points = %d, want 1", len(sum.DataPoints))
	}

	return sum.DataPoints[0]
}

func durationHistogram(t *testing.T, data metricdata.Metrics) metricdata.Histogram[float64] {
	t.Helper()

	histogram, ok := data.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("metric %q: got %T, want Histogram[float64]", data.Name, data.Data)
	}
	if data.Unit != "s" {
		t.Fatalf("metric %q unit = %q, want s", data.Name, data.Unit)
	}

	return histogram
}

func onlyHistogramPoint(
	t *testing.T,
	histogram metricdata.Histogram[float64],
) metricdata.HistogramDataPoint[float64] {
	t.Helper()

	if len(histogram.DataPoints) != 1 {
		t.Fatalf("data points = %d, want 1", len(histogram.DataPoints))
	}

	return histogram.DataPoints[0]
}

func assertHistogramPoint(
	t *testing.T,
	point metricdata.HistogramDataPoint[float64],
	count uint64,
	sum float64,
) {
	t.Helper()

	if point.Count != count {
		t.Fatalf("histogram count = %d, want %d", point.Count, count)
	}
	if math.Abs(point.Sum-sum) > 1e-12 {
		t.Fatalf("histogram sum = %g, want %g", point.Sum, sum)
	}
}

func newTestTracer(t *testing.T, opts ...TracerOpt) (*Tracer, *tracetest.SpanRecorder) {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)
	t.Cleanup(func() {
		if err := provider.Shutdown(context.WithoutCancel(t.Context())); err != nil {
			t.Error(err)
		}
	})

	opts = append(
		opts,
		TracerProvider(provider),
		TracerPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		)),
	)

	return NewTracer(opts...), recorder
}

func onlyEndedSpan(t *testing.T, recorder *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}

	return spans[0]
}

func assertAttributes(t *testing.T, got []attribute.KeyValue, want ...attribute.KeyValue) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("attributes = %v, want %v", got, want)
	}

	gotSet := attribute.NewSet(slices.Clone(got)...)
	wantSet := attribute.NewSet(slices.Clone(want)...)
	if !gotSet.Equals(&wantSet) {
		t.Fatalf("attributes = %v, want %v", gotSet.ToSlice(), wantSet.ToSlice())
	}
}

func assertSpanStatus(t *testing.T, span sdktrace.ReadOnlySpan, err error) {
	t.Helper()

	wantCode := codes.Unset
	wantDescription := ""
	if err != nil {
		wantCode = codes.Error
		wantDescription = err.Error()
	}

	status := span.Status()
	if status.Code != wantCode {
		t.Fatalf("span status code = %s, want %s", status.Code, wantCode)
	}
	if status.Description != wantDescription {
		t.Fatalf("span status description = %q, want %q", status.Description, wantDescription)
	}

	for _, event := range span.Events() {
		if event.Name == "exception" {
			t.Fatal("returned errors must not produce exception events")
		}
	}
}

func testSpanContext(traceID, spanID byte, sampled bool) trace.SpanContext {
	var flags trace.TraceFlags
	if sampled {
		flags = trace.FlagsSampled
	}

	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{15: traceID},
		SpanID:     trace.SpanID{7: spanID},
		TraceFlags: flags,
	})
}

func assertSpanContextIdentity(t *testing.T, got, want trace.SpanContext) {
	t.Helper()

	// Propagated contexts become remote; compare identity and sampling only.
	if got.TraceID() != want.TraceID() {
		t.Fatalf("trace ID = %v, want %v", got.TraceID(), want.TraceID())
	}
	if got.SpanID() != want.SpanID() {
		t.Fatalf("span ID = %v, want %v", got.SpanID(), want.SpanID())
	}
	if got.IsSampled() != want.IsSampled() {
		t.Fatalf("sampled = %v, want %v", got.IsSampled(), want.IsSampled())
	}
}

func linkedRecord(sc trace.SpanContext) *kgo.Record {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(
		trace.ContextWithSpanContext(context.Background(), sc),
		carrier,
	)

	record := &kgo.Record{Topic: "a"}
	for key, value := range carrier {
		record.Headers = append(record.Headers, kgo.RecordHeader{
			Key:   key,
			Value: []byte(value),
		})
	}

	return record
}
