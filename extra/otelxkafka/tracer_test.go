package otelxkafka

import (
	"context"
	"testing"
	"time"

	"github.com/mkbeh/xkafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

func TestTracerHandleSpan(t *testing.T) {
	tracer, recorder := newTestTracer(t)
	tracer.setRuntime("orders", map[string]string{"service": "orders-api"}, "orders-group", "")

	firstContext := testSpanContext(1, 1)
	secondContext := testSpanContext(2, 2)

	records := []*kgo.Record{
		{
			Topic:     "orders",
			Partition: 1,
			Offset:    10,
		},
		{
			Topic:     "orders",
			Partition: 2,
			Offset:    20,
		},
	}
	injectSpanContext(t, tracer.propagator, records[0], firstContext)
	injectSpanContext(t, tracer.propagator, records[1], secondContext)

	ctx := tracer.OnHandleStart(context.Background(), records)
	tracer.OnHandleEnd(ctx, records, time.Second, nil)

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}

	span := spans[0]
	if span.Name() != "handle orders" {
		t.Fatalf("span name = %q, want %q", span.Name(), "handle orders")
	}
	if span.SpanKind() != trace.SpanKindConsumer {
		t.Fatalf("span kind = %v, want %v", span.SpanKind(), trace.SpanKindConsumer)
	}
	if span.Status().Code != codes.Unset {
		t.Fatalf("span status = %v, want %v", span.Status().Code, codes.Unset)
	}

	scope := span.InstrumentationScope()
	if scope.Name != instrumentationName {
		t.Fatalf("scope name = %q, want %q", scope.Name, instrumentationName)
	}
	if scope.Version != semVersion() {
		t.Fatalf("scope version = %q, want %q", scope.Version, semVersion())
	}
	if scope.SchemaURL != semconv.SchemaURL {
		t.Fatalf("scope schema URL = %q, want %q", scope.SchemaURL, semconv.SchemaURL)
	}

	assertStringAttribute(t, span.Attributes(), messagingSystemAttribute, messagingSystemKafka)
	assertStringAttribute(t, span.Attributes(), operationNameAttribute, handleOperationName)
	assertStringAttribute(t, span.Attributes(), operationTypeAttribute, processOperationType)
	assertStringAttribute(t, span.Attributes(), clientIDAttribute, "orders")
	assertStringAttribute(t, span.Attributes(), consumerGroupAttribute, "orders-group")
	assertStringAttribute(t, span.Attributes(), destinationNameAttribute, "orders")
	assertInt64Attribute(t, span.Attributes(), recordCountAttribute, 2)
	assertAttributeMissing(t, span.Attributes(), destinationPartitionIDAttribute)

	links := span.Links()
	if len(links) != 2 {
		t.Fatalf("span links = %d, want 2", len(links))
	}
	if links[0].SpanContext != firstContext {
		t.Fatalf("first link span context = %v, want %v", links[0].SpanContext, firstContext)
	}
	if links[1].SpanContext != secondContext {
		t.Fatalf("second link span context = %v, want %v", links[1].SpanContext, secondContext)
	}

	assertStringAttribute(t, links[0].Attributes, destinationNameAttribute, "orders")
	assertStringAttribute(t, links[0].Attributes, destinationPartitionIDAttribute, "1")
	assertInt64Attribute(t, links[0].Attributes, kafkaOffsetAttribute, 10)

	assertStringAttribute(t, links[1].Attributes, destinationNameAttribute, "orders")
	assertStringAttribute(t, links[1].Attributes, destinationPartitionIDAttribute, "2")
	assertInt64Attribute(t, links[1].Attributes, kafkaOffsetAttribute, 20)
}

func TestTracerHandleSpanMixedTopics(t *testing.T) {
	tracer, recorder := newTestTracer(t)
	tracer.setRuntime("consumer", nil, "consumer-group", "")

	records := []*kgo.Record{
		{Topic: "orders", Partition: 0, Offset: 1},
		{Topic: "payments", Partition: 0, Offset: 2},
	}

	ctx := tracer.OnHandleStart(context.Background(), records)
	tracer.OnHandleEnd(ctx, records, time.Second, kerr.UnknownTopicOrPartition)

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}

	span := spans[0]
	if span.Name() != handleOperationName {
		t.Fatalf("span name = %q, want %q", span.Name(), handleOperationName)
	}
	assertAttributeMissing(t, span.Attributes(), destinationNameAttribute)
	assertAttributeMissing(t, span.Attributes(), destinationPartitionIDAttribute)
	assertStringAttribute(t, span.Attributes(), errorTypeAttribute, kerr.UnknownTopicOrPartition.Message)

	if span.Status().Code != codes.Error {
		t.Fatalf("span status = %v, want %v", span.Status().Code, codes.Error)
	}
	if len(span.Events()) != 1 {
		t.Fatalf("span events = %d, want 1", len(span.Events()))
	}
}

func TestTracerRecordLinksFallback(t *testing.T) {
	tracer, _ := newTestTracer(t)

	t.Run("local context is ignored", func(t *testing.T) {
		spanContext := testLocalSpanContext(3, 3)
		record := &kgo.Record{
			Topic:   "orders",
			Context: trace.ContextWithSpanContext(context.Background(), spanContext),
		}

		links := tracer.recordLinks([]*kgo.Record{record})
		if len(links) != 0 {
			t.Fatalf("record links = %d, want 0", len(links))
		}
	})

	t.Run("remote context is used", func(t *testing.T) {
		spanContext := testSpanContext(4, 4)
		record := &kgo.Record{
			Topic:   "orders",
			Context: trace.ContextWithRemoteSpanContext(context.Background(), spanContext),
		}

		links := tracer.recordLinks([]*kgo.Record{record})
		if len(links) != 1 {
			t.Fatalf("record links = %d, want 1", len(links))
		}
		if links[0].SpanContext != spanContext {
			t.Fatalf("link span context = %v, want %v", links[0].SpanContext, spanContext)
		}
	})
}

func TestTracerSettlementSpans(t *testing.T) {
	t.Run("offset commit", func(t *testing.T) {
		tracer, recorder := newTestTracer(t)
		tracer.setRuntime("consumer", nil, "consumer-group", "")

		parent := testSpanContext(3, 3)
		ctx := trace.ContextWithSpanContext(context.Background(), parent)
		duration := 75 * time.Millisecond

		tracer.OnOffsetCommit(ctx, duration, nil)

		spans := recorder.Ended()
		if len(spans) != 1 {
			t.Fatalf("ended spans = %d, want 1", len(spans))
		}

		span := spans[0]
		if span.Name() != offsetCommitOperationName {
			t.Fatalf("span name = %q, want %q", span.Name(), offsetCommitOperationName)
		}
		if span.SpanKind() != trace.SpanKindClient {
			t.Fatalf("span kind = %v, want %v", span.SpanKind(), trace.SpanKindClient)
		}
		if span.Parent().TraceID() != parent.TraceID() || span.Parent().SpanID() != parent.SpanID() {
			t.Fatalf("span parent = %v, want trace/span IDs from %v", span.Parent(), parent)
		}
		if got := span.EndTime().Sub(span.StartTime()); got != duration {
			t.Fatalf("span duration = %v, want %v", got, duration)
		}

		assertStringAttribute(t, span.Attributes(), messagingSystemAttribute, messagingSystemKafka)
		assertStringAttribute(t, span.Attributes(), operationNameAttribute, offsetCommitOperationName)
		assertStringAttribute(t, span.Attributes(), operationTypeAttribute, settleOperationType)
		assertStringAttribute(t, span.Attributes(), consumerGroupAttribute, "consumer-group")
	})

	t.Run("share ack", func(t *testing.T) {
		tracer, recorder := newTestTracer(t)
		tracer.setRuntime("share", nil, "", "share-group")

		tracer.OnShareAckFlush(
			context.Background(),
			25*time.Millisecond,
			kerr.UnknownTopicOrPartition,
		)

		spans := recorder.Ended()
		if len(spans) != 1 {
			t.Fatalf("ended spans = %d, want 1", len(spans))
		}

		span := spans[0]
		if span.Name() != shareAckOperationName {
			t.Fatalf("span name = %q, want %q", span.Name(), shareAckOperationName)
		}
		assertStringAttribute(t, span.Attributes(), shareGroupAttribute, "share-group")
		assertAttributeMissing(t, span.Attributes(), consumerGroupAttribute)
		assertStringAttribute(t, span.Attributes(), errorTypeAttribute, kerr.UnknownTopicOrPartition.Message)
		if span.Status().Code != codes.Error {
			t.Fatalf("span status = %v, want %v", span.Status().Code, codes.Error)
		}
	})
}

func TestTracerGroupTransactionParentsHandler(t *testing.T) {
	tracer, recorder := newTestTracer(t)
	tracer.setRuntime("eos", nil, "eos-group", "")

	txCtx := tracer.OnTransactionStart(context.Background(), xkafka.TransactionTypeGroup)
	handleCtx := tracer.OnHandleStart(txCtx, []*kgo.Record{{Topic: "input", Partition: 0, Offset: 1}})
	tracer.OnHandleEnd(handleCtx, nil, time.Second, nil)
	tracer.OnTransactionEnd(
		txCtx,
		xkafka.TransactionTypeGroup,
		xkafka.TransactionOutcomeCommit,
		time.Second,
		nil,
	)

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("ended spans = %d, want 2", len(spans))
	}

	var handler, transaction sdktrace.ReadOnlySpan
	for _, span := range spans {
		switch span.Name() {
		case "handle input":
			handler = span
		case transactionSpanName:
			transaction = span
		}
	}
	if handler == nil {
		t.Fatal("handler span not found")
	}
	if transaction == nil {
		t.Fatal("transaction span not found")
	}

	if transaction.SpanKind() != trace.SpanKindInternal {
		t.Fatalf("transaction span kind = %v, want %v", transaction.SpanKind(), trace.SpanKindInternal)
	}
	if handler.Parent().SpanID() != transaction.SpanContext().SpanID() {
		t.Fatalf(
			"handler parent span ID = %v, want transaction span ID %v",
			handler.Parent().SpanID(),
			transaction.SpanContext().SpanID(),
		)
	}

	assertStringAttribute(t, transaction.Attributes(), messagingSystemAttribute, messagingSystemKafka)
	assertStringAttribute(t, transaction.Attributes(), consumerGroupAttribute, "eos-group")
	assertStringAttribute(t, transaction.Attributes(), transactionTypeAttribute, string(xkafka.TransactionTypeGroup))
	assertStringAttribute(t, transaction.Attributes(), transactionOutcomeAttribute, string(xkafka.TransactionOutcomeCommit))
}

func newTestTracer(t *testing.T) (*Tracer, *tracetest.SpanRecorder) {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(recorder),
	)
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown tracer provider: %v", err)
		}
	})

	tracer := NewTracer(
		TracerProvider(provider),
		TracerPropagator(propagation.TraceContext{}),
	)

	return tracer, recorder
}

func injectSpanContext(
	t *testing.T,
	propagator propagation.TextMapPropagator,
	record *kgo.Record,
	spanContext trace.SpanContext,
) {
	t.Helper()

	ctx := trace.ContextWithRemoteSpanContext(context.Background(), spanContext)
	propagator.Inject(ctx, recordCarrier{record: record})
}

func testSpanContext(traceSeed, spanSeed byte) trace.SpanContext {
	var traceID trace.TraceID
	var spanID trace.SpanID

	for i := range traceID {
		traceID[i] = traceSeed
	}
	for i := range spanID {
		spanID[i] = spanSeed
	}

	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
}

func testLocalSpanContext(traceSeed, spanSeed byte) trace.SpanContext {
	spanContext := testSpanContext(traceSeed, spanSeed)

	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    spanContext.TraceID(),
		SpanID:     spanContext.SpanID(),
		TraceFlags: spanContext.TraceFlags(),
	})
}

func assertStringAttribute(
	t *testing.T,
	attrs []attribute.KeyValue,
	key string,
	want string,
) {
	t.Helper()

	value, ok := findAttribute(attrs, key)
	if !ok {
		t.Fatalf("attribute %q not found", key)
	}
	if got := value.AsString(); got != want {
		t.Fatalf("attribute %q = %q, want %q", key, got, want)
	}
}

func assertInt64Attribute(
	t *testing.T,
	attrs []attribute.KeyValue,
	key string,
	want int64,
) {
	t.Helper()

	value, ok := findAttribute(attrs, key)
	if !ok {
		t.Fatalf("attribute %q not found", key)
	}
	if got := value.AsInt64(); got != want {
		t.Fatalf("attribute %q = %d, want %d", key, got, want)
	}
}

func assertAttributeMissing(t *testing.T, attrs []attribute.KeyValue, key string) {
	t.Helper()

	if _, ok := findAttribute(attrs, key); ok {
		t.Fatalf("attribute %q unexpectedly found", key)
	}
}

func findAttribute(attrs []attribute.KeyValue, key string) (attribute.Value, bool) {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value, true
		}
	}

	return attribute.Value{}, false
}
