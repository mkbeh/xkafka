package otelxkafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mkbeh/xkafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

func TestTracerProduce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		records         []*kgo.Record
		resolvedTopic   string
		wantSpanName    string
		wantDestination string
		wantBatchCount  int
		err             error
		wantErrorType   string
	}{
		{
			name:            "single failed record",
			records:         []*kgo.Record{{Topic: "a"}},
			wantSpanName:    "send a",
			wantDestination: "a",
			err:             fmt.Errorf("produce: %w", kerr.RequestTimedOut),
			wantErrorType:   "REQUEST_TIMED_OUT",
		},
		{
			name:            "same topic batch",
			records:         []*kgo.Record{{Topic: "a"}, {Topic: "a"}},
			wantSpanName:    "send a",
			wantDestination: "a",
			wantBatchCount:  2,
		},
		{
			name:           "mixed topic batch",
			records:        []*kgo.Record{{Topic: "a"}, {Topic: "b"}},
			wantSpanName:   "send",
			wantBatchCount: 2,
		},
		{
			name:            "topic resolved before end",
			records:         []*kgo.Record{{}, {}},
			resolvedTopic:   "orders",
			wantSpanName:    "send orders",
			wantDestination: "orders",
			wantBatchCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracer, recorder := newTestTracer(t, ClientID("client"))
			parent := testSpanContext(1, 1, true)
			ctx := tracer.OnProduceStart(
				trace.ContextWithSpanContext(t.Context(), parent),
				tt.records,
			)
			sendContext := trace.SpanContextFromContext(ctx)

			for _, record := range tt.records {
				tracer.OnProduceRecord(ctx, record)

				extracted := propagation.TraceContext{}.Extract(
					t.Context(),
					recordCarrier{record: record},
				)
				assertSpanContextIdentity(t, trace.SpanContextFromContext(extracted), sendContext)

				if tt.resolvedTopic != "" {
					// franz-go resolves the effective topic before OnProduceEnd.
					record.Topic = tt.resolvedTopic
				}
			}

			if got := len(recorder.Started()); got != 1 {
				t.Fatalf("started spans = %d, want 1", got)
			}
			if got := len(recorder.Ended()); got != 0 {
				t.Fatalf("ended spans before produce end = %d, want 0", got)
			}

			tracer.OnProduceEnd(ctx, tt.records, time.Millisecond, tt.err)

			span := onlyEndedSpan(t, recorder)
			assertSpanContextIdentity(t, span.Parent(), parent)
			if span.Name() != tt.wantSpanName {
				t.Fatalf("span name = %q, want %q", span.Name(), tt.wantSpanName)
			}
			if span.SpanKind() != trace.SpanKindProducer {
				t.Fatalf("span kind = %s, want producer", span.SpanKind())
			}

			wantAttrs := []attribute.KeyValue{
				semconv.MessagingClientID("client"),
				semconv.MessagingSystemKafka,
				semconv.MessagingOperationName("send"),
				semconv.MessagingOperationTypeSend,
			}
			if tt.wantDestination != "" {
				wantAttrs = append(wantAttrs, semconv.MessagingDestinationName(tt.wantDestination))
			}
			if tt.wantBatchCount > 0 {
				wantAttrs = append(wantAttrs, semconv.MessagingBatchMessageCount(tt.wantBatchCount))
			}
			if tt.wantErrorType != "" {
				wantAttrs = append(wantAttrs, semconv.ErrorTypeKey.String(tt.wantErrorType))
			}

			assertAttributes(t, span.Attributes(), wantAttrs...)
			assertSpanStatus(t, span, tt.err)
		})
	}
}

func TestTracerAsyncPropagation(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)

	member, err := baggage.NewMember("tenant", "acme")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}

	sc := testSpanContext(1, 2, true)
	ctx := baggage.ContextWithBaggage(
		trace.ContextWithSpanContext(t.Context(), sc),
		bag,
	)
	record := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: "traceparent", Value: []byte("old")},
		{Key: "application", Value: []byte("preserved")},
	}}

	tracer.OnProduceRecord(ctx, record)
	tracer.OnProduceRecord(ctx, record)

	carrier := recordCarrier{record: record}
	extracted := tracer.propagator.Extract(t.Context(), carrier)
	assertSpanContextIdentity(t, trace.SpanContextFromContext(extracted), sc)

	if got := baggage.FromContext(extracted).Member("tenant").Value(); got != "acme" {
		t.Fatalf("baggage tenant = %q, want %q", got, "acme")
	}
	if got := carrier.Get("application"); got != "preserved" {
		t.Fatalf("application header = %q, want %q", got, "preserved")
	}

	traceparents := 0
	for _, header := range record.Headers {
		if header.Key == "traceparent" {
			traceparents++
		}
	}
	if traceparents != 1 {
		t.Fatalf("traceparent headers = %d, want 1", traceparents)
	}
	if got := len(recorder.Started()); got != 0 {
		t.Fatalf("async propagation started %d spans, want 0", got)
	}
}

func TestTracerHandle(t *testing.T) {
	t.Parallel()

	handlerErr := errors.New("handler failed")
	tests := []struct {
		name            string
		records         []*kgo.Record
		wantSpanName    string
		wantDestination string
		wantPartition   string
		wantBatchCount  int
		err             error
		wantErrorType   string
	}{
		{
			name: "same partition",
			records: []*kgo.Record{
				{Topic: "a", Partition: 2},
				{Topic: "a", Partition: 2},
			},
			wantSpanName:    "process a",
			wantDestination: "a",
			wantPartition:   "2",
			wantBatchCount:  2,
		},
		{
			name: "different partitions with error",
			records: []*kgo.Record{
				{Topic: "a", Partition: 0},
				{Topic: "a", Partition: 1},
			},
			wantSpanName:    "process a",
			wantDestination: "a",
			wantBatchCount:  2,
			err:             handlerErr,
			wantErrorType:   testGenericErrorType,
		},
		{
			name: "mixed topics",
			records: []*kgo.Record{
				{Topic: "a", Partition: 0},
				{Topic: "b", Partition: 0},
			},
			wantSpanName:   "process",
			wantBatchCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracer, recorder := newTestTracer(t, ClientID("client"), ConsumerGroup("workers"))
			ctx := tracer.OnHandleStart(t.Context(), tt.records)
			tracer.OnHandleEnd(ctx, tt.records, time.Millisecond, tt.err)

			span := onlyEndedSpan(t, recorder)
			if span.Name() != tt.wantSpanName {
				t.Fatalf("span name = %q, want %q", span.Name(), tt.wantSpanName)
			}
			if span.SpanKind() != trace.SpanKindConsumer {
				t.Fatalf("span kind = %s, want consumer", span.SpanKind())
			}

			wantAttrs := []attribute.KeyValue{
				semconv.MessagingClientID("client"),
				semconv.MessagingConsumerGroupName("workers"),
				semconv.MessagingSystemKafka,
				semconv.MessagingOperationName("process"),
				semconv.MessagingOperationTypeProcess,
			}
			if tt.wantDestination != "" {
				wantAttrs = append(wantAttrs, semconv.MessagingDestinationName(tt.wantDestination))
			}
			if tt.wantPartition != "" {
				wantAttrs = append(wantAttrs, semconv.MessagingDestinationPartitionID(tt.wantPartition))
			}
			if tt.wantBatchCount > 0 {
				wantAttrs = append(wantAttrs, semconv.MessagingBatchMessageCount(tt.wantBatchCount))
			}
			if tt.wantErrorType != "" {
				wantAttrs = append(wantAttrs, semconv.ErrorTypeKey.String(tt.wantErrorType))
			}

			assertAttributes(t, span.Attributes(), wantAttrs...)
			assertSpanStatus(t, span, tt.err)
		})
	}
}

func TestTracerRecordLinks(t *testing.T) {
	t.Parallel()

	a := testSpanContext(1, 1, true)
	b := testSpanContext(1, 2, true)
	c := testSpanContext(2, 1, true)

	state, err := trace.ParseTraceState("vendor=other")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		records []*kgo.Record
		want    []trace.SpanContext
	}{
		{
			name: "deduplicates by trace and span ID",
			records: []*kgo.Record{
				linkedRecord(a),
				linkedRecord(a.WithTraceState(state)),
				linkedRecord(b),
				linkedRecord(a),
				linkedRecord(c),
				linkedRecord(b),
			},
			want: []trace.SpanContext{a, b, c},
		},
		{
			name: "ignores invalid and unsampled contexts",
			records: []*kgo.Record{
				nil,
				{},
				linkedRecord(testSpanContext(3, 1, false)),
				{Headers: []kgo.RecordHeader{{Key: "traceparent", Value: []byte("invalid")}}},
			},
		},
		{
			name: "ignores record context without propagated headers",
			records: []*kgo.Record{
				{
					Topic:   "a",
					Context: trace.ContextWithRemoteSpanContext(t.Context(), a),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracer, recorder := newTestTracer(t)
			ctx := tracer.OnHandleStart(t.Context(), tt.records)
			tracer.OnHandleEnd(ctx, tt.records, time.Millisecond, nil)

			span := onlyEndedSpan(t, recorder)
			links := span.Links()
			if len(links) != len(tt.want) {
				t.Fatalf("links = %d, want %d", len(links), len(tt.want))
			}
			if span.DroppedLinks() != 0 {
				t.Fatalf("dropped links = %d, want 0", span.DroppedLinks())
			}
			for i, link := range links {
				assertSpanContextIdentity(t, link.SpanContext, tt.want[i])
			}
			if span.Parent().IsValid() {
				t.Fatal("message creation contexts must be links, not a selected batch parent")
			}
		})
	}
}

func TestTracerSettlementTimestamps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		group         ClientOpt
		groupAttr     attribute.KeyValue
		record        func(*Tracer, context.Context, time.Duration, error)
		err           error
		wantErrorType string
	}{
		{
			name:      "commit",
			group:     ConsumerGroup("workers"),
			groupAttr: semconv.MessagingConsumerGroupName("workers"),
			record:    (*Tracer).OnOffsetCommit,
		},
		{
			name:          "ack",
			group:         ShareGroup("shared"),
			groupAttr:     attribute.String("xkafka.share.group.name", "shared"),
			record:        (*Tracer).OnShareAckFlush,
			err:           kerr.RequestTimedOut,
			wantErrorType: "REQUEST_TIMED_OUT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracer, recorder := newTestTracer(t, tt.group)
			records := []*kgo.Record{{Topic: "a"}}
			ctx := tracer.OnHandleStart(t.Context(), records)
			parent := trace.SpanContextFromContext(ctx)
			tracer.OnHandleEnd(ctx, records, 0, nil)

			const duration = 37 * time.Millisecond
			before := time.Now()
			tt.record(tracer, ctx, duration, tt.err)
			after := time.Now()

			spans := recorder.Ended()
			if len(spans) != 2 {
				t.Fatalf("ended spans = %d, want process and settlement", len(spans))
			}
			span := spans[1]

			if span.Name() != tt.name {
				t.Fatalf("span name = %q, want %q", span.Name(), tt.name)
			}
			if span.SpanKind() != trace.SpanKindClient {
				t.Fatalf("span kind = %s, want client", span.SpanKind())
			}
			assertSpanContextIdentity(t, span.Parent(), parent)

			if got := span.EndTime().Sub(span.StartTime()); got != duration {
				t.Fatalf("span duration = %v, want %v", got, duration)
			}
			if span.EndTime().Before(before) || span.EndTime().After(after) {
				t.Fatalf("span end time %v is outside %v .. %v", span.EndTime(), before, after)
			}

			wantAttrs := []attribute.KeyValue{
				tt.groupAttr,
				semconv.MessagingSystemKafka,
				semconv.MessagingOperationName(tt.name),
				semconv.MessagingOperationTypeSettle,
			}
			if tt.wantErrorType != "" {
				wantAttrs = append(wantAttrs, semconv.ErrorTypeKey.String(tt.wantErrorType))
			}

			assertAttributes(t, span.Attributes(), wantAttrs...)
			assertSpanStatus(t, span, tt.err)
		})
	}
}

func TestTracerTransactions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		transactionType   xkafka.TransactionType
		outcome           xkafka.TransactionOutcome
		err               error
		wantType          string
		wantOutcome       string
		wantConsumerGroup bool
		wantErrorType     string
	}{
		{
			name:            "producer abort",
			transactionType: xkafka.TransactionTypeProducer,
			outcome:         xkafka.TransactionOutcomeAbort,
			wantType:        "producer",
			wantOutcome:     "abort",
		},
		{
			name:              "group error",
			transactionType:   xkafka.TransactionTypeGroup,
			outcome:           xkafka.TransactionOutcomeError,
			err:               kerr.RequestTimedOut,
			wantType:          "group",
			wantOutcome:       "error",
			wantConsumerGroup: true,
			wantErrorType:     "REQUEST_TIMED_OUT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracer, recorder := newTestTracer(t, ClientID("client"), ConsumerGroup("workers"))
			ctx := tracer.OnTransactionStart(t.Context(), tt.transactionType)
			tracer.OnTransactionEnd(ctx, tt.transactionType, tt.outcome, 0, tt.err)

			span := onlyEndedSpan(t, recorder)
			if span.Name() != "xkafka.transaction" {
				t.Fatalf("span name = %q, want %q", span.Name(), "xkafka.transaction")
			}
			if span.SpanKind() != trace.SpanKindInternal {
				t.Fatalf("span kind = %s, want internal", span.SpanKind())
			}

			wantAttrs := []attribute.KeyValue{
				semconv.MessagingClientID("client"),
			}
			if tt.wantConsumerGroup {
				wantAttrs = append(wantAttrs, semconv.MessagingConsumerGroupName("workers"))
			}
			wantAttrs = append(
				wantAttrs,
				semconv.MessagingSystemKafka,
				attribute.String("xkafka.transaction.type", tt.wantType),
				attribute.String("xkafka.transaction.outcome", tt.wantOutcome),
			)
			if tt.wantErrorType != "" {
				wantAttrs = append(wantAttrs, semconv.ErrorTypeKey.String(tt.wantErrorType))
			}

			assertAttributes(t, span.Attributes(), wantAttrs...)
			assertSpanStatus(t, span, tt.err)
		})
	}
}

func TestTracerSpanOwnership(t *testing.T) {
	t.Parallel()

	tracer, recorder := newTestTracer(t)
	other := NewTracer(TracerProvider(tracer.provider))
	records := []*kgo.Record{{Topic: "a"}}

	txCtx := tracer.OnTransactionStart(t.Context(), xkafka.TransactionTypeGroup)
	handleCtx := tracer.OnHandleStart(txCtx, records)
	sendCtx := tracer.OnProduceStart(handleCtx, records)

	other.OnProduceEnd(sendCtx, records, 0, nil)
	if got := len(recorder.Ended()); got != 0 {
		t.Fatalf("another tracer ended %d spans, want 0", got)
	}

	tracer.OnProduceEnd(sendCtx, records, 0, nil)
	if !trace.SpanFromContext(handleCtx).IsRecording() {
		t.Fatal("ending producer span ended handler span")
	}
	if !trace.SpanFromContext(txCtx).IsRecording() {
		t.Fatal("ending producer span ended transaction span")
	}

	tracer.OnHandleEnd(handleCtx, records, 0, nil)
	tracer.OnTransactionEnd(
		txCtx,
		xkafka.TransactionTypeGroup,
		xkafka.TransactionOutcomeCommit,
		0,
		nil,
	)

	spans := recorder.Ended()
	if len(spans) != 3 {
		t.Fatalf("ended spans = %d, want 3", len(spans))
	}
	assertSpanContextIdentity(t, spans[0].Parent(), trace.SpanContextFromContext(handleCtx))
	assertSpanContextIdentity(t, spans[1].Parent(), trace.SpanContextFromContext(txCtx))
}
