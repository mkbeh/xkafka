package otelxkafka

import (
	"context"
	"strconv"
	"time"

	"github.com/mkbeh/xkafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	_ xkafka.HookProduceStart     = new(Tracer)
	_ xkafka.HookProduceRecord    = new(Tracer)
	_ xkafka.HookProduceEnd       = new(Tracer)
	_ xkafka.HookHandleStart      = new(Tracer)
	_ xkafka.HookHandleEnd        = new(Tracer)
	_ xkafka.HookOffsetCommit     = new(Tracer)
	_ xkafka.HookShareAckFlush    = new(Tracer)
	_ xkafka.HookTransactionStart = new(Tracer)
	_ xkafka.HookTransactionEnd   = new(Tracer)
)

const transactionSpanName = "xkafka.transaction"

type spanType uint8

const (
	producerSpan spanType = iota + 1
	handlerSpan
	transactionSpan
)

type spanKey struct {
	tracer *Tracer
	kind   spanType
}

type spanContextKey struct {
	traceID trace.TraceID
	spanID  trace.SpanID
}

// Tracer traces synchronous produce and xkafka runtime operations and propagates
// trace context through Kafka record headers.
//
// ProduceSync creates one producer span for the whole operation and propagates
// that span context to every record. Asynchronous Produce and TryProduce only
// propagate their current context, avoiding per-record producer spans. Consumer
// handler spans link to unique sampled message creation contexts.
type Tracer struct {
	provider   trace.TracerProvider
	propagator propagation.TextMapPropagator
	tracer     trace.Tracer

	clientAttrs   []attribute.KeyValue
	consumerAttrs []attribute.KeyValue
}

type tracerConfig struct {
	provider   trace.TracerProvider
	propagator propagation.TextMapPropagator
	client     clientConfig
}

// TracerOpt configures Tracer.
type TracerOpt interface {
	applyTracer(*tracerConfig)
}

type tracerOptFunc func(*tracerConfig)

func (o tracerOptFunc) applyTracer(cfg *tracerConfig) {
	o(cfg)
}

// TracerProvider configures the OpenTelemetry TracerProvider used by Tracer.
//
// If none is specified, the global TracerProvider is used.
func TracerProvider(provider trace.TracerProvider) TracerOpt {
	return tracerOptFunc(func(cfg *tracerConfig) {
		if provider != nil {
			cfg.provider = provider
		}
	})
}

// TracerPropagator configures the OpenTelemetry TextMapPropagator used to
// inject and extract message creation contexts in Kafka record headers.
//
// If none is specified, the global TextMapPropagator is used.
func TracerPropagator(propagator propagation.TextMapPropagator) TracerOpt {
	return tracerOptFunc(func(cfg *tracerConfig) {
		if propagator != nil {
			cfg.propagator = propagator
		}
	})
}

// NewTracer creates a Tracer for xkafka runtime operations.
func NewTracer(opts ...TracerOpt) *Tracer {
	var cfg tracerConfig

	for _, opt := range opts {
		opt.applyTracer(&cfg)
	}

	if cfg.provider == nil {
		cfg.provider = otel.GetTracerProvider()
	}
	if cfg.propagator == nil {
		cfg.propagator = otel.GetTextMapPropagator()
	}

	t := &Tracer{
		provider:   cfg.provider,
		propagator: cfg.propagator,
	}
	t.tracer = t.provider.Tracer(
		instrumentationName,
		trace.WithInstrumentationVersion(semVersion()),
		trace.WithSchemaURL(semconv.SchemaURL),
	)
	t.clientAttrs, t.consumerAttrs = newAttributeSets(
		cfg.client.clientID,
		cfg.client.consumerGroup,
		cfg.client.shareGroup,
		cfg.client.labels,
	)

	return t
}

// OnProduceStart implements xkafka.HookProduceStart.
func (t *Tracer) OnProduceStart(ctx context.Context, records []*kgo.Record) context.Context {
	if len(records) == 0 {
		return ctx
	}

	attrs := t.attributes(
		semconv.MessagingSystemKafka,
		semconv.MessagingOperationName(sendOperationName),
		semconv.MessagingOperationTypeSend,
	)
	if len(records) > 1 {
		attrs = append(attrs, semconv.MessagingBatchMessageCount(len(records)))
	}

	ctx, span := t.tracer.Start(
		ctx,
		sendOperationName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...),
	)

	return context.WithValue(ctx, spanKey{tracer: t, kind: producerSpan}, span)
}

// OnProduceRecord implements xkafka.HookProduceRecord.
func (t *Tracer) OnProduceRecord(ctx context.Context, record *kgo.Record) {
	if record == nil {
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	t.propagator.Inject(ctx, recordCarrier{record: record})
}

// OnProduceEnd implements xkafka.HookProduceEnd.
func (t *Tracer) OnProduceEnd(ctx context.Context, records []*kgo.Record, _ time.Duration, err error) {
	span := t.spanFromContext(ctx, producerSpan)
	if span == nil {
		return
	}

	topic, _ := commonRecordDestination(records)
	if topic != "" {
		span.SetName(sendOperationName + " " + topic)
		span.SetAttributes(semconv.MessagingDestinationName(topic))
	}

	endSpan(span, err)
}

// OnHandleStart implements xkafka.HookHandleStart.
func (t *Tracer) OnHandleStart(ctx context.Context, records []*kgo.Record) context.Context {
	attrs := t.consumerAttributes(
		semconv.MessagingSystemKafka,
		semconv.MessagingOperationName(processOperationName),
		semconv.MessagingOperationTypeProcess,
	)

	if len(records) > 1 {
		attrs = append(attrs, semconv.MessagingBatchMessageCount(len(records)))
	}

	spanName := processOperationName

	topic, partition := commonRecordDestination(records)
	if topic != "" {
		spanName += " " + topic
		attrs = append(attrs, semconv.MessagingDestinationName(topic))

		if partition >= 0 {
			attrs = append(
				attrs,
				semconv.MessagingDestinationPartitionID(
					strconv.FormatInt(int64(partition), 10),
				),
			)
		}
	}

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	}
	if links := t.recordLinks(records); len(links) > 0 {
		opts = append(opts, trace.WithLinks(links...))
	}

	ctx, span := t.tracer.Start(ctx, spanName, opts...)

	return context.WithValue(ctx, spanKey{tracer: t, kind: handlerSpan}, span)
}

// OnHandleEnd implements xkafka.HookHandleEnd.
func (t *Tracer) OnHandleEnd(ctx context.Context, _ []*kgo.Record, _ time.Duration, err error) {
	span := t.spanFromContext(ctx, handlerSpan)
	if span == nil {
		return
	}

	endSpan(span, err)
}

// OnOffsetCommit implements xkafka.HookOffsetCommit.
func (t *Tracer) OnOffsetCommit(ctx context.Context, duration time.Duration, err error) {
	t.recordSettlementSpan(ctx, offsetCommitOperationName, duration, err)
}

// OnShareAckFlush implements xkafka.HookShareAckFlush.
func (t *Tracer) OnShareAckFlush(ctx context.Context, duration time.Duration, err error) {
	t.recordSettlementSpan(ctx, shareAckOperationName, duration, err)
}

// OnTransactionStart implements xkafka.HookTransactionStart.
func (t *Tracer) OnTransactionStart(ctx context.Context, transactionType xkafka.TransactionType) context.Context {
	attrs := t.transactionAttributes(transactionType)
	attrs = append(
		attrs,
		semconv.MessagingSystemKafka,
		transactionTypeKey.String(string(transactionType)),
	)

	ctx, span := t.tracer.Start(
		ctx,
		transactionSpanName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attrs...),
	)

	return context.WithValue(ctx, spanKey{tracer: t, kind: transactionSpan}, span)
}

// OnTransactionEnd implements xkafka.HookTransactionEnd.
func (t *Tracer) OnTransactionEnd(
	ctx context.Context,
	_ xkafka.TransactionType,
	outcome xkafka.TransactionOutcome,
	_ time.Duration,
	err error,
) {
	span := t.spanFromContext(ctx, transactionSpan)
	if span == nil {
		return
	}

	span.SetAttributes(
		transactionOutcomeKey.String(string(outcome)),
	)
	endSpan(span, err)
}

// recordSettlementSpan records a completed settlement operation using its duration.
func (t *Tracer) recordSettlementSpan(ctx context.Context, operationName string, duration time.Duration, err error) {
	endTime := time.Now()
	startTime := endTime.Add(-duration)

	attrs := t.consumerAttributes(
		semconv.MessagingSystemKafka,
		semconv.MessagingOperationName(operationName),
		semconv.MessagingOperationTypeSettle,
	)

	_, span := t.tracer.Start(
		ctx,
		operationName,
		trace.WithTimestamp(startTime),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
	endSpan(span, err, trace.WithTimestamp(endTime))
}

// recordLinks returns unique sampled message creation contexts for the records.
func (t *Tracer) recordLinks(records []*kgo.Record) []trace.Link {
	var (
		links    []trace.Link
		firstKey spanContextKey
		seen     map[spanContextKey]struct{}
	)

	for _, record := range records {
		if record == nil {
			continue
		}

		extracted := t.propagator.Extract(
			context.Background(),
			recordCarrier{record: record},
		)
		spanContext := trace.SpanContextFromContext(extracted)
		if !spanContext.IsValid() || !spanContext.IsSampled() {
			continue
		}

		key := spanContextKey{
			traceID: spanContext.TraceID(),
			spanID:  spanContext.SpanID(),
		}

		// Most batches contain many records from the same upstream span.
		// Keep the first context separately to avoid allocating a dedup map.
		if len(links) == 0 {
			firstKey = key
			links = append(links, trace.Link{SpanContext: spanContext})
			continue
		}

		if seen == nil {
			if key == firstKey {
				continue
			}

			// Allocate the map only after a second unique context appears.
			seen = make(map[spanContextKey]struct{})
			seen[firstKey] = struct{}{}
		} else if _, ok := seen[key]; ok {
			continue
		}

		seen[key] = struct{}{}
		links = append(links, trace.Link{SpanContext: spanContext})
	}

	return links
}

func (t *Tracer) spanFromContext(ctx context.Context, kind spanType) trace.Span {
	span, _ := ctx.Value(spanKey{tracer: t, kind: kind}).(trace.Span)
	return span
}

func (t *Tracer) attributes(extra ...attribute.KeyValue) []attribute.KeyValue {
	return append(t.clientAttrs, extra...)
}

func (t *Tracer) consumerAttributes(extra ...attribute.KeyValue) []attribute.KeyValue {
	return append(t.consumerAttrs, extra...)
}

func (t *Tracer) transactionAttributes(transactionType xkafka.TransactionType) []attribute.KeyValue {
	if transactionType == xkafka.TransactionTypeGroup {
		return t.consumerAttributes()
	}

	return t.attributes()
}

func endSpan(span trace.Span, err error, opts ...trace.SpanEndOption) {
	if err != nil {
		span.SetAttributes(errorTypeAttribute(err))
		span.SetStatus(codes.Error, err.Error())
	}

	span.End(opts...)
}
