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
	_ xkafka.HookNewClient               = new(Tracer)
	_ xkafka.HookNewGroupTransactSession = new(Tracer)
	_ xkafka.HookOffsetCommit            = new(Tracer)
	_ xkafka.HookHandleStart             = new(Tracer)
	_ xkafka.HookHandleEnd               = new(Tracer)
	_ xkafka.HookShareAckFlush           = new(Tracer)
	_ xkafka.HookTransactionStart        = new(Tracer)
	_ xkafka.HookTransactionEnd          = new(Tracer)
)

const transactionSpanName = "xkafka.transaction"

type spanKind uint8

const (
	handlerSpan spanKind = iota + 1
	transactionSpan
)

type spanKey struct {
	tracer *Tracer
	kind   spanKind
}

// Tracer traces xkafka handler, settlement, and transaction runtime operations.
//
// Tracer complements franz-go record tracing: franz-go hooks trace record
// publish and receive operations, while Tracer adds xkafka process, settlement,
// and transaction spans.
type Tracer struct {
	provider   trace.TracerProvider
	propagator propagation.TextMapPropagator
	tracer     trace.Tracer

	clientAttributes attribute.Set
	consumerGroup    string
	shareGroup       string
}

// TracerOpt configures Tracer.
type TracerOpt interface {
	apply(*Tracer)
}

type tracerOptFunc func(*Tracer)

func (o tracerOptFunc) apply(t *Tracer) {
	o(t)
}

// TracerProvider configures the OpenTelemetry TracerProvider used by Tracer.
//
// If none is specified, the global TracerProvider is used.
func TracerProvider(provider trace.TracerProvider) TracerOpt {
	return tracerOptFunc(func(t *Tracer) {
		if provider != nil {
			t.provider = provider
		}
	})
}

// TracerPropagator configures the OpenTelemetry TextMapPropagator used to
// extract message creation contexts from Kafka record headers.
//
// If none is specified, the global TextMapPropagator is used.
func TracerPropagator(propagator propagation.TextMapPropagator) TracerOpt {
	return tracerOptFunc(func(t *Tracer) {
		if propagator != nil {
			t.propagator = propagator
		}
	})
}

// NewTracer creates a Tracer for xkafka runtime operations.
func NewTracer(opts ...TracerOpt) *Tracer {
	t := &Tracer{}

	for _, opt := range opts {
		opt.apply(t)
	}

	if t.provider == nil {
		t.provider = otel.GetTracerProvider()
	}
	if t.propagator == nil {
		t.propagator = otel.GetTextMapPropagator()
	}

	t.tracer = t.provider.Tracer(
		instrumentationName,
		trace.WithInstrumentationVersion(semVersion()),
		trace.WithSchemaURL(semconv.SchemaURL),
	)
	t.clientAttributes = attribute.NewSet()

	return t
}

func (t *Tracer) clone() *Tracer {
	clone := *t
	clone.clientAttributes = attribute.NewSet()
	clone.consumerGroup = ""
	clone.shareGroup = ""
	return &clone
}

func (t *Tracer) setRuntime(
	name string,
	labels map[string]string,
	consumerGroup string,
	shareGroup string,
) {
	t.clientAttributes = newClientAttributes(name, labels)
	t.consumerGroup = consumerGroup
	t.shareGroup = shareGroup
}

func (t *Tracer) OnNewClient(client *xkafka.Client) {
	t.setRuntime(
		client.Name(),
		client.Labels(),
		client.ConsumerGroup(),
		client.ShareGroup(),
	)
}

func (t *Tracer) OnNewGroupTransactSession(session *xkafka.GroupTransactSession) {
	t.setRuntime(
		session.Name(),
		session.Labels(),
		session.ConsumerGroup(),
		"",
	)
}

func (t *Tracer) OnOffsetCommit(ctx context.Context, duration time.Duration, err error) {
	t.endSettlementSpan(ctx, offsetCommitOperationName, duration, err)
}

func (t *Tracer) OnHandleStart(ctx context.Context, records []*kgo.Record) context.Context {
	attrs := t.consumerAttributes(
		semconv.MessagingSystemKafka,
		semconv.MessagingOperationName(processOperationName),
		semconv.MessagingOperationTypeProcess,
		semconv.MessagingBatchMessageCount(len(records)),
	)

	spanName := processOperationName
	topic, partition, sameTopic, samePartition := commonRecordDestination(records)
	if sameTopic && topic != "" {
		spanName += " " + topic
		attrs = append(attrs, semconv.MessagingDestinationName(topic))

		if samePartition && partition >= 0 {
			attrs = append(
				attrs,
				semconv.MessagingDestinationPartitionID(strconv.FormatInt(int64(partition), 10)),
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

func (t *Tracer) OnHandleEnd(
	ctx context.Context,
	_ []*kgo.Record,
	_ time.Duration,
	err error,
) {
	span := t.spanFromContext(ctx, handlerSpan)
	if span == nil {
		return
	}

	endSpan(span, err)
}

func (t *Tracer) OnShareAckFlush(ctx context.Context, duration time.Duration, err error) {
	t.endSettlementSpan(ctx, shareAckOperationName, duration, err)
}

func (t *Tracer) OnTransactionStart(
	ctx context.Context,
	transactionType xkafka.TransactionType,
) context.Context {
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

// Settlement hooks run after the Kafka operation completes and provide its
// duration, so the span is recorded with explicit start and end timestamps.
func (t *Tracer) endSettlementSpan(
	ctx context.Context,
	operationName string,
	duration time.Duration,
	err error,
) {
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
	endSpanAt(span, err, endTime)
}

func (t *Tracer) recordLinks(records []*kgo.Record) []trace.Link {
	links := make([]trace.Link, 0, len(records))

	for _, record := range records {
		if record == nil {
			continue
		}

		extracted := t.propagator.Extract(context.Background(), recordCarrier{record: record})
		spanContext := trace.SpanContextFromContext(extracted)
		if !spanContext.IsValid() && record.Context != nil {
			candidate := trace.SpanContextFromContext(record.Context)
			if candidate.IsValid() && candidate.IsRemote() {
				spanContext = candidate
			}
		}
		if !spanContext.IsValid() {
			continue
		}

		links = append(links, trace.Link{
			SpanContext: spanContext,
			Attributes:  recordLinkAttributes(record),
		})
	}

	return links
}

func (t *Tracer) spanFromContext(ctx context.Context, kind spanKind) trace.Span {
	span, _ := ctx.Value(spanKey{tracer: t, kind: kind}).(trace.Span)
	return span
}

func (t *Tracer) attributes(extra ...attribute.KeyValue) []attribute.KeyValue {
	attrs := t.clientAttributes.ToSlice()
	return append(attrs[:len(attrs):len(attrs)], extra...)
}

func (t *Tracer) consumerAttributes(extra ...attribute.KeyValue) []attribute.KeyValue {
	attrs := t.attributes(extra...)

	switch {
	case t.consumerGroup != "":
		attrs = append(attrs, semconv.MessagingConsumerGroupName(t.consumerGroup))
	case t.shareGroup != "":
		attrs = append(attrs, shareGroupKey.String(t.shareGroup))
	}

	return attrs
}

func (t *Tracer) transactionAttributes(transactionType xkafka.TransactionType) []attribute.KeyValue {
	if transactionType == xkafka.TransactionTypeGroup {
		return t.consumerAttributes()
	}

	return t.attributes()
}

func commonRecordDestination(records []*kgo.Record) (topic string, partition int32, sameTopic, samePartition bool) {
	if len(records) == 0 || records[0] == nil {
		return "", 0, false, false
	}

	topic = records[0].Topic
	partition = records[0].Partition
	sameTopic = true
	samePartition = true

	for _, record := range records[1:] {
		if record == nil {
			return topic, partition, false, false
		}

		if record.Topic != topic {
			sameTopic = false
			samePartition = false
		}
		if record.Partition != partition {
			samePartition = false
		}
	}

	return topic, partition, sameTopic, samePartition
}

func recordLinkAttributes(record *kgo.Record) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 3)

	if record.Topic != "" {
		attrs = append(attrs, semconv.MessagingDestinationName(record.Topic))
	}
	if record.Partition >= 0 {
		attrs = append(
			attrs,
			semconv.MessagingDestinationPartitionID(strconv.FormatInt(int64(record.Partition), 10)),
		)
	}
	if record.Offset >= 0 {
		attrs = append(attrs, semconv.MessagingKafkaOffsetKey.Int64(record.Offset))
	}

	return attrs
}

func endSpan(span trace.Span, err error) {
	if err != nil {
		span.SetAttributes(errorType(err))
		span.SetStatus(codes.Error, err.Error())
	}

	span.End()
}

func endSpanAt(span trace.Span, err error, endTime time.Time) {
	if err != nil {
		span.SetAttributes(errorType(err))
		span.SetStatus(codes.Error, err.Error())
	}

	span.End(trace.WithTimestamp(endTime))
}
