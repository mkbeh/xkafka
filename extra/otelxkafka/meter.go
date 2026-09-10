package otelxkafka

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/mkbeh/xkafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/semconv/v1.43.0/messagingconv"
)

var (
	_ xkafka.HookProduceError   = new(Meter)
	_ xkafka.HookFetchError     = new(Meter)
	_ xkafka.HookHandleEnd      = new(Meter)
	_ xkafka.HookOffsetCommit   = new(Meter)
	_ xkafka.HookShareAck       = new(Meter)
	_ xkafka.HookShareAckFlush  = new(Meter)
	_ xkafka.HookTransactionEnd = new(Meter)
)

const (
	produceErrorsMetricName       = "xkafka.produce.errors"
	fetchErrorsMetricName         = "xkafka.fetch.errors"
	handleRecordsMetricName       = "xkafka.handler.records"
	shareAckRecordsMetricName     = "xkafka.share.ack.records"
	transactionDurationMetricName = "xkafka.transaction.duration"
)

// durationBuckets are histogram boundaries in seconds.
var durationBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Meter exports xkafka runtime metrics through OpenTelemetry hooks.
type Meter struct {
	provider    metric.MeterProvider
	meter       metric.Meter
	instruments instruments

	clientAttrs   []attribute.KeyValue
	consumerAttrs []attribute.KeyValue
}

type meterConfig struct {
	provider metric.MeterProvider
	client   clientConfig
}

type instruments struct {
	// Producer.
	produceErrors metric.Int64Counter

	// Consumer.
	fetchErrors             metric.Int64Counter
	clientOperationDuration messagingconv.ClientOperationDuration

	// Handler.
	processDuration messagingconv.ProcessDuration
	handleRecords   metric.Int64Counter

	// Share Group.
	shareAckRecords metric.Int64Counter

	// Transactions.
	transactionDuration metric.Float64Histogram
}

// MeterOpt configures Meter.
type MeterOpt interface {
	applyMeter(*meterConfig)
}

type meterOptFunc func(*meterConfig)

func (o meterOptFunc) applyMeter(cfg *meterConfig) {
	o(cfg)
}

// MeterProvider configures the OpenTelemetry MeterProvider used by Meter.
//
// If none is specified, the global MeterProvider is used.
func MeterProvider(provider metric.MeterProvider) MeterOpt {
	return meterOptFunc(func(cfg *meterConfig) {
		if provider != nil {
			cfg.provider = provider
		}
	})
}

// NewMeter creates a Meter for xkafka runtime metrics.
func NewMeter(opts ...MeterOpt) *Meter {
	cfg := meterConfig{}

	for _, opt := range opts {
		opt.applyMeter(&cfg)
	}

	if cfg.provider == nil {
		cfg.provider = otel.GetMeterProvider()
	}

	m := &Meter{provider: cfg.provider}
	m.meter = m.provider.Meter(
		ScopeName,
		metric.WithInstrumentationVersion(Version()),
		metric.WithSchemaURL(semconv.SchemaURL),
	)
	m.instruments = m.newInstruments()
	m.clientAttrs, m.consumerAttrs = newAttributeSets(
		cfg.client.clientID,
		cfg.client.consumerGroup,
		cfg.client.shareGroup,
		cfg.client.labels,
	)

	return m
}

// OnProduceError implements xkafka.HookProduceError.
func (m *Meter) OnProduceError(record *kgo.Record, err error) {
	attrs := m.attributes()

	if record != nil && record.Topic != "" {
		attrs = append(attrs, semconv.MessagingDestinationName(record.Topic))
	}

	attrs = append(attrs, errorTypeAttribute(err))

	ctx := context.Background()
	if record != nil && record.Context != nil {
		ctx = record.Context
	}

	m.instruments.produceErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// OnFetchError implements xkafka.HookFetchError.
func (m *Meter) OnFetchError(ctx context.Context, topic string, partition int32, recoverable bool, err error) {
	attrs := m.consumerAttributes(
		fetchErrorRecoverableKey.Bool(recoverable),
	)
	if topic != "" {
		attrs = append(attrs, semconv.MessagingDestinationName(topic))
	}
	if partition >= 0 {
		attrs = append(
			attrs,
			semconv.MessagingDestinationPartitionID(strconv.FormatInt(int64(partition), 10)),
		)
	}

	attrs = append(attrs, errorTypeAttribute(err))

	m.instruments.fetchErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// OnHandleEnd implements xkafka.HookHandleEnd.
func (m *Meter) OnHandleEnd(ctx context.Context, records []*kgo.Record, duration time.Duration, err error) {
	processEnabled := m.instruments.processDuration.Enabled(ctx)
	handleEnabled := err == nil && m.instruments.handleRecords.Enabled(ctx)

	if !processEnabled && !handleEnabled {
		return
	}

	// Derived attribute slices are capped before appending so baseAttrs remains reusable.
	baseAttrs := m.consumerAttributes()

	if processEnabled {
		attrs := baseAttrs[:len(baseAttrs):len(baseAttrs)]

		topic, _ := commonRecordDestination(records)
		if topic != "" {
			attrs = append(attrs, semconv.MessagingDestinationName(topic))
		}

		if err != nil {
			attrs = append(attrs, errorTypeAttribute(err))
		}

		m.instruments.processDuration.Record(
			ctx,
			duration.Seconds(),
			processOperationName,
			messagingconv.SystemKafka,
			attrs...,
		)
	}

	if !handleEnabled {
		return
	}

	for topic, count := range countRecordsByTopic(records) {
		attrs := baseAttrs
		if topic != "" {
			attrs = append(
				attrs[:len(attrs):len(attrs)],
				semconv.MessagingDestinationName(topic),
			)
		}

		m.instruments.handleRecords.Add(ctx, count, metric.WithAttributes(attrs...))
	}
}

// OnOffsetCommit implements xkafka.HookOffsetCommit.
func (m *Meter) OnOffsetCommit(ctx context.Context, duration time.Duration, err error) {
	attrs := m.consumerAttributes(
		m.instruments.clientOperationDuration.AttrOperationType(messagingconv.OperationTypeSettle),
	)
	if err != nil {
		attrs = append(attrs, errorTypeAttribute(err))
	}

	m.instruments.clientOperationDuration.Record(
		ctx,
		duration.Seconds(),
		offsetCommitOperationName,
		messagingconv.SystemKafka,
		attrs...,
	)
}

// OnShareAck implements xkafka.HookShareAck.
func (m *Meter) OnShareAck(ctx context.Context, outcome xkafka.ShareAckOutcome, recordCount int) {
	attrs := m.consumerAttributes(
		shareAckOutcomeKey.String(string(outcome)),
	)

	m.instruments.shareAckRecords.Add(
		ctx,
		int64(recordCount),
		metric.WithAttributes(attrs...),
	)
}

// OnShareAckFlush implements xkafka.HookShareAckFlush.
func (m *Meter) OnShareAckFlush(ctx context.Context, duration time.Duration, err error) {
	attrs := m.consumerAttributes(
		m.instruments.clientOperationDuration.AttrOperationType(messagingconv.OperationTypeSettle),
	)
	if err != nil {
		attrs = append(attrs, errorTypeAttribute(err))
	}

	m.instruments.clientOperationDuration.Record(
		ctx,
		duration.Seconds(),
		shareAckOperationName,
		messagingconv.SystemKafka,
		attrs...,
	)
}

// OnTransactionEnd implements xkafka.HookTransactionEnd.
func (m *Meter) OnTransactionEnd(
	ctx context.Context,
	transactionType xkafka.TransactionType,
	outcome xkafka.TransactionOutcome,
	duration time.Duration,
	err error,
) {
	attrs := m.transactionAttributes(transactionType)
	attrs = append(
		attrs,
		transactionTypeKey.String(string(transactionType)),
		transactionOutcomeKey.String(string(outcome)),
	)
	if err != nil {
		attrs = append(attrs, errorTypeAttribute(err))
	}

	m.instruments.transactionDuration.Record(
		ctx,
		duration.Seconds(),
		metric.WithAttributes(attrs...),
	)
}

func (m *Meter) newInstruments() instruments {
	produceErrors, err := m.meter.Int64Counter(
		produceErrorsMetricName,
		metric.WithDescription("The number of records that failed to produce."),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		log.Printf("failed to create produce errors instrument, %v", err)
	}

	fetchErrors, err := m.meter.Int64Counter(
		fetchErrorsMetricName,
		metric.WithDescription("The number of Kafka fetch errors."),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		log.Printf("failed to create fetch errors instrument, %v", err)
	}

	clientOperationDuration, err := messagingconv.NewClientOperationDuration(
		m.meter,
		metric.WithExplicitBucketBoundaries(durationBuckets...),
	)
	if err != nil {
		log.Printf("failed to create client operation duration instrument, %v", err)
	}

	processDuration, err := messagingconv.NewProcessDuration(
		m.meter,
		metric.WithExplicitBucketBoundaries(durationBuckets...),
	)
	if err != nil {
		log.Printf("failed to create process duration instrument, %v", err)
	}

	handleRecords, err := m.meter.Int64Counter(
		handleRecordsMetricName,
		metric.WithDescription("The number of records successfully processed by the handler."),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		log.Printf("failed to create handler records instrument, %v", err)
	}

	shareAckRecords, err := m.meter.Int64Counter(
		shareAckRecordsMetricName,
		metric.WithDescription("The number of Share Group records by acknowledgement outcome."),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		log.Printf("failed to create share ack records instrument, %v", err)
	}

	transactionDuration, err := m.meter.Float64Histogram(
		transactionDurationMetricName,
		metric.WithDescription("The duration of xkafka transaction attempts."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(durationBuckets...),
	)
	if err != nil {
		log.Printf("failed to create transaction duration instrument, %v", err)
	}

	return instruments{
		produceErrors:           produceErrors,
		fetchErrors:             fetchErrors,
		clientOperationDuration: clientOperationDuration,
		processDuration:         processDuration,
		handleRecords:           handleRecords,
		shareAckRecords:         shareAckRecords,
		transactionDuration:     transactionDuration,
	}
}

func (m *Meter) attributes(extra ...attribute.KeyValue) []attribute.KeyValue {
	return append(m.clientAttrs, extra...)
}

func (m *Meter) consumerAttributes(extra ...attribute.KeyValue) []attribute.KeyValue {
	return append(m.consumerAttrs, extra...)
}

func (m *Meter) transactionAttributes(transactionType xkafka.TransactionType) []attribute.KeyValue {
	if transactionType == xkafka.TransactionTypeGroup {
		return m.consumerAttributes()
	}

	return m.attributes()
}

func commonRecordDestination(records []*kgo.Record) (topic string, partition int32) {
	if len(records) == 0 || records[0] == nil {
		return "", -1
	}

	topic = records[0].Topic
	partition = records[0].Partition

	for _, record := range records[1:] {
		if record == nil || record.Topic != topic {
			return "", -1
		}

		if partition >= 0 && record.Partition != partition {
			partition = -1
		}
	}

	return topic, partition
}

func countRecordsByTopic(records []*kgo.Record) map[string]int64 {
	counts := make(map[string]int64)

	for _, record := range records {
		if record == nil {
			continue
		}

		counts[record.Topic]++
	}

	return counts
}
