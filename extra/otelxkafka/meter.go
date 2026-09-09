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
	_ xkafka.HookNewClient               = new(Meter)
	_ xkafka.HookNewGroupTransactSession = new(Meter)
	_ xkafka.HookProduceError            = new(Meter)
	_ xkafka.HookFetchError              = new(Meter)
	_ xkafka.HookOffsetCommit            = new(Meter)
	_ xkafka.HookHandleEnd               = new(Meter)
	_ xkafka.HookShareAck                = new(Meter)
	_ xkafka.HookShareAckFlush           = new(Meter)
	_ xkafka.HookTransactionEnd          = new(Meter)
)

const (
	produceErrorsMetricName       = "xkafka.produce.errors"
	fetchErrorsMetricName         = "xkafka.fetch.errors"
	handleRecordsMetricName       = "xkafka.handler.records"
	shareAckRecordsMetricName     = "xkafka.share.ack.records"
	transactionDurationMetricName = "xkafka.transaction.duration"
)

var (
	messagingDurationBuckets = []float64{
		0.005,
		0.01,
		0.025,
		0.05,
		0.075,
		0.1,
		0.25,
		0.5,
		0.75,
		1,
		2.5,
		5,
		7.5,
		10,
	}
	transactionDurationBuckets = []float64{
		0.005,
		0.01,
		0.025,
		0.05,
		0.075,
		0.1,
		0.25,
		0.5,
		0.75,
		1,
		2.5,
		5,
		7.5,
		10,
		15,
		30,
		60,
	}
)

// Meter exports xkafka runtime metrics through OpenTelemetry hooks.
type Meter struct {
	provider    metric.MeterProvider
	meter       metric.Meter
	instruments instruments

	clientAttributes attribute.Set
	consumerGroup    string
	shareGroup       string
}

// MeterOpt configures Meter.
type MeterOpt interface {
	apply(*Meter)
}

type meterOptFunc func(*Meter)

func (o meterOptFunc) apply(m *Meter) {
	o(m)
}

// MeterProvider configures the OpenTelemetry MeterProvider used by Meter.
//
// If none is specified, the global MeterProvider is used.
func MeterProvider(provider metric.MeterProvider) MeterOpt {
	return meterOptFunc(func(m *Meter) {
		if provider != nil {
			m.provider = provider
		}
	})
}

// NewMeter creates a Meter for xkafka runtime metrics.
func NewMeter(opts ...MeterOpt) *Meter {
	m := &Meter{}

	for _, opt := range opts {
		opt.apply(m)
	}

	if m.provider == nil {
		m.provider = otel.GetMeterProvider()
	}

	m.meter = m.provider.Meter(
		instrumentationName,
		metric.WithInstrumentationVersion(semVersion()),
		metric.WithSchemaURL(semconv.SchemaURL),
	)
	m.instruments = m.newInstruments()
	m.clientAttributes = attribute.NewSet()

	return m
}

func (m *Meter) clone() *Meter {
	clone := *m
	clone.clientAttributes = attribute.NewSet()
	clone.consumerGroup = ""
	clone.shareGroup = ""
	return &clone
}

func (m *Meter) setRuntime(
	name string,
	labels map[string]string,
	consumerGroup string,
	shareGroup string,
) {
	m.clientAttributes = newClientAttributes(name, labels)
	m.consumerGroup = consumerGroup
	m.shareGroup = shareGroup
}

func (m *Meter) OnNewClient(client *xkafka.Client) {
	m.setRuntime(
		client.Name(),
		client.Labels(),
		client.ConsumerGroup(),
		client.ShareGroup(),
	)
}

func (m *Meter) OnNewGroupTransactSession(session *xkafka.GroupTransactSession) {
	m.setRuntime(
		session.Name(),
		session.Labels(),
		session.ConsumerGroup(),
		"",
	)
}

func (m *Meter) OnProduceError(record *kgo.Record, err error) {
	attrs := appendRecordAttributes(m.attributes(), record)
	if err != nil {
		attrs = append(attrs, errorType(err))
	}

	ctx := context.Background()
	if record != nil && record.Context != nil {
		ctx = record.Context
	}

	m.instruments.produceErrors.Add(
		ctx,
		1,
		metric.WithAttributes(attrs...),
	)
}

func (m *Meter) OnFetchError(
	ctx context.Context,
	topic string,
	partition int32,
	recoverable bool,
	err error,
) {
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
	if err != nil {
		attrs = append(attrs, errorType(err))
	}

	m.instruments.fetchErrors.Add(
		ctx,
		1,
		metric.WithAttributes(attrs...),
	)
}

func (m *Meter) OnOffsetCommit(ctx context.Context, duration time.Duration, err error) {
	attrs := m.consumerAttributes(
		m.instruments.clientOperationDuration.AttrOperationType(messagingconv.OperationTypeSettle),
	)
	if err != nil {
		attrs = append(attrs, errorType(err))
	}

	m.instruments.clientOperationDuration.Record(
		ctx,
		duration.Seconds(),
		offsetCommitOperationName,
		messagingconv.SystemKafka,
		attrs...,
	)
}

func (m *Meter) OnHandleEnd(
	ctx context.Context,
	records []*kgo.Record,
	duration time.Duration,
	err error,
) {
	if m.instruments.processDuration.Enabled(ctx) {
		attrs := m.consumerAttributes()
		if err != nil {
			attrs = append(attrs, errorType(err))
		}

		m.instruments.processDuration.Record(
			ctx,
			duration.Seconds(),
			processOperationName,
			messagingconv.SystemKafka,
			attrs...,
		)
	}

	if err != nil || !m.instruments.handleRecords.Enabled(ctx) {
		return
	}

	for topic, count := range countRecordsByTopic(records) {
		attrs := m.consumerAttributes()
		if topic != "" {
			attrs = append(attrs, semconv.MessagingDestinationName(topic))
		}

		m.instruments.handleRecords.Add(
			ctx,
			count,
			metric.WithAttributes(attrs...),
		)
	}
}

func (m *Meter) OnShareAck(
	ctx context.Context,
	outcome xkafka.ShareAckOutcome,
	recordCount int,
) {
	attrs := m.consumerAttributes(
		shareAckOutcomeKey.String(string(outcome)),
	)

	m.instruments.shareAckRecords.Add(
		ctx,
		int64(recordCount),
		metric.WithAttributes(attrs...),
	)
}

func (m *Meter) OnShareAckFlush(ctx context.Context, duration time.Duration, err error) {
	attrs := m.consumerAttributes(
		m.instruments.clientOperationDuration.AttrOperationType(messagingconv.OperationTypeSettle),
	)
	if err != nil {
		attrs = append(attrs, errorType(err))
	}

	m.instruments.clientOperationDuration.Record(
		ctx,
		duration.Seconds(),
		shareAckOperationName,
		messagingconv.SystemKafka,
		attrs...,
	)
}

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
		attrs = append(attrs, errorType(err))
	}

	m.instruments.transactionDuration.Record(
		ctx,
		duration.Seconds(),
		metric.WithAttributes(attrs...),
	)
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

func (m *Meter) newInstruments() instruments {
	produceErrors, err := m.meter.Int64Counter(
		produceErrorsMetricName,
		metric.WithDescription("The number of records that failed to produce."),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		log.Printf("failed to create produceErrors instrument, %v", err)
	}

	fetchErrors, err := m.meter.Int64Counter(
		fetchErrorsMetricName,
		metric.WithDescription("The number of Kafka fetch errors."),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		log.Printf("failed to create fetchErrors instrument, %v", err)
	}

	clientOperationDuration, err := messagingconv.NewClientOperationDuration(
		m.meter,
		metric.WithExplicitBucketBoundaries(messagingDurationBuckets...),
	)
	if err != nil {
		log.Printf("failed to create clientOperationDuration instrument, %v", err)
	}

	processDuration, err := messagingconv.NewProcessDuration(
		m.meter,
		metric.WithExplicitBucketBoundaries(messagingDurationBuckets...),
	)
	if err != nil {
		log.Printf("failed to create processDuration instrument, %v", err)
	}

	handleRecords, err := m.meter.Int64Counter(
		handleRecordsMetricName,
		metric.WithDescription("The number of records successfully processed by the handler."),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		log.Printf("failed to create handleRecords instrument, %v", err)
	}

	shareAckRecords, err := m.meter.Int64Counter(
		shareAckRecordsMetricName,
		metric.WithDescription("The number of Share Group records by acknowledgement outcome."),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		log.Printf("failed to create shareAckRecords instrument, %v", err)
	}

	transactionDuration, err := m.meter.Float64Histogram(
		transactionDurationMetricName,
		metric.WithDescription("The duration of xkafka transaction attempts."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(transactionDurationBuckets...),
	)
	if err != nil {
		log.Printf("failed to create transactionDuration instrument, %v", err)
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
	attrs := m.clientAttributes.ToSlice()
	return append(attrs[:len(attrs):len(attrs)], extra...)
}

func (m *Meter) consumerAttributes(extra ...attribute.KeyValue) []attribute.KeyValue {
	attrs := m.attributes(extra...)

	switch {
	case m.consumerGroup != "":
		attrs = append(attrs, semconv.MessagingConsumerGroupName(m.consumerGroup))
	case m.shareGroup != "":
		attrs = append(attrs, shareGroupKey.String(m.shareGroup))
	}

	return attrs
}

func (m *Meter) transactionAttributes(transactionType xkafka.TransactionType) []attribute.KeyValue {
	if transactionType == xkafka.TransactionTypeGroup {
		return m.consumerAttributes()
	}

	return m.attributes()
}

func appendRecordAttributes(attrs []attribute.KeyValue, record *kgo.Record) []attribute.KeyValue {
	if record == nil {
		return attrs
	}

	if record.Topic != "" {
		attrs = append(attrs, semconv.MessagingDestinationName(record.Topic))
	}
	if record.Partition >= 0 {
		attrs = append(
			attrs,
			semconv.MessagingDestinationPartitionID(strconv.FormatInt(int64(record.Partition), 10)),
		)
	}

	return attrs
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
