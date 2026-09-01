package otelxkafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/mkbeh/xkafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/mkbeh/xkafka/extra/otelxkafka"

const (
	produceErrorMetricName             = "xkafka.client.produce.error"
	fetchErrorMetricName               = "xkafka.client.fetch.error"
	handleMetricName                   = "xkafka.client.handler.call"
	handleRecordMetricName             = "xkafka.client.handler.record"
	handleErrorMetricName              = "xkafka.client.handler.error"
	handleDurationMetricName           = "xkafka.client.handler.duration"
	offsetCommitErrorMetricName        = "xkafka.client.offset.commit.error"
	shareAckErrorMetricName            = "xkafka.client.share.ack.error"
	transactionCommitMetricName        = "xkafka.client.transaction.commit"
	transactionAbortMetricName         = "xkafka.client.transaction.abort"
	transactionErrorMetricName         = "xkafka.client.transaction.error"
	transactionDurationMetricName      = "xkafka.client.transaction.duration"
	groupTransactionCommitMetricName   = "xkafka.client.group.transaction.commit"
	groupTransactionAbortMetricName    = "xkafka.client.group.transaction.abort"
	groupTransactionErrorMetricName    = "xkafka.client.group.transaction.error"
	groupTransactionDurationMetricName = "xkafka.client.group.transaction.duration"
)

const clientNameAttribute = "xkafka.client.name"

// Metrics exports xkafka client statistics through OpenTelemetry.
//
// Metrics is safe for concurrent use and reuse across multiple clients and
// group transaction sessions.
type Metrics struct {
	meter       metric.Meter
	instruments clientMetricInstruments
}

type clientMetricInstruments struct {
	produceError             metric.Int64ObservableCounter
	fetchError               metric.Int64ObservableCounter
	handle                   metric.Int64ObservableCounter
	handleRecord             metric.Int64ObservableCounter
	handleError              metric.Int64ObservableCounter
	handleDuration           metric.Float64ObservableCounter
	offsetCommitError        metric.Int64ObservableCounter
	shareAckError            metric.Int64ObservableCounter
	transactionCommit        metric.Int64ObservableCounter
	transactionAbort         metric.Int64ObservableCounter
	transactionError         metric.Int64ObservableCounter
	transactionDuration      metric.Float64ObservableCounter
	groupTransactionCommit   metric.Int64ObservableCounter
	groupTransactionAbort    metric.Int64ObservableCounter
	groupTransactionError    metric.Int64ObservableCounter
	groupTransactionDuration metric.Float64ObservableCounter
}

type registration struct {
	registration metric.Registration
}

func (r *registration) Close() {
	if r == nil || r.registration == nil {
		return
	}

	if err := r.registration.Unregister(); err != nil {
		otel.Handle(fmt.Errorf("otelxkafka: unregister client metrics: %w", err))
	}
}

// Register registers OpenTelemetry metrics for one metrics source.
//
// The source must have a stable name. Source labels are exported as metric
// attributes and must remain stable and low-cardinality.
func (m *Metrics) Register(source xkafka.MetricsSource) (xkafka.MetricsRegistration, error) {
	if m == nil {
		return nil, errors.New("otelxkafka: metrics is nil")
	}
	if source == nil {
		return nil, errors.New("otelxkafka: metrics source is nil")
	}
	if source.Name() == "" {
		return nil, errors.New("otelxkafka: client name must not be empty")
	}

	attributes := newClientMetricAttributes(source.Name(), source.Labels())
	option := metric.WithAttributeSet(attributes)

	registered, err := m.meter.RegisterCallback(
		func(_ context.Context, observer metric.Observer) error {
			m.instruments.observe(observer, source.Stats(), option)
			return nil
		},
		m.instruments.produceError,
		m.instruments.fetchError,
		m.instruments.handle,
		m.instruments.handleRecord,
		m.instruments.handleError,
		m.instruments.handleDuration,
		m.instruments.offsetCommitError,
		m.instruments.shareAckError,
		m.instruments.transactionCommit,
		m.instruments.transactionAbort,
		m.instruments.transactionError,
		m.instruments.transactionDuration,
		m.instruments.groupTransactionCommit,
		m.instruments.groupTransactionAbort,
		m.instruments.groupTransactionError,
		m.instruments.groupTransactionDuration,
	)
	if err != nil {
		return nil, fmt.Errorf("otelxkafka: register client metrics callback: %w", err)
	}

	return &registration{registration: registered}, nil
}

func newClientMetricInstruments(meter metric.Meter) (clientMetricInstruments, error) {
	var instruments clientMetricInstruments
	var err error

	instruments.produceError, err = newInt64Counter(
		meter,
		produceErrorMetricName,
		"The cumulative number of records that failed to produce.",
		"{error}",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.fetchError, err = newInt64Counter(
		meter,
		fetchErrorMetricName,
		"The cumulative number of Kafka fetch errors.",
		"{error}",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.handle, err = newInt64Counter(
		meter,
		handleMetricName,
		"The cumulative number of handler calls, including retries.",
		"{call}",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.handleRecord, err = newInt64Counter(
		meter,
		handleRecordMetricName,
		"The cumulative number of records passed to handlers, including retries.",
		"{record}",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.handleError, err = newInt64Counter(
		meter,
		handleErrorMetricName,
		"The cumulative number of handler calls that returned an error or panicked.",
		"{error}",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.handleDuration, err = newFloat64Counter(
		meter,
		handleDurationMetricName,
		"The cumulative time spent executing handlers.",
		"s",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.offsetCommitError, err = newInt64Counter(
		meter,
		offsetCommitErrorMetricName,
		"The cumulative number of failed consumer offset commit attempts.",
		"{error}",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.shareAckError, err = newInt64Counter(
		meter,
		shareAckErrorMetricName,
		"The cumulative number of failed Share Group acknowledgment flush attempts.",
		"{error}",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.transactionCommit, err = newInt64Counter(
		meter,
		transactionCommitMetricName,
		"The cumulative number of successfully committed RunInTx transactions.",
		"{transaction}",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.transactionAbort, err = newInt64Counter(
		meter,
		transactionAbortMetricName,
		"The cumulative number of cleanly aborted RunInTx transactions.",
		"{transaction}",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.transactionError, err = newInt64Counter(
		meter,
		transactionErrorMetricName,
		"The cumulative number of RunInTx calls that could not complete cleanly.",
		"{error}",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.transactionDuration, err = newFloat64Counter(
		meter,
		transactionDurationMetricName,
		"The cumulative duration of RunInTx calls.",
		"s",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.groupTransactionCommit, err = newInt64Counter(
		meter,
		groupTransactionCommitMetricName,
		"The cumulative number of committed group transactions.",
		"{transaction}",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.groupTransactionAbort, err = newInt64Counter(
		meter,
		groupTransactionAbortMetricName,
		"The cumulative number of cleanly aborted group transactions.",
		"{transaction}",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.groupTransactionError, err = newInt64Counter(
		meter,
		groupTransactionErrorMetricName,
		"The cumulative number of failed group transaction operations.",
		"{error}",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	instruments.groupTransactionDuration, err = newFloat64Counter(
		meter,
		groupTransactionDurationMetricName,
		"The cumulative duration of group transactions.",
		"s",
	)
	if err != nil {
		return clientMetricInstruments{}, err
	}

	return instruments, nil
}

func newInt64Counter(
	meter metric.Meter,
	name,
	description,
	unit string,
) (metric.Int64ObservableCounter, error) {
	instrument, err := meter.Int64ObservableCounter(
		name,
		metric.WithDescription(description),
		metric.WithUnit(unit),
	)
	if err != nil {
		return nil, fmt.Errorf("otelxkafka: create %s: %w", name, err)
	}

	return instrument, nil
}

func newFloat64Counter(
	meter metric.Meter,
	name,
	description,
	unit string,
) (metric.Float64ObservableCounter, error) {
	instrument, err := meter.Float64ObservableCounter(
		name,
		metric.WithDescription(description),
		metric.WithUnit(unit),
	)
	if err != nil {
		return nil, fmt.Errorf("otelxkafka: create %s: %w", name, err)
	}

	return instrument, nil
}

func newClientMetricAttributes(name string, labels map[string]string) attribute.Set {
	attributes := make([]attribute.KeyValue, 0, len(labels)+1)

	for key, value := range labels {
		if key == clientNameAttribute {
			continue
		}

		attributes = append(attributes, attribute.String(key, value))
	}

	attributes = append(attributes, attribute.String(clientNameAttribute, name))

	return attribute.NewSet(attributes...)
}

func (instruments *clientMetricInstruments) observe(
	observer metric.Observer,
	stats xkafka.Stats,
	option metric.ObserveOption,
) {
	observer.ObserveInt64(instruments.produceError, stats.ProduceErrorCount, option)
	observer.ObserveInt64(instruments.fetchError, stats.FetchErrorCount, option)
	observer.ObserveInt64(instruments.handle, stats.HandleCount, option)
	observer.ObserveInt64(instruments.handleRecord, stats.HandleRecordCount, option)
	observer.ObserveInt64(instruments.handleError, stats.HandleErrorCount, option)
	observer.ObserveFloat64(instruments.handleDuration, stats.HandleDuration.Seconds(), option)
	observer.ObserveInt64(instruments.offsetCommitError, stats.OffsetCommitErrorCount, option)
	observer.ObserveInt64(instruments.shareAckError, stats.ShareAckErrorCount, option)
	observer.ObserveInt64(instruments.transactionCommit, stats.TransactionCommitCount, option)
	observer.ObserveInt64(instruments.transactionAbort, stats.TransactionAbortCount, option)
	observer.ObserveInt64(instruments.transactionError, stats.TransactionErrorCount, option)
	observer.ObserveFloat64(instruments.transactionDuration, stats.TransactionDuration.Seconds(), option)
	observer.ObserveInt64(instruments.groupTransactionCommit, stats.GroupTransactionCommitCount, option)
	observer.ObserveInt64(instruments.groupTransactionAbort, stats.GroupTransactionAbortCount, option)
	observer.ObserveInt64(instruments.groupTransactionError, stats.GroupTransactionErrorCount, option)
	observer.ObserveFloat64(instruments.groupTransactionDuration, stats.GroupTransactionDuration.Seconds(), option)
}
