package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/mkbeh/xkafka/internal/pkg/kprom"
	"github.com/mkbeh/xkafka/internal/pkg/kslog"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
)

// GroupTransactSession consumes records in a Kafka consumer group and produces
// output records transactionally, committing output records and consumed offsets
// atomically.
type GroupTransactSession struct {
	enabled bool
	id      string

	session *kgo.GroupTransactSession
	fmt     *kgo.RecordFormatter
	logger  *slog.Logger

	handleFetches fetchesHandler
	promiseFunc   func(*kgo.Record, error)

	skipFatalErrors bool
	exitCh          chan struct{}

	batchSize                int
	pollInterval             time.Duration
	suspendProcessingTimeout time.Duration

	clientOptions []kgo.Opt
	meterOptions  []kotel.MeterOpt
	tracerOptions []kotel.TracerOpt

	pollTicker      *time.Ticker
	producerMetrics *kprom.ProducerMetrics
	consumerMetrics *kprom.ConsumerMetrics

	namespace string
	labels    map[string]string
}

// NewGroupTransactSession creates a consumer group transaction session for
// Kafka-to-Kafka exactly-once processing.
//
// The session consumes records from a group, lets the handler produce output
// records, and commits produced records together with consumed offsets in one
// Kafka transaction.
//
// If the handler returns an error or a rebalance happens before the transaction
// is committed, the transaction is aborted.
func NewGroupTransactSession(opts ...GroupTransactSessionOption) (*GroupTransactSession, error) {
	s := &GroupTransactSession{
		batchSize:                100,
		skipFatalErrors:          true,
		exitCh:                   make(chan struct{}),
		pollInterval:             time.Millisecond * 300,
		suspendProcessingTimeout: time.Second * 30,
	}

	s.addMeterOption(kotel.MeterProvider(otel.GetMeterProvider()))
	s.addTracerOption(kotel.TracerProvider(otel.GetTracerProvider()))

	for _, opt := range opts {
		opt.applyGroupTransactSession(s)
	}

	if s.handleFetches == nil {
		return nil, fmt.Errorf("group transact session handler must be set")
	}

	if s.promiseFunc == nil {
		s.promiseFunc = s.loggingPromise
	}

	formatter, err := newFormatter()
	if err != nil {
		return nil, err
	}

	s.fmt = formatter

	if s.logger == nil {
		s.logger = slog.Default()
	}

	s.logger = s.logger.With(kslog.Component("kafka_group_transact_session"))

	instrumenting := kotel.NewKotel(
		kotel.WithMeter(kotel.NewMeter(s.meterOptions...)),
		kotel.WithTracer(kotel.NewTracer(s.tracerOptions...)),
	)

	s.exposeMetrics()

	metrics := kprom.NewMetrics(s.namespace, "kafka", s.labels)
	s.producerMetrics = metrics.Producer()
	s.consumerMetrics = metrics.Consumer()

	s.clientOptions = append(s.clientOptions,
		kgo.WithLogger(kslog.NewKgoAdapter(s.logger)),
		kgo.WithHooks(instrumenting.Hooks(), metrics.Hooks()),
		kgo.KeepRetryableFetchErrors(),
	)

	return s, nil
}

func (s *GroupTransactSession) PreRun(ctx context.Context) error {
	if !s.enabled {
		return nil
	}

	session, err := kgo.NewGroupTransactSession(s.clientOptions...)
	if err != nil {
		return err
	}

	if err := session.Client().Ping(ctx); err != nil {
		session.Close()
		return fmt.Errorf("kafka: %w", err)
	}

	s.session = session
	s.pollTicker = time.NewTicker(s.pollInterval)

	return nil
}

func (s *GroupTransactSession) Shutdown(ctx context.Context) error {
	select {
	case <-s.exitCh:
	default:
		close(s.exitCh)
	}

	if s.pollTicker != nil {
		s.pollTicker.Stop()
	}

	if s.session == nil {
		return nil
	}

	s.session.Close()

	return nil
}

func (s *GroupTransactSession) Run(ctx context.Context) error {
	if !s.enabled {
		return nil
	}

	for range s.pollTicker.C {
		select {
		case <-s.exitCh:
			return nil
		default:
		}

		fetches := s.session.PollRecords(ctx, s.batchSize)
		if fetches.IsClientClosed() {
			s.logger.InfoContext(ctx, "kafka group transact session closed", kslog.ConsumerLabels(s.labels))
			return nil
		}

		for _, fetchErr := range fetches.Errors() {
			s.logger.ErrorContext(ctx, "error fetching records", kslog.Error(fetchErr.Err))
			s.consumerMetrics.CollectHandleErrors(fetchErr.Topic)

			if !kerr.IsRetriable(fetchErr.Err) && !s.skipFatalErrors {
				return fetchErr.Err
			}
		}

		s.handleFetches(ctx, fetches)
	}

	return nil
}

func (s *GroupTransactSession) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	s.session.Produce(ctx, record, promise)
}

func (s *GroupTransactSession) TryProduce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	s.session.TryProduce(ctx, record, promise)
}

func (s *GroupTransactSession) ProduceSync(ctx context.Context, records ...*kgo.Record) error {
	results := s.session.ProduceSync(ctx, records...)
	for _, r := range results {
		if r.Err != nil {
			s.logger.ErrorContext(ctx, "error produce message sync", kslog.Error(r.Err))
			s.producerMetrics.CollectProduceError(recordTopic(r.Record))
		}
	}

	return results.FirstErr()
}

func (s *GroupTransactSession) handleFetchesBatch(handler BatchGroupTransactSessionFunc) fetchesHandler {
	return func(ctx context.Context, fetches kgo.Fetches) {
		records := fetches.Records()
		if len(records) == 0 {
			return
		}

		startTime := time.Now()

		committed, handleErr, txErr := s.handleBatchInTransaction(ctx, records, handler)

		for _, record := range records {
			s.consumerMetrics.CollectHandleProcessTiming(startTime, record.Topic)
		}

		if txErr != nil {
			for _, record := range records {
				s.consumerMetrics.CollectHandleErrors(record.Topic)
			}

			s.logger.ErrorContext(ctx, "error handling group transaction",
				kslog.Error(txErr),
				kslog.Records(s.formatRecords(records...)),
			)

			time.Sleep(s.suspendProcessingTimeout)

			return
		}

		if handleErr != nil {
			for _, record := range records {
				s.consumerMetrics.CollectHandleErrors(record.Topic)
			}

			s.logger.ErrorContext(ctx, "error handling records in group transaction",
				kslog.Error(handleErr),
				kslog.Records(s.formatRecords(records...)),
			)

			time.Sleep(s.suspendProcessingTimeout)

			return
		}

		if !committed {
			s.logger.InfoContext(ctx, "group transaction aborted before commit",
				kslog.ConsumerLabels(s.labels),
			)

			return
		}

		s.logger.DebugContext(ctx, "group transaction committed",
			kslog.Count(len(records)),
		)
	}
}

func (s *GroupTransactSession) handleBatchInTransaction(
	ctx context.Context,
	records []*kgo.Record,
	handler BatchGroupTransactSessionFunc,
) (committed bool, handleErr, txErr error) {
	if err := s.session.Begin(); err != nil {
		return false, nil, fmt.Errorf("kafka: begin group transaction: %w", err)
	}

	handleErr = handler(ctx, records, s)

	endTry := kgo.TryCommit
	if handleErr != nil {
		endTry = kgo.TryAbort
	}

	committed, err := s.session.End(ctx, endTry)
	if err != nil {
		return false, handleErr, fmt.Errorf("kafka: end group transaction: %w", err)
	}

	return committed, handleErr, nil
}

func (s *GroupTransactSession) loggingPromise(record *kgo.Record, err error) {
	var ctx context.Context
	if record.Context == nil {
		ctx = context.Background()
	} else {
		ctx = record.Context
	}

	if err != nil {
		s.producerMetrics.CollectProduceError(recordTopic(record))
		s.logger.ErrorContext(ctx, "kafka async producer error",
			kslog.Error(err),
			kslog.Record(s.fmt.AppendRecord(nil, record)),
		)
	}
}

func (s *GroupTransactSession) formatRecords(records ...*kgo.Record) []byte {
	buff := make([]byte, 0)

	for _, record := range records {
		buff = s.fmt.AppendRecord(buff, record)
	}

	return buff
}

func (s *GroupTransactSession) getClientID() string {
	if s.id == "" {
		return uuid.New().String()
	}

	return s.id
}

func (s *GroupTransactSession) addClientOption(opt kgo.Opt) {
	s.clientOptions = append(s.clientOptions, opt)
}

func (s *GroupTransactSession) addMeterOption(opt kotel.MeterOpt) {
	s.meterOptions = append(s.meterOptions, opt)
}

func (s *GroupTransactSession) addTracerOption(opt kotel.TracerOpt) {
	s.tracerOptions = append(s.tracerOptions, opt)
}

func (s *GroupTransactSession) setMetricLabel(k, v string) {
	if s.labels == nil {
		s.labels = make(map[string]string)
	}

	s.labels[k] = v
}

func (s *GroupTransactSession) exposeMetrics() {
	if s.labels == nil {
		s.labels = make(map[string]string)
	}

	s.labels["client_id"] = s.getClientID()
	s.labels["client_kind"] = "group_transact_session"
}
