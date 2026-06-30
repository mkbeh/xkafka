package xkafka

import (
	"context"
	"errors"
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

// handleFetchesFunc adapts fetched Kafka records to a configured processing strategy.
type handleFetchesFunc func(ctx context.Context, fetches kgo.Fetches)

// conn is the minimal Kafka client interface shared by Client and GroupTransactSession.
type conn interface {
	Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error))
	TryProduce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error))
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults

	PollFetches(ctx context.Context) kgo.Fetches
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
}

var (
	_ conn = (*kgo.Client)(nil)
	_ conn = (*kgo.GroupTransactSession)(nil)
)

// client contains the shared runtime state used by Client and GroupTransactSession.
type client struct {
	conn conn

	fmt     *kgo.RecordFormatter
	logger  *slog.Logger
	metrics *kprom.Metrics

	enabled     bool
	promiseFunc PromiseFunc

	handleFetches       handleFetchesFunc
	clientHandleFetches func(*Client) handleFetchesFunc
	groupHandleFetches  func(*GroupTransactSession) handleFetchesFunc

	clientID        string
	groupSpecified  bool
	batchSize       int
	skipFatalErrors bool

	pollInterval             time.Duration
	suspendProcessingTimeout time.Duration
	suspendCommittingTimeout time.Duration

	shareRejectAfterDeliveries int32
	shareReleaseTimeout        time.Duration

	clientOps  []kgo.Opt
	meterOpts  []kotel.MeterOpt
	tracerOpts []kotel.TracerOpt

	namespace string
	labels    map[string]string

	exitCh chan struct{}
}

func newClient(opts ...Opt) (*client, error) {
	c := &client{
		logger: slog.Default(),

		enabled: true,

		batchSize:       100,
		skipFatalErrors: true,

		pollInterval:             time.Second * 1,
		suspendProcessingTimeout: time.Second * 30,
		suspendCommittingTimeout: time.Second * 10,

		labels: map[string]string{},
		exitCh: make(chan struct{}),
	}

	c.meterOpts = append(c.meterOpts, kotel.MeterProvider(otel.GetMeterProvider()))
	c.tracerOpts = append(c.tracerOpts, kotel.TracerProvider(otel.GetTracerProvider()))

	for _, opt := range opts {
		opt.apply(c)
	}

	c.applyClientID()

	formatter, err := newFormatter()
	if err != nil {
		return nil, fmt.Errorf("kafka: create record formatter: %w", err)
	}

	instrumenting := kotel.NewKotel(
		kotel.WithMeter(kotel.NewMeter(c.meterOpts...)),
		kotel.WithTracer(kotel.NewTracer(c.tracerOpts...)),
	)

	metrics := kprom.NewMetrics(c.namespace, "kafka", c.labels)

	c.fmt = formatter
	c.metrics = metrics
	c.logger = c.logger.With(kslog.Component("kafka_client"))

	c.clientOps = append(c.clientOps,
		kgo.WithLogger(kslog.NewKgoAdapter(c.logger)),
		kgo.WithHooks(instrumenting.Hooks(), metrics.Hooks()),
		kgo.KeepRetryableFetchErrors(),
	)

	return c, nil
}

func (c *client) Produce(ctx context.Context, record *kgo.Record, promise PromiseFunc) {
	c.conn.Produce(ctx, record, c.wrapPromise(promise))
}

func (c *client) TryProduce(ctx context.Context, record *kgo.Record, promise PromiseFunc) {
	c.conn.TryProduce(ctx, record, c.wrapPromise(promise))
}

func (c *client) ProduceSync(ctx context.Context, records ...*kgo.Record) error {
	results := c.conn.ProduceSync(ctx, records...)
	for _, r := range results {
		if r.Err != nil {
			c.logger.ErrorContext(ctx, "error produce message sync", kslog.Error(r.Err))
			c.metrics.Producer().CollectProduceError(recordTopic(r.Record))
		}
	}

	return results.FirstErr()
}

func (c *client) HandleFetches(ctx context.Context) error {
	if !c.enabled {
		return nil
	}

	if c.conn == nil {
		return errors.New("kafka: conn is nil")
	}

	if c.handleFetches == nil {
		return errors.New("kafka: fetches handler is nil")
	}

	pollTicker := time.NewTicker(c.pollInterval)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.exitCh:
			return nil
		case <-pollTicker.C:
		}

		fetches := c.conn.PollRecords(ctx, c.batchSize)
		if fetches.IsClientClosed() {
			c.logger.InfoContext(ctx, "kafka client closed for topic(s)", kslog.ConsumerLabels(c.labels))
			return nil
		}

		for _, fetchErr := range fetches.Errors() {
			c.logger.ErrorContext(ctx, "error fetching records", kslog.Error(fetchErr.Err))
			c.metrics.Consumer().CollectHandleErrors(fetchErr.Topic)

			if !kerr.IsRetriable(fetchErr.Err) && !c.skipFatalErrors {
				return fetchErr.Err
			}
		}

		c.handleFetches(ctx, fetches)
	}
}

// Close stops the polling loop and is safe to call multiple times.
func (c *client) Close() {
	if c.exitCh != nil {
		select {
		case <-c.exitCh:
		default:
			close(c.exitCh)
		}
	}
}

func (c *client) wrapPromise(promise PromiseFunc) PromiseFunc {
	return func(record *kgo.Record, err error) {
		c.loggingPromise(record, err)

		if promise != nil {
			promise(record, err)
			return
		}

		if c.promiseFunc != nil {
			c.promiseFunc(record, err)
		}
	}
}

func (c *client) loggingPromise(record *kgo.Record, err error) {
	var ctx context.Context
	if record.Context == nil {
		ctx = context.Background()
	} else {
		ctx = record.Context
	}

	if err != nil {
		c.metrics.Producer().CollectProduceError(recordTopic(record))
		c.logger.ErrorContext(ctx, "kafka async producer error",
			kslog.Error(err),
			kslog.Record(c.fmt.AppendRecord(nil, record)),
		)
	}
}

func (c *client) formatRecords(records ...*kgo.Record) []byte {
	buff := make([]byte, 0)

	for _, record := range records {
		buff = c.fmt.AppendRecord(buff, record)
	}

	return buff
}

func (c *client) setMetricLabel(key, value string) {
	if c.labels == nil {
		c.labels = make(map[string]string)
	}

	c.labels[key] = value
}

func (c *client) applyClientID() {
	if c.clientID == "" {
		c.clientID = uuid.New().String()
	}

	c.clientOps = append(c.clientOps, kgo.ClientID(c.clientID))
	c.tracerOpts = append(c.tracerOpts, kotel.ClientID(c.clientID))
	c.setMetricLabel("client_id", c.clientID)
}

func newFormatter() (*kgo.RecordFormatter, error) {
	return kgo.NewRecordFormatter("topic: %t, key: %k, msg: %v")
}

func recordTopic(record *kgo.Record) string {
	if record == nil {
		return ""
	}

	return record.Topic
}
