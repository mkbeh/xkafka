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

type fetchesHandler = func(ctx context.Context, fetches kgo.Fetches)

type Consumer struct {
	enabled bool
	id      string

	conn            *kgo.Client
	fmt             *kgo.RecordFormatter
	logger          *slog.Logger
	handleFetches   fetchesHandler
	batchSize       int
	groupSpecified  bool
	skipFatalErrors bool
	exitCh          chan struct{}

	pollInterval             time.Duration
	suspendProcessingTimeout time.Duration
	suspendCommittingTimeout time.Duration

	clientOptions []kgo.Opt
	meterOptions  []kotel.MeterOpt
	tracerOptions []kotel.TracerOpt

	pollTicker *time.Ticker
	metrics    *kprom.Metrics
	namespace  string
	labels     map[string]string
}

func NewConsumer(opts ...ConsumerOption) (*Consumer, error) {
	c := &Consumer{
		batchSize:                100,
		skipFatalErrors:          true,
		exitCh:                   make(chan struct{}),
		pollInterval:             time.Millisecond * 300,
		suspendProcessingTimeout: time.Second * 30,
		suspendCommittingTimeout: time.Second * 10,
	}

	c.addMeterOption(kotel.MeterProvider(otel.GetMeterProvider()))
	c.addTracerOption(kotel.TracerProvider(otel.GetTracerProvider()))

	for _, opt := range opts {
		opt.apply(c)
	}

	if c.handleFetches == nil {
		return nil, fmt.Errorf("fetches handler must be set")
	}

	formatter, err := newFormatter()
	if err != nil {
		return nil, err
	}

	c.fmt = formatter

	if c.logger == nil {
		c.logger = slog.Default()
	}
	c.logger.With(kslog.Component("kafka_consumer"))

	instrumenting := kotel.NewKotel(
		kotel.WithMeter(kotel.NewMeter(c.meterOptions...)),
	)

	c.exposeMetrics()
	c.metrics = kprom.NewMetrics(c.namespace, "kafka", c.labels)

	c.clientOptions = append(c.clientOptions,
		kgo.WithLogger(kslog.NewKgoAdapter(c.logger)),
		kgo.WithHooks(instrumenting.Hooks(), c.metrics),
		kgo.KeepRetryableFetchErrors(),
	)

	return c, nil
}

func (c *Consumer) PreRun(ctx context.Context) error {
	if !c.enabled {
		return nil
	}

	conn, err := kgo.NewClient(c.clientOptions...)
	if err != nil {
		return err
	}

	if err := conn.Ping(ctx); err != nil {
		return err
	}

	c.conn = conn
	c.pollTicker = time.NewTicker(c.pollInterval)

	return nil
}

func (c *Consumer) Shutdown(_ context.Context) error {
	close(c.exitCh)

	if c.pollTicker != nil {
		c.pollTicker.Stop()
	}

	if c.conn != nil {
		c.conn.Close()
	}

	return nil
}

func (c *Consumer) Run(ctx context.Context) error {
	if !c.enabled {
		return nil
	}

	for range c.pollTicker.C {
		select {
		case <-c.exitCh:
			return nil
		default:
		}

		fetches := c.conn.PollRecords(ctx, c.batchSize)
		if fetches.IsClientClosed() {
			c.logger.InfoContext(ctx, "kafka client closed for topic(s)", kslog.ConsumerLabels(c.labels))
			return nil
		}

		for _, fetchErr := range fetches.Errors() {
			c.logger.ErrorContext(ctx, "error fetching records", kslog.Error(fetchErr.Err))
			c.metrics.CollectHandleErrors(fetchErr.Topic)

			if !kerr.IsRetriable(fetchErr.Err) && !c.skipFatalErrors {
				return fetchErr.Err
			}
		}

		c.handleFetches(ctx, fetches)
	}

	return nil
}

func (c *Consumer) handleFetchesBatch(handler BatchHandlerFunc) fetchesHandler {
	return func(ctx context.Context, fetches kgo.Fetches) {
		records := fetches.Records()
		if len(records) == 0 {
			return
		}

		defer func(startTime time.Time) {
			for _, r := range records {
				c.metrics.CollectHandleProcessTiming(startTime, r.Topic)
			}
		}(time.Now())

	infiniteLoop:
		for {
			select {
			case <-c.exitCh:
				return
			default:
				if err := handler(ctx, records); err != nil {
					c.metrics.CollectHandleErrors("")
					c.logger.ErrorContext(ctx, "error handling records",
						kslog.Error(err),
						kslog.Records(c.formatRecords(records...)),
					)
					time.Sleep(c.suspendProcessingTimeout)
				} else {
					c.commitInternalOffsetsEternal(ctx)
					break infiniteLoop
				}
			}
		}
	}
}

func (c *Consumer) handleFetchesEach(handler HandlerFunc) fetchesHandler {
	handleUntilSuccessful := func(ctx context.Context, record *kgo.Record) {
		defer func(startTime time.Time) {
			c.metrics.CollectHandleProcessTiming(startTime, record.Topic)
		}(time.Now())

	infiniteLoop:
		for {
			select {
			case <-c.exitCh:
				return
			default:
				if err := handler(ctx, record); err != nil {
					c.metrics.CollectHandleErrors(record.Topic)
					c.logger.ErrorContext(ctx, "error handling record",
						kslog.Error(err),
						kslog.Records(c.formatRecords(record)),
					)
					time.Sleep(c.suspendProcessingTimeout)
				} else {
					c.commitRecordOffsetEternal(ctx, record)
					break infiniteLoop
				}
			}
		}
	}

	return func(ctx context.Context, fetches kgo.Fetches) {
		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			select {
			case <-c.exitCh:
				return
			default:
				handleUntilSuccessful(ctx, record)
			}
		}
	}
}

func (c *Consumer) commitInternalOffsetsEternal(ctx context.Context) {
	if !c.groupSpecified {
		return
	}

infiniteLoop:
	for {
		select {
		case <-c.exitCh:
			return
		default:
			if err := c.conn.CommitUncommittedOffsets(ctx); err != nil {
				c.metrics.CollectHandleErrors("")
				c.logger.ErrorContext(ctx, "error committing offsets", kslog.Error(err))
				time.Sleep(c.suspendCommittingTimeout)
			} else {
				c.logger.DebugContext(ctx, "offsets committed", kslog.Count(len(c.conn.CommittedOffsets())))
				break infiniteLoop
			}
		}
	}
}

func (c *Consumer) commitRecordOffsetEternal(ctx context.Context, record *kgo.Record) {
	if !c.groupSpecified {
		return
	}

infiniteLoop:
	for {
		select {
		case <-c.exitCh:
			return
		default:
			if err := c.conn.CommitRecords(ctx, record); err != nil {
				c.metrics.CollectHandleErrors(record.Topic)
				c.logger.ErrorContext(ctx, "error committing records", kslog.Error(err))
				time.Sleep(c.suspendCommittingTimeout)
			} else {
				c.logger.DebugContext(ctx, "record processed", kslog.Record(c.formatRecords(record)))
				break infiniteLoop
			}
		}
	}
}

func (c *Consumer) formatRecords(records ...*kgo.Record) []byte {
	buff := make([]byte, 0)

	for _, record := range records {
		buff = c.fmt.AppendRecord(buff, record)
	}

	return buff
}

func (c *Consumer) getClientID() string {
	if c.id == "" {
		return uuid.New().String()
	}
	return c.id
}

func (c *Consumer) addClientOption(opt kgo.Opt) {
	c.clientOptions = append(c.clientOptions, opt)
}

func (c *Consumer) addMeterOption(opt kotel.MeterOpt) {
	c.meterOptions = append(c.meterOptions, opt)
}

func (c *Consumer) addTracerOption(opt kotel.TracerOpt) {
	c.tracerOptions = append(c.tracerOptions, opt)
}

func (c *Consumer) setMetricLabel(k, v string) {
	if c.labels == nil {
		c.labels = make(map[string]string)
	}
	c.labels[k] = v
}

func (c *Consumer) exposeMetrics() {
	if c.labels == nil {
		c.labels = make(map[string]string)
	}

	c.labels["client_id"] = c.getClientID()
	c.labels["client_kind"] = "consumer"
}
