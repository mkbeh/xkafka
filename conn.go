package xkafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// handleFetchesFunc adapts fetched Kafka records to a configured processing strategy.
type handleFetchesFunc func(ctx context.Context, fetches kgo.Fetches)

// conn is the minimal Kafka client interface shared by Client and GroupTransactSession.
type conn interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	AllowRebalance()
	Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error))
	TryProduce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error))
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults
}

var (
	_ conn = (*kgo.Client)(nil)
	_ conn = (*kgo.GroupTransactSession)(nil)
)

// client contains the shared runtime state used by Client and GroupTransactSession.
type client struct {
	conn conn

	fmt    *kgo.RecordFormatter
	logger kgo.Logger

	name    string
	labels  map[string]string
	metrics Metrics

	promiseFunc PromiseFunc

	handleFetches       handleFetchesFunc
	clientHandleFetches func(*Client) handleFetchesFunc
	groupHandleFetches  func(*GroupTransactSession) handleFetchesFunc

	consumerGroup   string
	shareGroup      string
	manualCommit    bool
	autoCommitMarks bool
	blockRebalance  bool
	maxPollRecords  int

	pollInterval             time.Duration
	suspendProcessingTimeout time.Duration
	suspendCommittingTimeout time.Duration

	shareRejectAfterDeliveries int32
	shareReleaseTimeout        time.Duration

	kafkaOpts []kgo.Opt

	stats statsCollector

	closeOnce sync.Once
	exitCh    chan struct{}
}

func newClient(opts ...Opt) (*client, error) {
	c := &client{
		logger: newDefaultLogger(),

		maxPollRecords: 100,

		pollInterval:             time.Second,
		suspendProcessingTimeout: time.Second * 30,
		suspendCommittingTimeout: time.Second * 10,

		labels: make(map[string]string),
		exitCh: make(chan struct{}),
	}

	for _, opt := range opts {
		opt.apply(c)
	}

	c.applyName()

	formatter, err := newFormatter()
	if err != nil {
		return nil, fmt.Errorf("kafka: create record formatter: %w", err)
	}
	c.fmt = formatter

	c.kafkaOpts = append(c.kafkaOpts, kgo.WithLogger(c.logger))

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
			c.stats.recordProduceError()
			c.logger.Log(kgo.LogLevelError, "error produce message sync", logKeyError, r.Err)
		}
	}

	return results.FirstErr()
}

func (c *client) HandleFetches(ctx context.Context) error {
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

		fetches := c.conn.PollRecords(ctx, c.maxPollRecords)
		if fetches.IsClientClosed() {
			c.logger.Log(kgo.LogLevelDebug, "kafka client closed for topic(s)", logKeyConsumerGroup, c.consumerGroup)
			return nil
		}

		if err := c.processFetches(ctx, fetches); err != nil {
			return err
		}
	}
}

func (c *client) processFetches(ctx context.Context, fetches kgo.Fetches) error {
	if c.blockRebalance {
		defer c.conn.AllowRebalance()
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := c.handleFetchErrors(fetches); err != nil {
		return err
	}

	c.handleFetches(ctx, fetches)

	return nil
}

func (c *client) handleFetchErrors(fetches kgo.Fetches) error {
	var firstErr error
	var firstTopic string
	var firstPartition int32

	fetches.EachError(func(topic string, partition int32, err error) {
		c.stats.recordFetchError()

		if isRecoverableFetchError(err) {
			c.logger.Log(kgo.LogLevelWarn, "recoverable error fetching records",
				logKeyError, err,
				logKeyTopic, topic,
			)
			return
		}

		c.logger.Log(kgo.LogLevelError, "error fetching records",
			logKeyError, err,
			logKeyTopic, topic,
		)

		if firstErr == nil {
			firstErr = err
			firstTopic = topic
			firstPartition = partition
		}
	})

	if firstErr == nil {
		return nil
	}

	return fmt.Errorf(
		"kafka: fetch topic %q partition %d: %w",
		firstTopic,
		firstPartition,
		firstErr,
	)
}

func isRecoverableFetchError(err error) bool {
	if kerr.IsRetriable(err) {
		return true
	}

	if _, ok := errors.AsType[*kgo.ErrDataLoss](err); ok {
		return true
	}

	if _, ok := errors.AsType[*kgo.ErrGroupSession](err); ok {
		return true
	}

	return false
}

func (c *client) wait(ctx context.Context, delay time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-c.exitCh:
		return false
	default:
	}

	if delay <= 0 {
		return true
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-c.exitCh:
		return false
	case <-timer.C:
		return true
	}
}

func (c *client) applyKafkaOptions(conn *kgo.Client) {
	c.consumerGroup, _ = conn.OptValue(kgo.ConsumerGroup).(string)
	c.shareGroup, _ = conn.OptValue(kgo.ShareGroup).(string)

	if c.consumerGroup != "" {
		c.manualCommit, _ = conn.OptValue(kgo.DisableAutoCommit).(bool)
		c.autoCommitMarks, _ = conn.OptValue(kgo.AutoCommitMarks).(bool)
		c.blockRebalance, _ = conn.OptValue(kgo.BlockRebalanceOnPoll).(bool)
	}
}

func (c *client) Client() *kgo.Client {
	switch conn := c.conn.(type) {
	case *kgo.Client:
		return conn
	case *kgo.GroupTransactSession:
		return conn.Client()
	default:
		panic("kafka: unsupported connection")
	}
}

func (c *client) Session() *kgo.GroupTransactSession {
	conn, ok := c.conn.(*kgo.GroupTransactSession)
	if !ok {
		panic("kafka: group transact session connection expected")
	}

	return conn
}

// Close stops the polling loop and is safe to call concurrently.
func (c *client) Close() {
	c.closeOnce.Do(func() {
		close(c.exitCh)
	})
}

func (c *client) Name() string {
	if c == nil {
		return ""
	}

	return c.name
}

func (c *client) Label(key string) (string, bool) {
	if c == nil {
		return "", false
	}

	value, ok := c.labels[key]
	return value, ok
}

func (c *client) Labels() map[string]string {
	if c == nil {
		return nil
	}

	return maps.Clone(c.labels)
}

func (c *client) applyName() {
	if c.name == "" {
		return
	}

	c.kafkaOpts = append(c.kafkaOpts, kgo.ClientID(c.name))
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
	if err != nil {
		c.stats.recordProduceError()
		c.logger.Log(kgo.LogLevelError, "kafka async producer error",
			logKeyError, err,
			logKeyRecord, c.fmt.AppendRecord(nil, record),
		)
	}
}

func (c *client) formatRecords(records ...*kgo.Record) string {
	buff := make([]byte, 0)

	for _, record := range records {
		buff = c.fmt.AppendRecord(buff, record)
	}

	return string(buff)
}

func newFormatter() (*kgo.RecordFormatter, error) {
	return kgo.NewRecordFormatter("topic: %t, key: %k, msg: %v")
}
