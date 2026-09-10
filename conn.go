package xkafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// clientConn is the minimal Kafka client interface shared by Client and GroupTransactSession.
type clientConn interface {
	Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error))
	TryProduce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error))
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults

	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	AllowRebalance()
}

var (
	_ clientConn = (*kgo.Client)(nil)
	_ clientConn = (*kgo.GroupTransactSession)(nil)
)

// handleFetchesFunc adapts fetched Kafka records to a configured processing strategy.
type handleFetchesFunc func(ctx context.Context, fetches kgo.Fetches) error

// client contains the shared runtime state used by Client and GroupTransactSession.
type client struct {
	conn      clientConn
	kafkaOpts []kgo.Opt

	formatter *kgo.RecordFormatter
	logger    kgo.Logger

	name  string
	hooks hooks

	promiseFunc    PromiseFunc
	defaultPromise PromiseFunc

	handleFetches  handleFetchesFunc
	batchHandler   BatchHandlerFunc
	sessionHandler BatchTxHandlerFunc

	consumerGroup     string
	shareGroup        string
	maxPollRecords    int
	maxHandlerRetries int

	manualCommit    bool
	autoCommitMarks bool
	blockRebalance  bool

	pollInterval             time.Duration
	suspendProcessingTimeout time.Duration
	suspendCommittingTimeout time.Duration

	shareRejectAfterDeliveries int32
	shareReleaseTimeout        time.Duration

	polling      atomic.Bool
	shutdownOnce sync.Once
	shutdownErr  error
	exitCh       chan struct{}
}

func newClient(opts ...Opt) (*client, error) {
	c := &client{
		maxPollRecords:           100,
		maxHandlerRetries:        -1,
		pollInterval:             time.Second,
		suspendProcessingTimeout: 30 * time.Second,
		suspendCommittingTimeout: 10 * time.Second,
		exitCh:                   make(chan struct{}),
	}

	for _, opt := range opts {
		opt.apply(c)
	}

	c.applyName()

	formatter, err := newFormatter()
	if err != nil {
		return nil, fmt.Errorf("kafka: create record formatter: %w", err)
	}
	c.formatter = formatter

	c.initDefaultPromise()

	if c.logger != nil {
		c.kafkaOpts = append(c.kafkaOpts, kgo.WithLogger(c.logger))
	}

	return c, nil
}

func (c *client) Produce(ctx context.Context, record *kgo.Record, promise PromiseFunc) {
	c.hooks.onProduceRecord(recordContext(ctx, record), record)

	if promise == nil {
		c.conn.Produce(ctx, record, c.defaultPromise)
		return
	}

	c.conn.Produce(ctx, record, c.wrapPromise(promise))
}

func (c *client) TryProduce(ctx context.Context, record *kgo.Record, promise PromiseFunc) {
	c.hooks.onProduceRecord(recordContext(ctx, record), record)

	if promise == nil {
		c.conn.TryProduce(ctx, record, c.defaultPromise)
		return
	}

	c.conn.TryProduce(ctx, record, c.wrapPromise(promise))
}

func (c *client) ProduceSync(ctx context.Context, records ...*kgo.Record) error {
	ctx = c.hooks.onProduceStart(ctx, records)

	for _, record := range records {
		c.hooks.onProduceRecord(ctx, record)
	}

	startTime := time.Now()
	results := c.conn.ProduceSync(ctx, records...)
	duration := time.Since(startTime)

	var (
		firstErr    error
		firstRecord *kgo.Record
	)

	for i := range results {
		result := &results[i]
		if result.Err == nil {
			continue
		}

		c.hooks.onProduceError(result.Record, result.Err)

		if firstErr == nil {
			firstErr = result.Err
			firstRecord = result.Record
		}
	}

	var err error
	if firstErr != nil {
		err = fmt.Errorf("kafka: produce records: %w", firstErr)
	}

	c.hooks.onProduceEnd(ctx, records, duration, err)

	if err == nil {
		return nil
	}

	if c.logEnabled(kgo.LogLevelError) {
		c.log(kgo.LogLevelError, "error producing records",
			logKeyError, firstErr,
			logKeyRecord, c.formatRecord(firstRecord),
		)
	}

	return err
}

func (c *client) HandleFetches(ctx context.Context) error {
	if c.conn == nil {
		return errors.New("kafka: conn is nil")
	}

	if c.handleFetches == nil {
		return errors.New("kafka: fetches handler is nil")
	}

	if !c.polling.CompareAndSwap(false, true) {
		return errors.New("kafka: fetch loop already running")
	}
	defer c.polling.Store(false)

	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.exitCh:
			return nil
		case <-ticker.C:
		}

		fetches := c.conn.PollRecords(ctx, c.maxPollRecords)
		if fetches.IsClientClosed() {
			return nil
		}

		if err := c.processFetches(ctx, fetches); err != nil {
			return err
		}
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

func (c *client) Name() string {
	if c == nil {
		return ""
	}

	return c.name
}

// processFetches processes a single PollRecords result.
// The separate scope ensures AllowRebalance is deferred per poll when BlockRebalanceOnPoll is enabled.
func (c *client) processFetches(ctx context.Context, fetches kgo.Fetches) error {
	if c.blockRebalance {
		defer c.conn.AllowRebalance()
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := c.handleFetchErrors(ctx, fetches); err != nil {
		return err
	}

	return c.handleFetches(ctx, fetches)
}

func (c *client) handleFetchErrors(ctx context.Context, fetches kgo.Fetches) error {
	var fatal kgo.FetchError
	var recoverable kgo.FetchError

	fetches.EachError(func(topic string, partition int32, err error) {
		isRecoverable := isRecoverableFetchError(err)
		c.hooks.onFetchError(ctx, topic, partition, isRecoverable, err)

		if isRecoverable {
			if recoverable.Err == nil {
				recoverable = kgo.FetchError{
					Topic:     topic,
					Partition: partition,
					Err:       err,
				}
			}
			return
		}

		if fatal.Err == nil {
			fatal = kgo.FetchError{
				Topic:     topic,
				Partition: partition,
				Err:       err,
			}
		}
	})

	if fatal.Err != nil {
		c.log(kgo.LogLevelError, "error fetching records",
			logKeyError, fatal.Err,
			logKeyTopic, fatal.Topic,
			logKeyPartition, fatal.Partition,
		)

		return fmt.Errorf("kafka: fetch topic %q partition %d: %w", fatal.Topic, fatal.Partition, fatal.Err)
	}

	if recoverable.Err != nil {
		c.log(kgo.LogLevelWarn, "recoverable error fetching records",
			logKeyError, recoverable.Err,
			logKeyTopic, recoverable.Topic,
			logKeyPartition, recoverable.Partition,
		)
	}

	return nil
}

func (c *client) parseKafkaOptions(conn *kgo.Client) {
	c.consumerGroup, _ = conn.OptValue(kgo.ConsumerGroup).(string)
	c.shareGroup, _ = conn.OptValue(kgo.ShareGroup).(string)

	if c.consumerGroup != "" {
		c.manualCommit, _ = conn.OptValue(kgo.DisableAutoCommit).(bool)
		c.autoCommitMarks, _ = conn.OptValue(kgo.AutoCommitMarks).(bool)
		c.blockRebalance, _ = conn.OptValue(kgo.BlockRebalanceOnPoll).(bool)
	}
}

func (c *client) applyName() {
	if c.name == "" {
		return
	}

	c.kafkaOpts = append(c.kafkaOpts, kgo.ClientID(c.name))
}

func (c *client) initDefaultPromise() {
	c.defaultPromise = func(record *kgo.Record, err error) {
		c.produceErrorPromise(record, err)

		if c.promiseFunc != nil {
			c.promiseFunc(record, err)
		}
	}
}

func (c *client) wrapPromise(promise PromiseFunc) PromiseFunc {
	return func(record *kgo.Record, err error) {
		c.produceErrorPromise(record, err)
		promise(record, err)
	}
}

func (c *client) produceErrorPromise(record *kgo.Record, err error) {
	if err == nil {
		return
	}

	c.hooks.onProduceError(record, err)

	if c.logEnabled(kgo.LogLevelError) {
		c.logger.Log(kgo.LogLevelError, "error producing record",
			logKeyError, err,
			logKeyRecord, c.formatRecord(record),
		)
	}
}

func (c *client) formatRecord(record *kgo.Record) string {
	return string(c.formatter.AppendRecord(nil, record))
}

func recordContext(ctx context.Context, record *kgo.Record) context.Context {
	if record != nil && record.Context != nil {
		return record.Context
	}

	return ctx
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

func newFormatter() (*kgo.RecordFormatter, error) {
	return kgo.NewRecordFormatter("topic: %t, key: %k, msg: %v")
}
