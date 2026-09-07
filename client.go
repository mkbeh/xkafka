package xkafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Client provides Kafka produce and consume operations.
type Client struct {
	cl *client
}

// NewClient creates a Kafka client with the provided options.
func NewClient(opts ...Opt) (*Client, error) {
	cl, err := newClient(opts...)
	if err != nil {
		return nil, err
	}

	conn, err := kgo.NewClient(cl.kafkaOpts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create client: %w", err)
	}

	cl.conn = conn
	cl.applyKafkaOptions(conn)

	c := &Client{
		cl: cl,
	}

	if err := c.bindHandler(); err != nil {
		conn.Close()
		return nil, err
	}

	cl.hooks.onNewClient(c)

	return c, nil
}

// Name returns the logical client name configured with WithName.
func (c *Client) Name() string {
	if c == nil || c.cl == nil {
		return ""
	}

	return c.cl.Name()
}

// Label returns one client label without allocating a copy of all labels.
func (c *Client) Label(key string) (string, bool) {
	if c == nil || c.cl == nil {
		return "", false
	}

	return c.cl.Label(key)
}

// Labels returns a detached copy of the client labels.
func (c *Client) Labels() map[string]string {
	if c == nil || c.cl == nil {
		return nil
	}

	return c.cl.Labels()
}

// ConsumerGroup returns the configured consumer group.
func (c *Client) ConsumerGroup() string {
	if c == nil || c.cl == nil {
		return ""
	}

	return c.cl.consumerGroup
}

// ShareGroup returns the configured Share Group.
func (c *Client) ShareGroup() string {
	if c == nil || c.cl == nil {
		return ""
	}

	return c.cl.shareGroup
}

func (c *Client) Ping(ctx context.Context) error {
	if err := c.cl.Client().Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping client: %w", err)
	}

	return nil
}

func (c *Client) Produce(ctx context.Context, record *kgo.Record, promise PromiseFunc) {
	c.cl.Produce(ctx, record, promise)
}

func (c *Client) TryProduce(ctx context.Context, record *kgo.Record, promise PromiseFunc) {
	c.cl.TryProduce(ctx, record, promise)
}

func (c *Client) ProduceSync(ctx context.Context, records ...*kgo.Record) error {
	return c.cl.ProduceSync(ctx, records...)
}

// RunInTx executes fn inside a Kafka transaction.
//
// The transaction is committed when fn returns nil. Once begun, errors before
// the terminal commit attempt cause the transaction to be aborted. Commit
// failures are handled according to franz-go transaction recovery semantics.
//
// Panics are re-thrown. If a panic occurs before the terminal commit begins,
// xkafka first attempts to abort the transaction.
func (c *Client) RunInTx(ctx context.Context, fn TxFunc) (err error) {
	if fn == nil {
		return errors.New("kafka: transaction function is nil")
	}

	ctx = c.cl.hooks.onTransactionStart(ctx, TransactionTypeProducer)
	startTime := time.Now()
	outcome := TransactionOutcomeError

	// Report the final transaction result after transaction cleanup completes.
	defer func() {
		c.cl.hooks.onTransactionEnd(
			ctx,
			TransactionTypeProducer,
			outcome,
			time.Since(startTime),
			err,
		)
	}()

	conn := c.cl.Client()

	if err = conn.BeginTransaction(); err != nil {
		err = fmt.Errorf("kafka: begin transaction: %w", err)
		return err
	}

	abortOnExit := true

	// Abort the transaction on errors or panics until the terminal commit begins.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("kafka: transaction panic: %v", r)

			c.cl.log(kgo.LogLevelError, "panic recovered in kafka transaction, aborting", logKeyError, r)

			if abortOnExit {
				if abortErr := c.abortTransaction(ctx); abortErr == nil {
					outcome = TransactionOutcomeAbort
				}
			}

			panic(r)
		}

		if abortOnExit && err != nil {
			if abortErr := c.abortTransaction(ctx); abortErr != nil {
				err = fmt.Errorf("kafka: transaction failed: %w; abort failed: %w", err, abortErr)
			} else {
				outcome = TransactionOutcomeAbort
			}
		}
	}()

	if err = fn(ctx, &Tx{cl: c.cl}); err != nil {
		return err
	}

	if err = conn.Flush(ctx); err != nil {
		err = fmt.Errorf("kafka: flush buffered records: %w", err)
		return err
	}

	// EndTransaction is terminal. From this point on, commit failures are
	// recovered only according to EndTransaction's documented semantics.
	abortOnExit = false

	if err = conn.EndTransaction(ctx, kgo.TryCommit); err != nil {
		if shouldAbortAfterCommit(err) {
			if abortErr := c.abortTransaction(ctx); abortErr != nil {
				err = fmt.Errorf("kafka: commit transaction: %w; recovery failed: %w", err, abortErr)
				return err
			}
		}

		err = fmt.Errorf("kafka: commit transaction: %w", err)
		return err
	}

	outcome = TransactionOutcomeCommit

	return nil
}

func (c *Client) HandleFetches(ctx context.Context) error {
	return c.cl.HandleFetches(ctx)
}

// Shutdown stops polling, flushes pending records and acks, and closes the underlying client.
func (c *Client) Shutdown(ctx context.Context) error {
	closed := false

	c.cl.shutdownOnce.Do(func() {
		closed = true
		close(c.cl.exitCh)

		if c.cl.conn == nil {
			return
		}

		var err error
		conn := c.cl.Client()

		if flushErr := conn.Flush(ctx); flushErr != nil {
			c.cl.log(kgo.LogLevelError, "error flushing producer records", logKeyError, flushErr)
			err = errors.Join(err, flushErr)
		}

		if c.cl.shareGroup != "" {
			startTime := time.Now()
			flushErr := conn.FlushAcks(ctx)

			c.cl.hooks.onShareAckFlush(ctx, time.Since(startTime), flushErr)

			if flushErr != nil {
				c.cl.log(kgo.LogLevelError, "error flushing share group acks", logKeyError, flushErr)
				err = errors.Join(err, flushErr)
			}
		}

		if c.cl.blockRebalance {
			conn.CloseAllowingRebalance()
		} else {
			conn.Close()
		}

		c.cl.shutdownErr = err
	})

	if closed {
		c.cl.hooks.onClientClosed(c)
	}

	return c.cl.shutdownErr
}

func (c *Client) bindHandler() error {
	if c.cl.batchHandler == nil {
		if c.cl.consumerGroup != "" || c.cl.shareGroup != "" {
			return errors.New("kafka: consumer requires batch handler")
		}

		return nil
	}

	if c.cl.shareGroup != "" {
		c.cl.handleFetches = c.handleShareFetchesBatch(c.cl.batchHandler)
		return nil
	}

	// Regular consumer group or direct consumption.
	c.cl.handleFetches = c.handleFetchesBatch(c.cl.batchHandler)

	return nil
}

func (c *Client) abortTransaction(ctx context.Context) error {
	conn := c.cl.Client()

	// Abort buffered records before ending the transaction so they cannot
	// be carried into the next transaction.
	if err := conn.AbortBufferedRecords(ctx); err != nil {
		c.cl.log(kgo.LogLevelError, "error aborting buffered records", logKeyError, err)
		return fmt.Errorf("kafka: abort buffered records: %w", err)
	}

	if err := conn.EndTransaction(ctx, kgo.TryAbort); err != nil {
		c.cl.log(kgo.LogLevelError, "error aborting transaction", logKeyError, err)
		return fmt.Errorf("kafka: abort transaction: %w", err)
	}

	return nil
}

func (c *Client) handleFetchesBatch(handler BatchHandlerFunc) handleFetchesFunc {
	return func(ctx context.Context, fetches kgo.Fetches) {
		records := fetches.Records()
		if len(records) == 0 {
			return
		}

		if !c.cl.wait(ctx, 0) {
			return
		}

		for {
			handleCtx, err := c.handleRecords(ctx, records, handler)
			if err != nil {
				if !c.cl.wait(ctx, c.cl.suspendProcessingTimeout) {
					return
				}

				continue
			}

			switch {
			case c.cl.manualCommit:
				c.commitOffsets(handleCtx)
			case c.cl.autoCommitMarks:
				c.cl.Client().MarkCommitRecords(records...)
			}

			return
		}
	}
}

func (c *Client) commitOffsets(ctx context.Context) {
	conn := c.cl.Client()

	for {
		startTime := time.Now()
		err := conn.CommitUncommittedOffsets(ctx)

		c.cl.hooks.onOffsetCommit(ctx, time.Since(startTime), err)

		if err == nil {
			return
		}

		c.cl.log(kgo.LogLevelError, "error committing offsets", logKeyError, err)

		if !c.cl.wait(ctx, c.cl.suspendCommittingTimeout) {
			return
		}
	}
}

func (c *Client) handleShareFetchesBatch(handler BatchHandlerFunc) handleFetchesFunc {
	return func(ctx context.Context, fetches kgo.Fetches) {
		records := fetches.Records()
		if len(records) == 0 {
			return
		}

		if !c.cl.wait(ctx, 0) {
			return
		}

		handleCtx, err := c.handleRecords(ctx, records, handler)

		status := kgo.AckAccept
		if err != nil {
			status = kgo.AckRelease
		}

		c.ackRecords(handleCtx, records, status)
	}
}

func (c *Client) ackRecords(ctx context.Context, records []*kgo.Record, status kgo.AckStatus) {
	var (
		acceptCount  int
		releaseCount int
		rejectCount  int
	)

	switch status {
	case kgo.AckAccept:
		for _, record := range records {
			record.Ack(kgo.AckAccept)
		}

		acceptCount = len(records)

	case kgo.AckRelease:
		for _, record := range records {
			if c.cl.shareRejectAfterDeliveries > 0 &&
				record.DeliveryCount() >= c.cl.shareRejectAfterDeliveries {
				record.Ack(kgo.AckReject)
				rejectCount++

				continue
			}

			record.Ack(kgo.AckRelease)
			releaseCount++
		}

	default:
		panic("xkafka: invalid share ack status")
	}

	c.cl.hooks.onShareAck(ctx, ShareAckAccept, acceptCount)
	c.cl.hooks.onShareAck(ctx, ShareAckRelease, releaseCount)
	c.cl.hooks.onShareAck(ctx, ShareAckReject, rejectCount)

	if releaseCount > 0 {
		// Delay released records, but always attempt to flush acknowledgements.
		c.cl.wait(ctx, c.cl.shareReleaseTimeout)
	}

	c.flushAcks(ctx)
}

func (c *Client) flushAcks(ctx context.Context) {
	conn := c.cl.Client()

	for {
		startTime := time.Now()
		err := conn.FlushAcks(ctx)

		c.cl.hooks.onShareAckFlush(ctx, time.Since(startTime), err)

		if err == nil {
			return
		}

		c.cl.log(kgo.LogLevelError, "error flushing share group acks", logKeyError, err)

		if !c.cl.wait(ctx, c.cl.suspendCommittingTimeout) {
			return
		}
	}
}

func (c *Client) handleRecords(
	ctx context.Context,
	records []*kgo.Record,
	handler BatchHandlerFunc,
) (handleCtx context.Context, handleErr error) {
	handleCtx = c.cl.hooks.onHandleStart(ctx, records)
	startTime := time.Now()

	defer func() {
		if r := recover(); r != nil {
			handleErr = fmt.Errorf("kafka: batch handler panic: %v", r)
		}

		c.cl.hooks.onHandleEnd(handleCtx, records, time.Since(startTime), handleErr)

		if handleErr != nil {
			c.cl.log(
				kgo.LogLevelError,
				"error handling records",
				logKeyError, handleErr,
				logKeyRecord, c.cl.formatRecord(records[0]),
				logKeyRecordCount, len(records),
			)
		}
	}()

	handleErr = handler(handleCtx, records)

	return handleCtx, handleErr
}

func shouldAbortAfterCommit(err error) bool {
	switch {
	case errors.Is(err, kerr.OperationNotAttempted),
		errors.Is(err, kerr.TransactionAbortable),
		errors.Is(err, kerr.UnknownServerError):
		return true

	case errors.Is(err, kgo.ErrClientClosed):
		return false
	}

	// A non-Kafka error means the commit outcome may be unconfirmed,
	// typically because of a transport failure. franz-go requires retrying
	// EndTransaction with TryAbort to recover the producer ID and fence-abort
	// any transaction that may still be open broker-side.
	_, isKafkaErr := errors.AsType[*kerr.Error](err)

	return !isKafkaErr
}
