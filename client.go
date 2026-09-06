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

func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.cl == nil || c.cl.conn == nil {
		return fmt.Errorf("kafka: client is nil")
	}

	if err := c.cl.Client().Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping client: %w", err)
	}

	return nil
}

func (c *Client) HandleFetches(ctx context.Context) error {
	return c.cl.HandleFetches(ctx)
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
// The transaction is committed when fn returns nil. It is aborted when fn returns an error.
// Panics are recovered only long enough to abort the transaction, then re-thrown.
func (c *Client) RunInTx(ctx context.Context, fn TxFunc) (err error) {
	ctx = c.cl.hooks.onTransactionStart(ctx, TransactionTypeProducer)
	startTime := time.Now()
	outcome := TransactionOutcomeError
	var hookErr error

	defer func() {
		if hookErr == nil {
			hookErr = err
		}

		c.cl.hooks.onTransactionEnd(
			ctx,
			TransactionTypeProducer,
			outcome,
			time.Since(startTime),
			hookErr,
		)
	}()

	if fn == nil {
		return fmt.Errorf("kafka: transaction function is nil")
	}

	conn := c.cl.Client()

	if err = conn.BeginTransaction(); err != nil {
		return fmt.Errorf("kafka: begin transaction: %w", err)
	}

	shouldAbort := true

	defer func() {
		if r := recover(); r != nil {
			hookErr = fmt.Errorf("kafka: transaction panic: %v", r)
			c.cl.log(kgo.LogLevelError, "panic recovered in kafka transaction, aborting", logKeyError, r)

			if shouldAbort {
				if abortErr := c.abortTransaction(ctx); abortErr != nil {
					c.cl.log(kgo.LogLevelError, "kafka transaction abort after panic failed",
						logKeyError, abortErr,
					)
				} else {
					outcome = TransactionOutcomeAbort
				}
			}

			// RunInTx is responsible only for transaction cleanup.
			// The original panic is re-thrown so callers can handle it with their own
			// recovery middleware and so programming errors are not silently converted
			// into regular transaction errors.
			panic(r)
		}

		if shouldAbort && err != nil {
			if abortErr := c.abortTransaction(ctx); abortErr != nil {
				err = fmt.Errorf("kafka: transaction failed: %w; abort failed: %w", err, abortErr)
			} else {
				outcome = TransactionOutcomeAbort
			}
		}
	}()

	tx := &Tx{cl: c.cl}

	if err = fn(ctx, tx); err != nil {
		return err
	}

	if err = conn.Flush(ctx); err != nil {
		return fmt.Errorf("kafka: flush buffered records: %w", err)
	}

	// Commit is a terminal transaction operation.
	// After this point, do not run the deferred full abort because commit
	// failures are handled explicitly below.
	shouldAbort = false

	if err = conn.EndTransaction(ctx, kgo.TryCommit); err != nil {
		if shouldRetryAbortAfterCommit(err) {
			if abortErr := c.abortTransaction(ctx); abortErr != nil {
				return fmt.Errorf("kafka: commit transaction: %w; recovery failed: %w", err, abortErr)
			}
		}

		return fmt.Errorf("kafka: commit transaction: %w", err)
	}

	outcome = TransactionOutcomeCommit
	return nil
}

// Shutdown stops polling, flushes pending records and acks, and closes the underlying client.
func (c *Client) Shutdown(ctx context.Context) error {
	didShutdown := false
	defer func() {
		if didShutdown {
			c.cl.hooks.onClientClosed(c)
		}
	}()

	c.cl.shutdownOnce.Do(func() {
		didShutdown = true
		close(c.cl.exitCh)

		if c.cl.conn == nil {
			return
		}

		conn := c.cl.Client()

		var err error

		if flushErr := conn.Flush(ctx); flushErr != nil {
			c.cl.log(kgo.LogLevelError, "error flushing producer records", logKeyError, flushErr)
			err = errors.Join(err, flushErr)
		}

		if flushErr := conn.FlushAcks(ctx); flushErr != nil {
			c.cl.hooks.onShareAckError(ctx, flushErr)
			c.cl.log(kgo.LogLevelError, "error flushing share group acks", logKeyError, flushErr)
			err = errors.Join(err, flushErr)
		}

		c.cl.shutdownErr = err

		if c.cl.blockRebalance {
			conn.CloseAllowingRebalance()
		} else {
			conn.Close()
		}
	})

	return c.cl.shutdownErr
}

func (c *Client) abortTransaction(ctx context.Context) error {
	conn := c.cl.Client()

	// AbortBufferedRecords is required before aborting a transaction so that
	// buffered records are not accidentally carried into the next transaction.
	if err := conn.AbortBufferedRecords(ctx); err != nil {
		c.cl.log(kgo.LogLevelError, "error aborting buffered records", logKeyError, err)
		return fmt.Errorf("abort buffered records: %w", err)
	}

	if err := conn.EndTransaction(ctx, kgo.TryAbort); err != nil {
		c.cl.log(kgo.LogLevelError, "error rolling back transaction", logKeyError, err)
		return fmt.Errorf("abort transaction: %w", err)
	}

	return nil
}

func shouldRetryAbortAfterCommit(err error) bool {
	if kerr.IsRetriable(err) ||
		errors.Is(err, kerr.OperationNotAttempted) ||
		errors.Is(err, kerr.TransactionAbortable) ||
		errors.Is(err, kerr.UnknownServerError) {
		return true
	}

	// An attempted EndTxn that fails with a non-Kafka error has an
	// unconfirmed outcome (typically a transport failure). franz-go requires
	// a TryAbort retry so it can recover the producer ID and fence-abort any
	// transaction that may still be open broker-side.
	_, isKafkaErr := errors.AsType[*kerr.Error](err)

	return !isKafkaErr && !errors.Is(err, kgo.ErrClientClosed)
}

func (c *Client) handleFetchesBatch(handler BatchHandlerFunc) handleFetchesFunc {
	return func(ctx context.Context, fetches kgo.Fetches) error {
		records := fetches.Records()
		if len(records) == 0 {
			return nil
		}

		retries := 0

		for {
			select {
			case <-c.cl.exitCh:
				return nil
			default:
			}

			if retries > 0 {
				c.cl.hooks.onHandleRetry(ctx, retries)
			}

			if err := c.handleRecords(ctx, records, handler); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}

				select {
				case <-c.cl.exitCh:
					return nil
				default:
				}

				if c.cl.maxHandlerRetries > 0 && retries >= c.cl.maxHandlerRetries {
					c.cl.hooks.onHandleRetryExhausted(ctx, retries, err)
					return fmt.Errorf("kafka: handler retries exhausted after %d retries: %w", retries, err)
				}

				retries++

				if !c.cl.wait(ctx, c.cl.suspendProcessingTimeout) {
					return nil
				}

				continue
			}

			switch {
			case c.cl.manualCommit:
				c.commitOffsets(ctx)
			case c.cl.autoCommitMarks:
				c.cl.Client().MarkCommitRecords(records...)
			}

			return nil
		}
	}
}

func (c *Client) commitOffsets(ctx context.Context) {
	if !c.cl.manualCommit {
		return
	}

	conn := c.cl.Client()

	for {
		if err := conn.CommitUncommittedOffsets(ctx); err != nil {
			c.cl.hooks.onOffsetCommitError(ctx, err)
			c.cl.log(kgo.LogLevelError, "error committing offsets", logKeyError, err)

			if !c.cl.wait(ctx, c.cl.suspendCommittingTimeout) {
				return
			}

			continue
		}

		return
	}
}

func (c *Client) handleShareFetchesBatch(handler BatchHandlerFunc) handleFetchesFunc {
	return func(ctx context.Context, fetches kgo.Fetches) error {
		records := fetches.Records()
		if len(records) == 0 {
			return nil
		}

		select {
		case <-c.cl.exitCh:
			return nil
		default:
			if err := c.handleRecords(ctx, records, handler); err != nil {
				c.ackRecords(ctx, records, true)
				return nil
			}
			c.ackRecords(ctx, records, false)
		}

		return nil
	}
}

func (c *Client) ackRecords(ctx context.Context, records []*kgo.Record, isError bool) {
	var (
		hasRelease                             bool
		acceptCount, releaseCount, rejectCount int
	)

	for _, record := range records {
		status := kgo.AckAccept

		if isError {
			status = kgo.AckRelease
			if c.cl.shareRejectAfterDeliveries > 0 && record.DeliveryCount() >= c.cl.shareRejectAfterDeliveries {
				status = kgo.AckReject
			}
			if status == kgo.AckRelease {
				hasRelease = true
			}
		}

		record.Ack(status)

		switch status {
		case kgo.AckAccept:
			acceptCount++
		case kgo.AckRelease:
			releaseCount++
		case kgo.AckReject:
			rejectCount++
		}
	}

	c.cl.hooks.onShareAck(ctx, ShareAckAccept, acceptCount)
	c.cl.hooks.onShareAck(ctx, ShareAckRelease, releaseCount)
	c.cl.hooks.onShareAck(ctx, ShareAckReject, rejectCount)

	if hasRelease && c.cl.shareReleaseTimeout > 0 {
		timer := time.NewTimer(c.cl.shareReleaseTimeout)
		select {
		case <-ctx.Done():
		case <-c.cl.exitCh:
		case <-timer.C:
		}
		timer.Stop()
	}

	c.flushAcks(ctx)
}

func (c *Client) flushAcks(ctx context.Context) {
	conn := c.cl.Client()

	for {
		if err := conn.FlushAcks(ctx); err != nil {
			c.cl.hooks.onShareAckError(ctx, err)
			c.cl.log(kgo.LogLevelError, "error flushing share group acks", logKeyError, err)

			if !c.cl.wait(ctx, c.cl.suspendCommittingTimeout) {
				return
			}

			continue
		}

		return
	}
}

func (c *Client) handleRecords(ctx context.Context, records []*kgo.Record, handler BatchHandlerFunc) (err error) {
	ctx = c.cl.hooks.onHandleStart(ctx, len(records))
	startTime := time.Now()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("kafka: batch handler panic: %v", r)
		}

		c.cl.hooks.onHandleEnd(ctx, len(records), time.Since(startTime), err)

		if err != nil {
			c.cl.log(kgo.LogLevelError, "error handling records",
				logKeyError, err,
				logKeyRecord, c.cl.formatRecord(records[0]),
				logKeyRecordCount, len(records),
			)
		}
	}()

	return handler(ctx, records)
}
