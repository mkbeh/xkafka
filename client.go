package xkafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mkbeh/xkafka/internal/pkg/kprom"
	"github.com/mkbeh/xkafka/internal/pkg/kslog"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Client is a high-level Kafka client for producing records and consuming records through handlers.
type Client struct {
	conn *kgo.Client
	cl   *client
}

// NewClient creates a Kafka client with the provided options and verifies broker connectivity.
func NewClient(opts ...Opt) (*Client, error) {
	cl, err := newClient(opts...)
	if err != nil {
		return nil, err
	}

	conn, err := kgo.NewClient(cl.clientOps...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create client: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("kafka: ping client: %w", err)
	}

	cl.conn = conn

	c := &Client{
		conn: conn,
		cl:   cl,
	}

	if cl.clientHandleFetches != nil {
		cl.handleFetches = cl.clientHandleFetches(c)
	}

	return c, nil
}

func (c *Client) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	c.cl.Produce(ctx, record, promise)
}

func (c *Client) TryProduce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	c.cl.TryProduce(ctx, record, promise)
}

func (c *Client) ProduceSync(ctx context.Context, records ...*kgo.Record) error {
	return c.cl.ProduceSync(ctx, records...)
}

func (c *Client) HandleFetches(ctx context.Context) error {
	return c.cl.HandleFetches(ctx)
}

// RunInTx executes fn inside a Kafka transaction.
//
// The transaction is committed when fn returns nil. It is aborted when fn returns an error.
// Panics are recovered only long enough to abort the transaction, then re-thrown.
func (c *Client) RunInTx(ctx context.Context, fn TxFunc) (err error) {
	startTime := time.Now()
	txOutcome := kprom.TransactionOutcomeError

	defer func() {
		if c.cl.metrics != nil {
			c.cl.metrics.Producer().CollectTransaction(startTime, txOutcome)
		}
	}()

	if fn == nil {
		return fmt.Errorf("kafka: transaction function is nil")
	}

	if err = c.conn.BeginTransaction(); err != nil {
		return fmt.Errorf("kafka: begin transaction: %w", err)
	}

	shouldAbort := true

	defer func() {
		if r := recover(); r != nil {
			c.cl.logger.ErrorContext(ctx, "panic recovered in kafka transaction, aborting",
				slog.Any("error", r),
			)

			if shouldAbort {
				if abortErr := c.abortTransaction(ctx); abortErr != nil {
					c.cl.logger.ErrorContext(ctx, "kafka transaction abort after panic failed",
						kslog.Error(abortErr),
					)
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
				err = fmt.Errorf("kafka: transaction failed: %w; abort failed: %v", err, abortErr)
			}
		}
	}()

	if err = fn(ctx); err != nil {
		return err
	}

	if err = c.conn.Flush(ctx); err != nil {
		return fmt.Errorf("kafka: flush buffered records: %w", err)
	}

	// Commit is a terminal transaction operation.
	// After this point, do not run the deferred abort for arbitrary commit errors.
	// franz-go allows TryAbort only for specific transaction errors.
	shouldAbort = false

	if err = c.conn.EndTransaction(ctx, kgo.TryCommit); err != nil {
		if errors.Is(err, kerr.OperationNotAttempted) || errors.Is(err, kerr.TransactionAbortable) {
			if abortErr := c.abortTransaction(ctx); abortErr != nil {
				return fmt.Errorf("kafka: commit failed: %w; abort also failed: %v", err, abortErr)
			}

			return fmt.Errorf("kafka: commit failed, transaction aborted: %w", err)
		}

		return fmt.Errorf("kafka: commit transaction: %w", err)
	}

	txOutcome = kprom.TransactionOutcomeCommit
	return nil
}

// Shutdown stops polling, flushes pending records and acks, and closes the underlying client.
func (c *Client) Shutdown(ctx context.Context) error {
	c.cl.Close()

	if c.conn == nil {
		return nil
	}

	var err error

	if flushErr := c.conn.Flush(ctx); flushErr != nil {
		c.cl.logger.ErrorContext(ctx, "error flushing producer records", kslog.Error(flushErr))
		err = errors.Join(err, flushErr)
	}

	if flushErr := c.conn.FlushAcks(ctx); flushErr != nil {
		c.cl.logger.ErrorContext(ctx, "error flushing consumer acks", kslog.Error(flushErr))
		err = errors.Join(err, flushErr)
	}

	c.conn.Close()

	return err
}

func (c *Client) handleFetchesBatch(handler BatchHandlerFunc) handleFetchesFunc {
	return func(ctx context.Context, fetches kgo.Fetches) {
		records := fetches.Records()
		if len(records) == 0 {
			return
		}

		defer func(startTime time.Time) {
			for _, r := range records {
				c.cl.metrics.Consumer().CollectHandleProcessTiming(startTime, r.Topic)
			}
		}(time.Now())

	infiniteLoop:
		for {
			select {
			case <-c.cl.exitCh:
				return
			default:
				if err := c.handleBatch(ctx, records, handler); err != nil {
					time.Sleep(c.cl.suspendProcessingTimeout)
					continue
				}
				c.commitInternalOffsetsEternal(ctx)
				break infiniteLoop
			}
		}
	}
}

func (c *Client) commitInternalOffsetsEternal(ctx context.Context) {
	if !c.cl.groupSpecified {
		return
	}

infiniteLoop:
	for {
		select {
		case <-c.cl.exitCh:
			return
		default:
			if err := c.conn.CommitUncommittedOffsets(ctx); err != nil {
				c.cl.metrics.Consumer().CollectHandleErrors("")
				c.cl.logger.ErrorContext(ctx, "error committing offsets", kslog.Error(err))
				time.Sleep(c.cl.suspendCommittingTimeout)
			} else {
				c.cl.logger.DebugContext(ctx, "offsets committed", kslog.Count(len(c.conn.CommittedOffsets())))
				break infiniteLoop
			}
		}
	}
}

func (c *Client) handleShareFetchesBatch(handler BatchHandlerFunc) handleFetchesFunc {
	return func(ctx context.Context, fetches kgo.Fetches) {
		records := fetches.Records()
		if len(records) == 0 {
			return
		}

		defer func(startTime time.Time) {
			for _, r := range records {
				c.cl.metrics.Consumer().CollectHandleProcessTiming(startTime, r.Topic)
			}
		}(time.Now())

		select {
		case <-c.cl.exitCh:
			return
		default:
			if err := c.handleBatch(ctx, records, handler); err != nil {
				c.ackErrorRecordsEternal(ctx, records)
				return
			}
			c.ackRecordsEternal(ctx, records)
		}
	}
}

func (c *Client) handleBatch(
	ctx context.Context,
	records []*kgo.Record,
	handler BatchHandlerFunc,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("kafka: batch handler panic: %v", r)
		}

		if err != nil {
			c.cl.metrics.Consumer().CollectHandleErrors("")

			c.cl.logger.ErrorContext(ctx, "error handling records",
				kslog.Error(err),
				kslog.Records(c.cl.formatRecords(records...)),
			)
		}
	}()

	return handler(ctx, records)
}

func (c *Client) ackRecordsEternal(ctx context.Context, records []*kgo.Record) {
	for _, record := range records {
		record.Ack(kgo.AckAccept)
	}

	c.flushAcksEternal(ctx, "")
}

func (c *Client) ackErrorRecordsEternal(ctx context.Context, records []*kgo.Record) {
	releaseWaited := false

	for _, record := range records {
		status := c.errorAckStatus(record)

		if status == kgo.AckRelease && !releaseWaited {
			releaseWaited = true

			if c.cl.shareReleaseTimeout > 0 {
				timer := time.NewTimer(c.cl.shareReleaseTimeout)

				select {
				case <-ctx.Done():
				case <-c.cl.exitCh:
				case <-timer.C:
				}

				timer.Stop()
			}
		}

		record.Ack(status)
	}

	c.flushAcksEternal(ctx, "")
}

func (c *Client) errorAckStatus(record *kgo.Record) kgo.AckStatus {
	if c.cl.shareRejectAfterDeliveries > 0 &&
		record.DeliveryCount() >= c.cl.shareRejectAfterDeliveries {
		return kgo.AckReject
	}

	return kgo.AckRelease
}

func (c *Client) flushAcksEternal(ctx context.Context, topic string) {
	for {
		select {
		case <-c.cl.exitCh:
			return
		default:
			if err := c.conn.FlushAcks(ctx); err != nil {
				c.cl.metrics.Consumer().CollectHandleErrors(topic)
				c.cl.logger.ErrorContext(ctx, "error flushing share group acks", kslog.Error(err))
				time.Sleep(c.cl.suspendCommittingTimeout)
				continue
			}

			return
		}
	}
}

func (c *Client) abortTransaction(ctx context.Context) error {
	// AbortBufferedRecords is required before aborting a transaction so that
	// buffered records are not accidentally carried into the next transaction.
	if err := c.conn.AbortBufferedRecords(ctx); err != nil {
		c.cl.logger.ErrorContext(ctx, "error aborting buffered records", kslog.Error(err))
		return fmt.Errorf("abort buffered records: %w", err)
	}

	if err := c.conn.EndTransaction(ctx, kgo.TryAbort); err != nil {
		c.cl.logger.ErrorContext(ctx, "error rolling back transaction", kslog.Error(err))
		return fmt.Errorf("abort transaction: %w", err)
	}

	return nil
}
