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
	cl      *client
	conn    *kgo.Client
	metrics MetricsRegistration
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
		conn: conn,
		cl:   cl,
	}

	// Bind the fetch handler after Client is created because the adapter needs
	// Client-specific operations such as offset commits and Share Group acknowledgments.
	if cl.clientHandleFetches != nil {
		cl.handleFetches = cl.clientHandleFetches(c)
	}

	if err := c.registerMetrics(cl.metrics); err != nil {
		conn.Close()
		return nil, fmt.Errorf("kafka: register client metrics: %w", err)
	}

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

func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.conn == nil {
		return fmt.Errorf("kafka: client is nil")
	}

	if err := c.conn.Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping client: %w", err)
	}

	return nil
}

// Stats returns a snapshot of the current client statistics.
func (c *Client) Stats() Stats {
	if c == nil || c.cl == nil {
		return Stats{}
	}

	return c.cl.stats.snapshot()
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
	startTime := time.Now()
	outcome := transactionOutcomeError

	defer func() {
		c.cl.stats.recordTransaction(outcome, time.Since(startTime))
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
			c.cl.logger.Log(kgo.LogLevelError, "panic recovered in kafka transaction, aborting", logKeyError, r)

			if shouldAbort {
				if abortErr := c.abortTransaction(ctx); abortErr != nil {
					c.cl.logger.Log(kgo.LogLevelError, "kafka transaction abort after panic failed",
						logKeyError, abortErr,
					)
				} else {
					outcome = transactionOutcomeAbort
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
			} else {
				outcome = transactionOutcomeAbort
			}
		}
	}()

	tx := &Tx{cl: c.cl}

	if err = fn(ctx, tx); err != nil {
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

			outcome = transactionOutcomeAbort
			return fmt.Errorf("kafka: commit failed, transaction aborted: %w", err)
		}

		return fmt.Errorf("kafka: commit transaction: %w", err)
	}

	outcome = transactionOutcomeCommit
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
		c.cl.logger.Log(kgo.LogLevelError, "error flushing producer records", logKeyError, flushErr)
		err = errors.Join(err, flushErr)
	}

	if flushErr := c.conn.FlushAcks(ctx); flushErr != nil {
		c.cl.logger.Log(kgo.LogLevelError, "error flushing producer records", logKeyError, flushErr)
		err = errors.Join(err, flushErr)
	}

	if c.metrics != nil {
		c.metrics.Close()
	}

	c.conn.Close()

	return err
}

func (c *Client) registerMetrics(metrics Metrics) error {
	if metrics == nil {
		return nil
	}

	registration, err := metrics.Register(c)
	if err != nil {
		return err
	}

	c.metrics = registration
	return nil
}

func (c *Client) abortTransaction(ctx context.Context) error {
	// AbortBufferedRecords is required before aborting a transaction so that
	// buffered records are not accidentally carried into the next transaction.
	if err := c.conn.AbortBufferedRecords(ctx); err != nil {
		c.cl.logger.Log(kgo.LogLevelError, "error aborting buffered records", logKeyError, err)
		return fmt.Errorf("abort buffered records: %w", err)
	}

	if err := c.conn.EndTransaction(ctx, kgo.TryAbort); err != nil {
		c.cl.logger.Log(kgo.LogLevelError, "error rolling back transaction", logKeyError, err)
		return fmt.Errorf("abort transaction: %w", err)
	}

	return nil
}

func (c *Client) handleFetchesBatch(handler BatchHandlerFunc) handleFetchesFunc {
	return func(ctx context.Context, fetches kgo.Fetches) {
		records := fetches.Records()
		if len(records) == 0 {
			return
		}

	infiniteLoop:
		for {
			select {
			case <-c.cl.exitCh:
				return
			default:
				if err := c.handleRecords(ctx, records, handler); err != nil {
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
	if !c.cl.manualCommit {
		return
	}

infiniteLoop:
	for {
		select {
		case <-c.cl.exitCh:
			return
		default:
			if err := c.conn.CommitUncommittedOffsets(ctx); err != nil {
				c.cl.stats.recordOffsetCommitError()
				c.cl.logger.Log(kgo.LogLevelError, "error committing offsets", logKeyError, err)
				time.Sleep(c.cl.suspendCommittingTimeout)
			} else {
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

		select {
		case <-c.cl.exitCh:
			return
		default:
			if err := c.handleRecords(ctx, records, handler); err != nil {
				c.ackRecordsEternal(ctx, records, true)
				return
			}
			c.ackRecordsEternal(ctx, records, false)
		}
	}
}

func (c *Client) ackRecordsEternal(ctx context.Context, records []*kgo.Record, isError bool) {
	var hasRelease bool

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
	}

	if hasRelease && c.cl.shareReleaseTimeout > 0 {
		timer := time.NewTimer(c.cl.shareReleaseTimeout)
		select {
		case <-ctx.Done():
		case <-c.cl.exitCh:
		case <-timer.C:
		}
		timer.Stop()
	}

	c.flushAcksEternal(ctx)
}

func (c *Client) flushAcksEternal(ctx context.Context) {
	for {
		select {
		case <-c.cl.exitCh:
			return
		default:
			if err := c.conn.FlushAcks(ctx); err != nil {
				c.cl.stats.recordShareAckError()
				c.cl.logger.Log(kgo.LogLevelError, "error flushing share group acks", logKeyError, err)
				time.Sleep(c.cl.suspendCommittingTimeout)
				continue
			}

			return
		}
	}
}

func (c *Client) handleRecords(ctx context.Context, records []*kgo.Record, handler BatchHandlerFunc) (err error) {
	startTime := time.Now()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("kafka: batch handler panic: %v", r)
		}

		c.cl.stats.recordHandle(len(records), time.Since(startTime), err)

		if err != nil {
			c.cl.logger.Log(kgo.LogLevelError, "error handling records",
				logKeyError, err,
				logKeyRecords, c.cl.formatRecords(records...),
			)
		}
	}()

	return handler(ctx, records)
}
