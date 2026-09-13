package xkafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// GroupTransactSession provides transactional Kafka consume-process-produce
// operations.
//
// Each batch processed by [GroupTransactSession.HandleFetches] runs inside a
// group transaction so produced records and consumed offsets can be committed
// atomically.
type GroupTransactSession struct {
	cl *client
}

// NewGroupTransactSession creates a GroupTransactSession configured with opts.
//
// A batch transaction handler configured with
// [WithGroupTransactSessionBatchHandler] is required. Kafka-to-Kafka EOS also
// requires a consumer group and transactional ID configured through
// [WithKafkaOptions].
func NewGroupTransactSession(opts ...Opt) (*GroupTransactSession, error) {
	cl, err := newClient(opts...)
	if err != nil {
		return nil, err
	}

	conn, err := kgo.NewGroupTransactSession(cl.kafkaOpts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create group transact session: %w", err)
	}

	cl.conn = conn
	cl.parseKafkaOptions(conn.Client())

	g := &GroupTransactSession{
		cl: cl,
	}

	if err := g.bindHandler(); err != nil {
		conn.Close()
		return nil, err
	}

	cl.hooks.onNewGroupTransactSession(g)

	return g, nil
}

// Name returns the logical session name configured with [WithName].
//
// It returns an empty string if no name was configured.
func (g *GroupTransactSession) Name() string {
	if g == nil || g.cl == nil {
		return ""
	}

	return g.cl.Name()
}

// Ping verifies that at least one Kafka broker is reachable.
func (g *GroupTransactSession) Ping(ctx context.Context) error {
	if err := g.cl.Client().Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping group transact session: %w", err)
	}

	return nil
}

// Produce enqueues record for asynchronous delivery using the session producer.
//
// Delivery and backpressure semantics are the same as [Client.Produce].
func (g *GroupTransactSession) Produce(ctx context.Context, record *kgo.Record, promise PromiseFunc) {
	g.cl.Produce(ctx, record, promise)
}

// TryProduce attempts to enqueue record without waiting for producer buffer
// space.
//
// Delivery semantics are the same as [Client.TryProduce].
func (g *GroupTransactSession) TryProduce(ctx context.Context, record *kgo.Record, promise PromiseFunc) {
	g.cl.TryProduce(ctx, record, promise)
}

// ProduceSync produces records and waits for all of them to complete.
//
// Error semantics are the same as [Client.ProduceSync].
func (g *GroupTransactSession) ProduceSync(ctx context.Context, records ...*kgo.Record) error {
	return g.cl.ProduceSync(ctx, records...)
}

// HandleFetches polls Kafka and processes each fetched batch inside a group
// transaction.
//
// If the handler succeeds, the session attempts to commit produced records and
// consumed offsets atomically. If the handler returns an error or panics, the
// session attempts to abort the transaction and waits for the configured
// suspension delay before continuing.
//
// Transaction begin or end errors terminate the fetch loop.
//
// HandleFetches runs until ctx is canceled, the session is shut down, or
// processing returns a terminal error. Only one HandleFetches call may run at
// a time.
func (g *GroupTransactSession) HandleFetches(ctx context.Context) error {
	return g.cl.HandleFetches(ctx)
}

// Shutdown stops polling and closes the underlying group transaction session.
//
// Shutdown is idempotent and safe to call concurrently. Only the first call
// performs shutdown.
func (g *GroupTransactSession) Shutdown(_ context.Context) error {
	if g == nil || g.cl == nil {
		return nil
	}

	closed := false

	g.cl.shutdownOnce.Do(func() {
		closed = true
		close(g.cl.exitCh)

		if g.cl.conn == nil {
			return
		}

		conn := g.cl.Session()
		if g.cl.blockRebalance {
			conn.CloseAllowingRebalance()
		} else {
			conn.Close()
		}
	})

	if closed {
		g.cl.hooks.onGroupTransactSessionClosed(g)
	}

	return g.cl.shutdownErr
}

func (g *GroupTransactSession) bindHandler() error {
	if g.cl.sessionHandler == nil {
		return errors.New("kafka: group transact session requires batch handler")
	}

	g.cl.handleFetches = g.handleFetchesBatch(g.cl.sessionHandler)

	return nil
}

func (g *GroupTransactSession) handleFetchesBatch(handler BatchTxHandlerFunc) handleFetchesFunc {
	return func(ctx context.Context, fetches kgo.Fetches) error {
		records := fetches.Records()
		if len(records) == 0 {
			return nil
		}

		if !g.cl.wait(ctx, 0) {
			return nil
		}

		committed, handleErr, txErr := g.handleRecordsInTx(ctx, records, handler)

		if err := errors.Join(txErr, handleErr); err != nil {
			if g.cl.logEnabled(kgo.LogLevelError) {
				g.cl.log(kgo.LogLevelError, "error handling group transaction",
					logKeyError, err,
					logKeyRecord, g.cl.formatRecord(records[0]),
					logKeyRecordCount, len(records),
				)
			}

			// Begin and End errors indicate transaction-level failures. Stop the
			// fetch loop rather than starting another transaction on the same
			// session.
			if txErr != nil {
				return err
			}

			// Back off after handler failures to avoid a tight redelivery loop.
			g.cl.wait(ctx, g.cl.suspendProcessingTimeout)

			return nil
		}

		if !committed {
			g.cl.log(kgo.LogLevelDebug, "group transaction aborted before commit",
				logKeyConsumerGroup, g.cl.consumerGroup,
			)
		}

		return nil
	}
}

func (g *GroupTransactSession) handleRecordsInTx(
	ctx context.Context,
	records []*kgo.Record,
	handler BatchTxHandlerFunc,
) (committed bool, handleErr, txErr error) {
	ctx = g.cl.hooks.onTransactionStart(ctx, TransactionTypeGroup)
	startTime := time.Now()

	defer func() {
		// A completed transaction that did not commit is reported as an abort.
		outcome := TransactionOutcomeAbort
		switch {
		case txErr != nil:
			outcome = TransactionOutcomeError
		case committed:
			outcome = TransactionOutcomeCommit
		}

		// Prefer a transaction error over the handler error for transaction hooks.
		hookErr := txErr
		if hookErr == nil {
			hookErr = handleErr
		}

		g.cl.hooks.onTransactionEnd(
			ctx,
			TransactionTypeGroup,
			outcome,
			time.Since(startTime),
			hookErr,
		)
	}()

	conn := g.cl.Session()

	if err := conn.Begin(); err != nil {
		txErr = fmt.Errorf("kafka: begin group transaction: %w", err)
		return
	}

	tx := &Tx{cl: g.cl}
	handleErr = g.handleRecords(ctx, records, tx, handler)

	endTry := kgo.TryCommit
	if handleErr != nil {
		endTry = kgo.TryAbort
	}

	committed, txErr = conn.End(ctx, endTry)
	if txErr != nil {
		txErr = fmt.Errorf("kafka: end group transaction: %w", txErr)
	}

	return
}

func (g *GroupTransactSession) handleRecords(
	ctx context.Context,
	records []*kgo.Record,
	tx *Tx,
	handler BatchTxHandlerFunc,
) (err error) {
	ctx = g.cl.hooks.onHandleStart(ctx, records)
	startTime := time.Now()

	defer func() {
		if r := recover(); r != nil {
			// Convert handler panics into handler errors so the transaction
			// follows the normal abort path.
			err = fmt.Errorf("kafka: batch handler panic: %v", r)
		}

		g.cl.hooks.onHandleEnd(ctx, records, time.Since(startTime), err)
	}()

	return handler(ctx, records, tx)
}
