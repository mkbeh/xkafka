package xkafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// GroupTransactSession consumes records and produces records in the same Kafka transaction.
//
// It is intended for Kafka-to-Kafka consume-process-produce flows where consumed
// offsets and produced records must be committed atomically.
type GroupTransactSession struct {
	cl *client
}

// NewGroupTransactSession creates a Kafka group transaction session.
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

func (g *GroupTransactSession) bindHandler() error {
	if g.cl.sessionHandler == nil {
		return errors.New("kafka: group transact session requires batch handler")
	}

	g.cl.handleFetches = g.handleFetchesBatch(g.cl.sessionHandler)

	return nil
}

// Name returns the logical session name configured with WithName.
func (g *GroupTransactSession) Name() string {
	if g == nil || g.cl == nil {
		return ""
	}

	return g.cl.Name()
}

// Label returns one session label without allocating a copy of all labels.
func (g *GroupTransactSession) Label(key string) (string, bool) {
	if g == nil || g.cl == nil {
		return "", false
	}

	return g.cl.Label(key)
}

// Labels returns a detached copy of the session labels.
func (g *GroupTransactSession) Labels() map[string]string {
	if g == nil || g.cl == nil {
		return nil
	}

	return g.cl.Labels()
}

// ConsumerGroup returns the configured consumer group.
func (g *GroupTransactSession) ConsumerGroup() string {
	if g == nil || g.cl == nil {
		return ""
	}

	return g.cl.consumerGroup
}

func (g *GroupTransactSession) Ping(ctx context.Context) error {
	if g == nil || g.cl == nil || g.cl.conn == nil {
		return fmt.Errorf("kafka: group transact session is nil")
	}

	if err := g.cl.Client().Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping group transact session: %w", err)
	}

	return nil
}

func (g *GroupTransactSession) Produce(ctx context.Context, record *kgo.Record, promise PromiseFunc) {
	g.cl.Produce(ctx, record, promise)
}

func (g *GroupTransactSession) TryProduce(ctx context.Context, record *kgo.Record, promise PromiseFunc) {
	g.cl.TryProduce(ctx, record, promise)
}

func (g *GroupTransactSession) ProduceSync(ctx context.Context, records ...*kgo.Record) error {
	return g.cl.ProduceSync(ctx, records...)
}

func (g *GroupTransactSession) HandleFetches(ctx context.Context) error {
	return g.cl.HandleFetches(ctx)
}

// Shutdown stops polling and closes the group transaction session.
func (g *GroupTransactSession) Shutdown(_ context.Context) error {
	if g.cl == nil {
		return nil
	}

	didShutdown := false
	defer func() {
		if didShutdown {
			g.cl.hooks.onGroupTransactSessionClosed(g)
		}
	}()

	g.cl.shutdownOnce.Do(func() {
		didShutdown = true
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

	return g.cl.shutdownErr
}

func (g *GroupTransactSession) handleFetchesBatch(handler BatchTxHandlerFunc) handleFetchesFunc {
	return func(ctx context.Context, fetches kgo.Fetches) {
		records := fetches.Records()
		if len(records) == 0 {
			return
		}

		committed, handleErr, txErr := g.handleRecordsInTx(ctx, records, handler)

		if txErr != nil {
			g.cl.log(kgo.LogLevelError, "error handling group transaction",
				logKeyError, txErr,
				logKeyRecord, g.cl.formatRecord(records[0]),
				logKeyRecordCount, len(records),
			)

			g.cl.wait(ctx, g.cl.suspendProcessingTimeout)
			return
		}

		if handleErr != nil {
			g.cl.log(kgo.LogLevelError, "error handling records in group transaction",
				logKeyError, handleErr,
				logKeyRecord, g.cl.formatRecord(records[0]),
				logKeyRecordCount, len(records),
			)

			g.cl.wait(ctx, g.cl.suspendProcessingTimeout)
			return
		}

		if !committed {
			g.cl.log(kgo.LogLevelDebug, "group transaction aborted before commit",
				logKeyConsumerGroup, g.cl.consumerGroup,
			)
		}
	}
}

func (g *GroupTransactSession) handleRecordsInTx(
	ctx context.Context,
	records []*kgo.Record,
	handler BatchTxHandlerFunc,
) (committed bool, handleErr, txErr error) {
	ctx = g.cl.hooks.onTransactionStart(ctx, TransactionTypeGroup)
	conn := g.cl.Session()
	transactionStart := time.Now()

	defer func() {
		outcome := TransactionOutcomeAbort
		switch {
		case txErr != nil:
			outcome = TransactionOutcomeError
		case committed:
			outcome = TransactionOutcomeCommit
		}

		hookErr := txErr
		if hookErr == nil {
			hookErr = handleErr
		}

		g.cl.hooks.onTransactionEnd(
			ctx,
			TransactionTypeGroup,
			outcome,
			time.Since(transactionStart),
			hookErr,
		)
	}()

	if err := conn.Begin(); err != nil {
		return false, nil, fmt.Errorf("kafka: begin group transaction: %w", err)
	}

	tx := &Tx{cl: g.cl}

	handleErr = g.handleRecords(ctx, records, tx, handler)

	endTry := kgo.TryCommit
	if handleErr != nil {
		endTry = kgo.TryAbort
	}

	committed, err := conn.End(ctx, endTry)
	if err != nil {
		return false, handleErr, fmt.Errorf("kafka: end group transaction: %w", err)
	}

	return committed, handleErr, nil
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
			err = fmt.Errorf("kafka: batch handler panic: %v", r)
		}

		g.cl.hooks.onHandleEnd(ctx, records, time.Since(startTime), err)
	}()

	return handler(ctx, records, tx)
}
