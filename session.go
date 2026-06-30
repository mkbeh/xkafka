package xkafka

import (
	"context"
	"fmt"
	"time"

	"github.com/mkbeh/xkafka/internal/pkg/kslog"
	"github.com/twmb/franz-go/pkg/kgo"
)

// GroupTransactSession consumes records and produces records in the same Kafka transaction.
//
// It is intended for Kafka-to-Kafka consume-process-produce flows where consumed
// offsets and produced records must be committed atomically.
type GroupTransactSession struct {
	conn *kgo.GroupTransactSession
	cl   *client
}

// NewGroupTransactSession creates a Kafka group transaction session and verifies broker connectivity.
func NewGroupTransactSession(opts ...Opt) (*GroupTransactSession, error) {
	cl, err := newClient(opts...)
	if err != nil {
		return nil, err
	}

	conn, err := kgo.NewGroupTransactSession(cl.clientOps...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create group transact session: %w", err)
	}

	if err := conn.Client().Ping(context.Background()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("kafka: ping group transact session: %w", err)
	}

	cl.conn = conn

	g := &GroupTransactSession{
		conn: conn,
		cl:   cl,
	}

	// Bind the fetch handler after GroupTransactSession is created because the adapter
	// needs the session instance to produce records and commit offsets transactionally.
	if cl.groupHandleFetches != nil {
		cl.handleFetches = cl.groupHandleFetches(g)
	}

	return g, nil
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
	g.cl.Close()

	if g.conn == nil {
		return nil
	}

	g.conn.Close()

	return nil
}

func (g *GroupTransactSession) handleFetchesBatch(handler BatchTxHandlerFunc) handleFetchesFunc {
	return func(ctx context.Context, fetches kgo.Fetches) {
		records := fetches.Records()
		if len(records) == 0 {
			return
		}

		startTime := time.Now()

		committed, handleErr, txErr := g.handleBatchInTx(ctx, records, handler)

		for _, record := range records {
			g.cl.metrics.Consumer().CollectHandleProcessTiming(startTime, record.Topic)
		}

		if txErr != nil {
			g.cl.logger.ErrorContext(ctx, "error handling group transaction",
				kslog.Error(txErr),
				kslog.Records(g.cl.formatRecords(records...)),
			)

			g.handleGroupTxBatchError(ctx, records)
			return
		}

		if handleErr != nil {
			g.cl.logger.ErrorContext(ctx, "error handling records in group transaction",
				kslog.Error(handleErr),
				kslog.Records(g.cl.formatRecords(records...)),
			)

			g.handleGroupTxBatchError(ctx, records)
			return
		}

		if !committed {
			g.cl.logger.InfoContext(ctx, "group transaction aborted before commit",
				kslog.ConsumerLabels(g.cl.labels),
			)

			return
		}

		g.cl.logger.DebugContext(ctx, "group transaction committed",
			kslog.Count(len(records)),
		)
	}
}

func (g *GroupTransactSession) handleBatchInTx(
	ctx context.Context,
	records []*kgo.Record,
	handler BatchTxHandlerFunc,
) (committed bool, handleErr, txErr error) {
	if err := g.conn.Begin(); err != nil {
		return false, nil, fmt.Errorf("kafka: begin group transaction: %w", err)
	}

	defer func() {
		if r := recover(); r != nil {
			handleErr = fmt.Errorf("kafka: batch handler panic: %v", r)
			committed, txErr = g.conn.End(ctx, kgo.TryAbort)
		}
	}()

	tx := &Tx{cl: g.cl}

	handleErr = handler(ctx, records, tx)

	endTry := kgo.TryCommit
	if handleErr != nil {
		endTry = kgo.TryAbort
	}

	committed, err := g.conn.End(ctx, endTry)
	if err != nil {
		return false, handleErr, fmt.Errorf("kafka: end group transaction: %w", err)
	}

	return committed, handleErr, nil
}

func (g *GroupTransactSession) handleGroupTxBatchError(ctx context.Context, records []*kgo.Record) {
	for _, record := range records {
		g.cl.metrics.Consumer().CollectHandleErrors(record.Topic)
	}

	timer := time.NewTimer(g.cl.suspendProcessingTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-g.cl.exitCh:
	case <-timer.C:
	}
}
