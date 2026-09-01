package xkafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// GroupTransactSession consumes records and produces records in the same Kafka transaction.
//
// It is intended for Kafka-to-Kafka consume-process-produce flows where consumed
// offsets and produced records must be committed atomically.
type GroupTransactSession struct {
	cl      *client
	metrics MetricsRegistration
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
	cl.applyKafkaOptions(conn.Client())

	g := &GroupTransactSession{
		cl: cl,
	}

	// Bind the fetch handler after GroupTransactSession is created because the adapter
	// needs the session instance to produce records and commit offsets transactionally.
	if cl.groupHandleFetches != nil {
		cl.handleFetches = cl.groupHandleFetches(g)
	}

	if err := g.registerMetrics(cl.metrics); err != nil {
		conn.Close()
		return nil, fmt.Errorf("kafka: register group transact session metrics: %w", err)
	}

	return g, nil
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

func (g *GroupTransactSession) Ping(ctx context.Context) error {
	if g == nil || g.cl == nil || g.cl.conn == nil {
		return fmt.Errorf("kafka: group transact session is nil")
	}

	if err := g.cl.Client().Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping group transact session: %w", err)
	}

	return nil
}

// Stats returns a snapshot of the current session statistics.
func (g *GroupTransactSession) Stats() Stats {
	if g == nil || g.cl == nil {
		return Stats{}
	}

	return g.cl.stats.snapshot()
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

	g.cl.Close()

	if g.metrics != nil {
		g.metrics.Close()
	}

	if g.cl.conn != nil {
		g.cl.Session().Close()
	}

	return nil
}

func (g *GroupTransactSession) registerMetrics(metrics Metrics) error {
	if metrics == nil {
		return nil
	}

	registration, err := metrics.Register(g)
	if err != nil {
		return err
	}

	g.metrics = registration
	return nil
}

func (g *GroupTransactSession) handleFetchesBatch(handler BatchTxHandlerFunc) handleFetchesFunc {
	return func(ctx context.Context, fetches kgo.Fetches) {
		records := fetches.Records()
		if len(records) == 0 {
			return
		}

		committed, handleErr, txErr := g.handleRecordsInTx(ctx, records, handler)

		if txErr != nil {
			g.cl.logger.Log(kgo.LogLevelError, "error handling group transaction",
				logKeyRecord, txErr,
				logKeyRecords, g.cl.formatRecords(records...),
			)

			g.handleTxError(ctx)
			return
		}

		if handleErr != nil {
			g.cl.logger.Log(kgo.LogLevelError, "error handling records in group transaction",
				logKeyError, handleErr,
				logKeyRecords, g.cl.formatRecords(records...),
			)

			g.handleTxError(ctx)
			return
		}

		if !committed {
			g.cl.logger.Log(kgo.LogLevelDebug, "group transaction aborted before commit",
				logKeyConsumerGroup, g.cl.consumerGroup,
			)

			return
		}
	}
}

func (g *GroupTransactSession) handleRecordsInTx(
	ctx context.Context,
	records []*kgo.Record,
	handler BatchTxHandlerFunc,
) (committed bool, handleErr, txErr error) {
	conn := g.cl.Session()
	transactionStart := time.Now()

	defer func() {
		g.cl.stats.recordGroupTransaction(committed, txErr, time.Since(transactionStart))
	}()

	if err := conn.Begin(); err != nil {
		return false, nil, fmt.Errorf("kafka: begin group transaction: %w", err)
	}

	handleStart := time.Now()

	defer func() {
		if r := recover(); r != nil {
			handleErr = fmt.Errorf("kafka: batch handler panic: %v", r)
			committed, txErr = conn.End(ctx, kgo.TryAbort)
		}

		g.cl.stats.recordHandle(len(records), time.Since(handleStart), handleErr)
	}()

	tx := &Tx{cl: g.cl}

	handleErr = handler(ctx, records, tx)

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

func (g *GroupTransactSession) handleTxError(ctx context.Context) {
	timer := time.NewTimer(g.cl.suspendProcessingTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-g.cl.exitCh:
	case <-timer.C:
	}
}
