package xkafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Tx provides producing operations within an active Kafka transaction.
//
// A Tx is provided to [TxFunc] and [BatchTxHandlerFunc]. Transaction commit and
// abort are managed by [Client.RunInTx] or [GroupTransactSession].
type Tx struct {
	cl *client
}

// Produce enqueues record for asynchronous delivery within the current
// transaction.
//
// Delivery and backpressure semantics are the same as [Client.Produce].
func (tx *Tx) Produce(ctx context.Context, record *kgo.Record, promise PromiseFunc) {
	tx.cl.Produce(ctx, record, promise)
}

// TryProduce attempts to enqueue record within the current transaction without
// waiting for producer buffer space.
//
// Delivery semantics are the same as [Client.TryProduce].
func (tx *Tx) TryProduce(ctx context.Context, record *kgo.Record, promise PromiseFunc) {
	tx.cl.TryProduce(ctx, record, promise)
}

// ProduceSync produces records within the current transaction and waits for all
// of them to complete.
//
// Error semantics are the same as [Client.ProduceSync].
func (tx *Tx) ProduceSync(ctx context.Context, records ...*kgo.Record) error {
	return tx.cl.ProduceSync(ctx, records...)
}
