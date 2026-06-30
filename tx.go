package xkafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Tx exposes producing operations inside a Kafka transaction.
type Tx struct {
	cl *client
}

// Produce asynchronously produces a record inside the current Kafka transaction.
func (tx *Tx) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	tx.cl.Produce(ctx, record, promise)
}

// TryProduce attempts to enqueue a record inside the current Kafka transaction without blocking on producer backpressure.
func (tx *Tx) TryProduce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	tx.cl.TryProduce(ctx, record, promise)
}

// ProduceSync synchronously produces records inside the current Kafka transaction.
func (tx *Tx) ProduceSync(ctx context.Context, records ...*kgo.Record) error {
	return tx.cl.ProduceSync(ctx, records...)
}
