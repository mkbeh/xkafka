package xkafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type (
	// TxFunc handles work inside a Kafka transaction.
	TxFunc func(ctx context.Context, tx *Tx) error

	// PromiseFunc is called when an asynchronous produce request completes.
	PromiseFunc func(record *kgo.Record, err error)

	// BatchHandlerFunc handles a batch of consumed Kafka records.
	BatchHandlerFunc func(ctx context.Context, records []*kgo.Record) error

	// BatchTxHandlerFunc handles a batch of consumed Kafka records inside a group transaction.
	BatchTxHandlerFunc func(ctx context.Context, records []*kgo.Record, tx *Tx) error
)
