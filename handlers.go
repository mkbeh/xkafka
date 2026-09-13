package xkafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type (
	// TxFunc performs work inside a producer transaction.
	//
	// Returning nil allows [Client.RunInTx] to attempt to commit the transaction.
	// Returning an error causes [Client.RunInTx] to attempt to abort the
	// transaction.
	TxFunc func(ctx context.Context, tx *Tx) error

	// PromiseFunc is called when an asynchronous produce operation completes.
	//
	// err is nil when the record was produced successfully and non-nil when
	// delivery failed.
	PromiseFunc func(record *kgo.Record, err error)

	// BatchHandlerFunc processes a batch of consumed Kafka records.
	//
	// Returning nil signals successful processing. For regular consumers,
	// returning an error triggers the configured retry behavior. For Share
	// Groups, returning an error applies release or rejection semantics. Panics
	// are recovered and handled as errors.
	BatchHandlerFunc func(ctx context.Context, records []*kgo.Record) error

	// BatchTxHandlerFunc processes a batch of consumed Kafka records inside a
	// group transaction.
	//
	// Produced records should be written through tx. Returning nil allows
	// [GroupTransactSession] to attempt to commit the produced records together
	// with the consumed offsets. Returning an error causes an abort to be
	// attempted. Panics are recovered and handled as errors.
	BatchTxHandlerFunc func(ctx context.Context, records []*kgo.Record, tx *Tx) error
)
