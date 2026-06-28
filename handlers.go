package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type (
	TxFunc                        func(ctx context.Context) error
	HandlerFunc                   func(ctx context.Context, record *kgo.Record) error
	BatchHandlerFunc              func(ctx context.Context, records []*kgo.Record) error
	BatchGroupTransactSessionFunc func(ctx context.Context, records []*kgo.Record, session *GroupTransactSession) error
)

// fetchesHandler is an internal adapter used by consumer runtimes.
type fetchesHandler = func(ctx context.Context, fetches kgo.Fetches)
