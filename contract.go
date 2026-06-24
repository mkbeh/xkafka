package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type (
	TxFunc           func(ctx context.Context) error
	HandlerFunc      func(ctx context.Context, msg *kgo.Record) error
	BatchHandlerFunc func(ctx context.Context, msgs []*kgo.Record) error
)
