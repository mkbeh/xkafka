package kafka

import (
	"context"
	"fmt"
	"log/slog"

	kslog "github.com/mkbeh/kafka/pkg/logger"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	logger   *slog.Logger
	clientID string

	conn        *kgo.Client
	fmt         *kgo.RecordFormatter
	promiseFunc func(*kgo.Record, error)

	clientOptions []kgo.Opt
}

func NewProducer(opts ...ProducerOption) (*Producer, error) {
	c := new(Producer)

	for _, opt := range opts {
		opt.apply(c)
	}

	if c.promiseFunc == nil {
		c.promiseFunc = c.loggingPromise
	}

	formatter, err := newFormatter()
	if err != nil {
		return nil, err
	}

	c.fmt = formatter
	c.logger = wrapLogger(c.logger, producerComponentValue)

	c.addClientOption(kgo.WithLogger(kslog.NewKgoAdapter(c.logger)))
	c.addClientOption(kgo.ClientID(c.clientID))

	c.conn, err = kgo.NewClient(c.clientOptions...)
	if err != nil {
		return nil, err
	}

	if err := c.conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}

	return c, nil
}

func (c *Producer) Close(ctx context.Context) {
	if c.conn == nil {
		return
	}

	defer c.conn.Close()

	err := c.conn.Flush(ctx)
	if err != nil {
		c.logger.ErrorContext(ctx, "producer flush error", kslog.Error(err))
	}
}

func (c *Producer) ProduceAsync(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	if promise == nil {
		c.conn.Produce(ctx, record, c.promiseFunc)
	} else {
		c.conn.Produce(ctx, record, promise)
	}
}

func (c *Producer) ProduceSync(ctx context.Context, records ...*kgo.Record) error {
	return c.produce(ctx, records...)
}

func (c *Producer) RunInTx(ctx context.Context, record ...*kgo.Record) error {
	if err := c.conn.BeginTransaction(); err != nil {
		return err
	}

	if err := c.produce(ctx, record...); err != nil {
		c.rollback(ctx)
		return err
	}

	if err := c.conn.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return err
	}

	return nil
}

func (c *Producer) produce(ctx context.Context, records ...*kgo.Record) error {
	var err error
	results := c.conn.ProduceSync(ctx, records...)
	for _, r := range results {
		if r.Err != nil {
			err = r.Err
		}
	}

	// todo: add prometheus

	return err
}

func (c *Producer) rollback(ctx context.Context) {
	if err := c.conn.AbortBufferedRecords(ctx); err != nil { // this only happens if ctx is canceled
		c.logger.ErrorContext(ctx, "error aborting buffered records", kslog.Error(err))
		return
	}
	if err := c.conn.EndTransaction(ctx, kgo.TryAbort); err != nil {
		c.logger.ErrorContext(ctx, "error rolling back transaction", kslog.Error(err))
		return
	}
}

func (c *Producer) loggingPromise(rec *kgo.Record, err error) {
	var ctx context.Context
	if rec.Context == nil {
		ctx = context.Background()
	} else {
		ctx = rec.Context
	}
	if err != nil {
		c.logger.ErrorContext(ctx, "kafka async producer error",
			kslog.Error(err),
			kslog.Record(c.fmt.AppendRecord(nil, rec)),
		)
	} else {
		// todo: add prometheus
	}
}

func (c *Producer) addClientOption(opt kgo.Opt) {
	c.clientOptions = append(c.clientOptions, opt)
}
