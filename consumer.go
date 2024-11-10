package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kslog "github.com/mkbeh/kafka/pkg/logger"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fetchesHandler = func(ctx context.Context, fetches kgo.Fetches)

type Consumer struct {
	enabled bool

	conn          *kgo.Client
	fmt           *kgo.RecordFormatter
	logger        *slog.Logger
	clientID      string
	handleFetches fetchesHandler
	exitCh        chan struct{}

	batchSize      int
	groupSpecified bool
	clientOptions  []kgo.Opt

	pollInterval             time.Duration
	suspendProcessingTimeout time.Duration
	suspendCommittingTimeout time.Duration

	pollTicker *time.Ticker
}

func NewConsumer(opts ...ConsumerOption) (*Consumer, error) {
	c := &Consumer{
		exitCh:                   make(chan struct{}),
		pollInterval:             time.Millisecond * 300,
		suspendProcessingTimeout: time.Second * 30,
		suspendCommittingTimeout: time.Second * 10,
	}

	for _, opt := range opts {
		opt.apply(c)
	}

	formatter, err := newFormatter()
	if err != nil {
		return nil, err
	}

	c.fmt = formatter
	c.logger = wrapLogger(c.logger, consumerComponentValue)

	if c.handleFetches == nil {
		return nil, fmt.Errorf("fetches handler must be set")
	}

	c.addClientOption(kgo.WithLogger(kslog.NewKgoAdapter(c.logger)))
	c.addClientOption(kgo.ClientID(c.clientID))

	return c, nil
}

func (c *Consumer) PreRun(ctx context.Context) error {
	if !c.enabled {
		return nil
	}

	conn, err := kgo.NewClient(c.clientOptions...)
	if err != nil {
		return err
	}

	if err := conn.Ping(ctx); err != nil {
		return err
	}

	c.conn = conn
	c.pollTicker = time.NewTicker(c.pollInterval)

	return nil
}

func (c *Consumer) Shutdown(_ context.Context) error {
	close(c.exitCh)

	if c.pollTicker != nil {
		c.pollTicker.Stop()
	}

	if c.conn != nil {
		c.conn.Close()
	}

	return nil
}

func (c *Consumer) Run(ctx context.Context) error {
	if !c.enabled {
		return nil
	}

	for range c.pollTicker.C {
		select {
		case <-c.exitCh:
			return nil
		default:
		}

		fetches := c.conn.PollRecords(ctx, c.batchSize)
		if fetches.IsClientClosed() {
			c.logger.InfoContext(ctx, "kafka client closed for topic(s)") // todo: add fields
			return nil
		}

		for _, fetchErr := range fetches.Errors() {
			c.logger.ErrorContext(ctx, "error fetching records", kslog.Error(fetchErr.Err))
		}

		c.handleFetches(ctx, fetches)
	}

	return nil
}

func (c *Consumer) handleFetchesBatch(handler BatchHandlerFunc) fetchesHandler {
	return func(ctx context.Context, fetches kgo.Fetches) {
		records := fetches.Records()
		if len(records) == 0 {
			return
		}

	infiniteLoop:
		for {
			select {
			case <-c.exitCh:
				return
			default:
				if err := handler(ctx, records); err != nil {
					c.logger.ErrorContext(ctx, "error handling records",
						kslog.Error(err),
						kslog.Records(c.formatRecords(records...)),
					)
					time.Sleep(c.suspendProcessingTimeout)
				} else {
					c.commitInternalOffsetsEternal(ctx)
					break infiniteLoop
				}
			}
		}
	}
}

func (c *Consumer) handleFetchesEach(handler HandlerFunc) fetchesHandler {
	handleUntilSuccessful := func(ctx context.Context, record *kgo.Record) {
	infiniteLoop:
		for {
			select {
			case <-c.exitCh:
				return
			default:
				if err := handler(ctx, record); err != nil {
					c.logger.ErrorContext(ctx, "error handling record",
						kslog.Error(err),
						kslog.Records(c.formatRecords(record)),
					)
					time.Sleep(c.suspendProcessingTimeout)
				} else {
					c.commitRecordOffsetEternal(ctx, record)
					break infiniteLoop
				}
			}
		}
	}

	return func(ctx context.Context, fetches kgo.Fetches) {
		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			select {
			case <-c.exitCh:
				return
			default:
				handleUntilSuccessful(ctx, record)
			}
		}
	}
}

func (c *Consumer) commitInternalOffsetsEternal(ctx context.Context) {
	if !c.groupSpecified {
		return
	}

infiniteLoop:
	for {
		select {
		case <-c.exitCh:
			return
		default:
			if err := c.conn.CommitUncommittedOffsets(ctx); err != nil {
				c.logger.ErrorContext(ctx, "error committing offsets", kslog.Error(err))
				time.Sleep(c.suspendCommittingTimeout)
			} else {
				c.logger.DebugContext(ctx, "offsets committed", kslog.Count(len(c.conn.CommittedOffsets())))
				break infiniteLoop
			}
		}
	}
}

func (c *Consumer) commitRecordOffsetEternal(ctx context.Context, record *kgo.Record) {
	if !c.groupSpecified {
		return
	}

infiniteLoop:
	for {
		select {
		case <-c.exitCh:
			return
		default:
			if err := c.conn.CommitRecords(ctx, record); err != nil {
				c.logger.ErrorContext(ctx, "error committing records", kslog.Error(err))
				time.Sleep(c.suspendCommittingTimeout)
			} else {
				c.logger.DebugContext(ctx, "record processed", kslog.Record(c.formatRecords(record)))
				break infiniteLoop
			}
		}
	}
}

func (c *Consumer) formatRecords(records ...*kgo.Record) []byte {
	buff := make([]byte, 0)

	for _, record := range records {
		buff = c.fmt.AppendRecord(buff, record)
	}

	return buff
}

func (c *Consumer) addClientOption(opt kgo.Opt) {
	c.clientOptions = append(c.clientOptions, opt)
}
