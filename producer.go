package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/mkbeh/xkafka/internal/pkg/kprom"
	"github.com/mkbeh/xkafka/internal/pkg/kslog"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
)

type Producer struct {
	id            string
	conn          *kgo.Client
	fmt           *kgo.RecordFormatter
	logger        *slog.Logger
	promiseFunc   func(*kgo.Record, error)
	clientOptions []kgo.Opt
	meterOptions  []kotel.MeterOpt
	tracerOptions []kotel.TracerOpt
	metrics       *kprom.Metrics
	namespace     string
	labels        map[string]string
}

func NewProducer(opts ...ProducerOption) (*Producer, error) {
	p := new(Producer)

	p.addMeterOption(kotel.MeterProvider(otel.GetMeterProvider()))
	p.addTracerOption(kotel.TracerProvider(otel.GetTracerProvider()))

	for _, opt := range opts {
		opt.apply(p)
	}

	if p.promiseFunc == nil {
		p.promiseFunc = p.loggingPromise
	}

	formatter, err := newFormatter()
	if err != nil {
		return nil, err
	}

	p.fmt = formatter

	if p.logger == nil {
		p.logger = slog.Default()
	}
	p.logger.With(kslog.Component("kafka_producer"))

	instrumenting := kotel.NewKotel(
		kotel.WithMeter(kotel.NewMeter(p.meterOptions...)),
	)

	p.exposeMetrics()

	p.metrics = kprom.NewMetrics(p.namespace, "kafka", p.labels)

	p.clientOptions = append(p.clientOptions,
		kgo.WithLogger(kslog.NewKgoAdapter(p.logger)),
		kgo.WithHooks(instrumenting.Hooks(), p.metrics),
	)

	p.conn, err = kgo.NewClient(p.clientOptions...)
	if err != nil {
		return nil, err
	}

	if err := p.conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}

	return p, nil
}

func (p *Producer) Close(ctx context.Context) {
	if p.conn == nil {
		return
	}

	defer p.conn.Close()

	err := p.conn.Flush(ctx)
	if err != nil {
		p.logger.ErrorContext(ctx, "producer flush error", kslog.Error(err))
	}
}

func (p *Producer) ProduceAsync(ctx context.Context, record *kgo.Record) {
	p.conn.Produce(ctx, record, p.promiseFunc)
}

func (p *Producer) ProduceSync(ctx context.Context, records ...*kgo.Record) error {
	return p.produce(ctx, records...)
}

func (p *Producer) RunInTx(ctx context.Context, fn TxFunc) (err error) {
	startTime := time.Now()
	txOutcome := kprom.TransactionOutcomeError

	defer func() {
		if p.metrics != nil {
			p.metrics.CollectTransaction(startTime, txOutcome)
		}
	}()

	if fn == nil {
		return fmt.Errorf("kafka: transaction function is nil")
	}

	if err = p.conn.BeginTransaction(); err != nil {
		return fmt.Errorf("kafka: begin transaction: %w", err)
	}

	shouldAbort := true

	defer func() {
		if r := recover(); r != nil {
			p.logger.ErrorContext(ctx, "panic recovered in kafka transaction, aborting",
				slog.Any("error", r),
			)

			if shouldAbort {
				if abortErr := p.abortTransaction(ctx); abortErr != nil {
					p.logger.ErrorContext(ctx, "kafka transaction abort after panic failed",
						kslog.Error(abortErr),
					)
				}
			}

			// RunInTx is responsible only for transaction cleanup.
			// The original panic is re-thrown so callers can handle it with their own
			// recovery middleware and so programming errors are not silently converted
			// into regular transaction errors.
			panic(r)
		}

		if shouldAbort && err != nil {
			if abortErr := p.abortTransaction(ctx); abortErr != nil {
				err = fmt.Errorf("kafka: transaction failed: %w; abort failed: %v", err, abortErr)
			}
		}
	}()

	if err = fn(ctx); err != nil {
		return err
	}

	// EndTransaction does not flush buffered records by itself.
	// ProduceSync already waits for its records, but Flush keeps RunInTx safe
	// if fn used asynchronous or buffered produce calls.
	if err = p.conn.Flush(ctx); err != nil {
		return fmt.Errorf("kafka: flush buffered records: %w", err)
	}

	// Commit is a terminal transaction operation.
	// After this point, do not run the deferred abort for arbitrary commit errors.
	// franz-go allows TryAbort only for specific transaction errors.
	shouldAbort = false

	if err = p.conn.EndTransaction(ctx, kgo.TryCommit); err != nil {
		if errors.Is(err, kerr.OperationNotAttempted) || errors.Is(err, kerr.TransactionAbortable) {
			if abortErr := p.abortTransaction(ctx); abortErr != nil {
				return fmt.Errorf("kafka: commit failed: %w; abort also failed: %v", err, abortErr)
			}

			return fmt.Errorf("kafka: commit failed, transaction aborted: %w", err)
		}

		return fmt.Errorf("kafka: commit transaction: %w", err)
	}

	txOutcome = kprom.TransactionOutcomeCommit
	return nil
}

func (p *Producer) produce(ctx context.Context, records ...*kgo.Record) error {
	results := p.conn.ProduceSync(ctx, records...)
	for _, r := range results {
		if r.Err != nil {
			p.logger.ErrorContext(ctx, "error produce message sync", kslog.Error(r.Err))
			p.metrics.CollectProduceError(recordTopic(r.Record))
		}
	}

	return results.FirstErr()
}

func (p *Producer) abortTransaction(ctx context.Context) error {
	// AbortBufferedRecords is required before aborting a transaction so that
	// buffered records are not accidentally carried into the next transaction.
	if err := p.conn.AbortBufferedRecords(ctx); err != nil {
		p.logger.ErrorContext(ctx, "error aborting buffered records", kslog.Error(err))
		return fmt.Errorf("abort buffered records: %w", err)
	}

	if err := p.conn.EndTransaction(ctx, kgo.TryAbort); err != nil {
		p.logger.ErrorContext(ctx, "error rolling back transaction", kslog.Error(err))
		return fmt.Errorf("abort transaction: %w", err)
	}

	return nil
}

func (p *Producer) loggingPromise(record *kgo.Record, err error) {
	var ctx context.Context
	if record.Context == nil {
		ctx = context.Background()
	} else {
		ctx = record.Context
	}
	if err != nil {
		p.metrics.CollectProduceError(recordTopic(record))
		p.logger.ErrorContext(ctx, "kafka async producer error",
			kslog.Error(err),
			kslog.Record(p.fmt.AppendRecord(nil, record)),
		)
	}
}

func (p *Producer) getClientID() string {
	if p.id == "" {
		return uuid.New().String()
	}
	return p.id
}

func (p *Producer) addClientOption(opt kgo.Opt) {
	p.clientOptions = append(p.clientOptions, opt)
}

func (p *Producer) addMeterOption(opt kotel.MeterOpt) {
	p.meterOptions = append(p.meterOptions, opt)
}

func (p *Producer) addTracerOption(opt kotel.TracerOpt) {
	p.tracerOptions = append(p.tracerOptions, opt)
}

func (p *Producer) exposeMetrics() {
	if p.labels == nil {
		p.labels = make(map[string]string)
	}

	p.labels["client_id"] = p.getClientID()
	p.labels["client_kind"] = "producer"
}

func newFormatter() (*kgo.RecordFormatter, error) {
	return kgo.NewRecordFormatter("topic: %t, key: %k, msg: %v")
}

func recordTopic(record *kgo.Record) string {
	if record == nil {
		return ""
	}

	return record.Topic
}
