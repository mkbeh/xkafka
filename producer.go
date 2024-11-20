package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/mkbeh/kafka/internal/pkg/kprom"
	"github.com/mkbeh/kafka/internal/pkg/kslog"
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
	p.logger = wrapLogger(p.logger, producerComponentValue)

	instrumenting := kotel.NewKotel(
		kotel.WithMeter(kotel.NewMeter(p.meterOptions...)),
	)

	prom := kprom.NewMetrics(p.id, kprom.ProducerKind, "")

	p.addClientOptions(
		kgo.WithLogger(kslog.NewKgoAdapter(p.logger)),
		kgo.WithHooks(instrumenting.Hooks(), prom),
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

func (p *Producer) RunInTx(ctx context.Context, record ...*kgo.Record) error {
	if err := p.conn.BeginTransaction(); err != nil {
		return err
	}

	if err := p.produce(ctx, record...); err != nil {
		p.rollback(ctx)
		return err
	}

	if err := p.conn.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return err
	}

	return nil
}

func (p *Producer) produce(ctx context.Context, records ...*kgo.Record) error {
	results := p.conn.ProduceSync(ctx, records...)
	for _, r := range results {
		if r.Err != nil {
			p.logger.ErrorContext(ctx, "error produce message sync", kslog.Error(r.Err))
		}
	}

	return results.FirstErr()
}

func (p *Producer) rollback(ctx context.Context) {
	if err := p.conn.AbortBufferedRecords(ctx); err != nil { // this only happens if ctx is canceled
		p.logger.ErrorContext(ctx, "error aborting buffered records", kslog.Error(err))
		return
	}
	if err := p.conn.EndTransaction(ctx, kgo.TryAbort); err != nil {
		p.logger.ErrorContext(ctx, "error rolling back transaction", kslog.Error(err))
		return
	}
}

func (p *Producer) loggingPromise(record *kgo.Record, err error) {
	var ctx context.Context
	if record.Context == nil {
		ctx = context.Background()
	} else {
		ctx = record.Context
	}
	if err != nil {
		p.logger.ErrorContext(ctx, "kafka async producer error",
			kslog.Error(err),
			kslog.Record(p.fmt.AppendRecord(nil, record)),
		)
	}
}

func (p *Producer) addClientOptions(opts ...kgo.Opt) {
	p.clientOptions = append(p.clientOptions, opts...)
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

func newFormatter() (*kgo.RecordFormatter, error) {
	return kgo.NewRecordFormatter("topic: %t, key: %k, msg: %v")
}
