# otelxkafka

`otelxkafka` is an OpenTelemetry instrumentation package for
[xkafka](https://github.com/mkbeh/xkafka). It provides
[tracing](https://pkg.go.dev/go.opentelemetry.io/otel/trace) and
[metrics](https://pkg.go.dev/go.opentelemetry.io/otel/metric) through
[xkafka.Hook](https://pkg.go.dev/github.com/mkbeh/xkafka#Hook)
implementations. With `otelxkafka`, you can trace synchronous produce operations
and consumer batch processing, propagate trace context through Kafka records, and
collect runtime metrics for processing, errors, settlements, Share Group
acknowledgements, and transactions.

`otelxkafka` can be used alongside
[franz-go/plugin/kotel](https://github.com/twmb/franz-go/tree/master/plugin/kotel)
to collect native franz-go client metrics. When `otelxkafka.Tracer` is enabled,
do not also register the franz-go tracer. See the usage sections below and
the [OpenTelemetry documentation](https://opentelemetry.io/docs) for more
information.

## Tracing

`Tracer` provides OpenTelemetry tracing for xkafka. It creates spans for
synchronous produce operations, consumer batch processing, settlements, and
transactions, and propagates trace context through Kafka records.

### How it works

The `otelxkafka` tracer uses hooks to automatically create and close `send`,
`process`, settlement, and transaction spans as records flow through xkafka.
`ProduceSync` is traced as a single `send` operation, while `Produce` and
`TryProduce` only propagate trace context into Kafka records without creating
`send` spans. Consumer `process` spans link to unique sampled message creation contexts
propagated through the records in each batch. Offset commit and Share Group
acknowledgement flush attempts are recorded as settlement spans after they
complete.

The following table provides a visual representation of the tracer hook
lifecycle:

| Hook                   | Operation   | State  |
|------------------------|-------------|--------|
| `HookProduceStart`     | Send        | Start  |
| `HookProduceRecord`    | Propagation | Inject |
| `HookProduceEnd`       | Send        | End    |
| `HookHandleStart`      | Process     | Start  |
| `HookHandleEnd`        | Process     | End    |
| `HookOffsetCommit`     | Commit      | Record |
| `HookShareAckFlush`    | Ack         | Record |
| `HookTransactionStart` | Transaction | Start  |
| `HookTransactionEnd`   | Transaction | End    |

### Getting started

To start using `otelxkafka` for tracing, you will need to:

1. Set up a tracer provider.
2. Configure any desired tracer options.
3. Create a new `otelxkafka` tracer.
4. Create a new `otelxkafka` service.
5. Create a new xkafka client and pass in its hooks.

Here's an example of how you might do this:

<!-- @formatter:off -->

```go
// Initialize tracer provider.
tracerProvider, err := initTracerProvider()

// Create a new otelxkafka tracer.
tracerOpts := []otelxkafka.TracerOpt{
    otelxkafka.TracerProvider(tracerProvider),
    otelxkafka.TracerPropagator(
        propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}),
    ),
}
tracer := otelxkafka.NewTracer(tracerOpts...)

// Pass the tracer to NewKotel hook.
telemetryOpts := []otelxkafka.Opt{
    otelxkafka.WithTracer(tracer),
}

// Create a new otelxkafka service.
telemetry := otelxkafka.NewKotel(telemetryOpts...)

// Create a new xkafka client.
client, err := xkafka.NewClient(
    // Pass in the otelxkafka hooks.
    xkafka.WithHooks(telemetry.Hooks()...),
    // ... other opts.
)
```

<!-- @formatter:on -->

### Sending records

`ProduceSync` automatically creates a `send` span and propagates its trace context
through the produced records. If the supplied context contains an active span,
the `send` span continues that trace.

Here's an example of how you might do this:

<!-- @formatter:off -->

```go
func produceHandler(client *xkafka.Client) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        record := &kgo.Record{
            Topic: "orders",
            Value: []byte("order created"),
        }

        // ProduceSync creates the send span automatically.
        // If r.Context() contains an active span, the send span becomes its child.
        if err := client.ProduceSync(r.Context(), record); err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }

        w.WriteHeader(http.StatusAccepted)
    }
}
```

<!-- @formatter:on -->

### Processing records

Consumer processing is automatically traced by the registered `Tracer`. A `process` span is created for each batch
handler invocation and ended when the handler returns. Sampled message creation contexts propagated through the records
are linked to the `process` span rather than used as its parent.

Here is an example of how you might do this:

<!-- @formatter:off -->

```go
func processRecords(ctx context.Context, records []*kgo.Record) error {
    // The process span is created automatically, and ctx carries its trace context.
    for _, record := range records {
        fmt.Printf(
            "processed offset '%d' with key '%s' and value '%s'\n",
            record.Offset,
            string(record.Key),
            string(record.Value),
        )
    }

    // Optionally pass ctx to the next processing step.
	
    return nil
}
```

<!-- @formatter:on -->

## Metrics

`Meter` provides OpenTelemetry metrics for xkafka. It tracks runtime metrics
related to record processing, produce and fetch errors, settlements, Share Group
acknowledgements, and transactions. These metrics are counters and histograms tracked under the
following names:

| Metric                                | Type      | Description                                                                           |
| ------------------------------------- | --------- | ------------------------------------------------------------------------------------- |
| **Producing & Fetching**              |           |                                                                                       |
| `xkafka.produce.errors`               | Counter   | Failed produce records (`topic`, `error_type`).                                       |
| `xkafka.fetch.errors`                 | Counter   | Kafka fetch errors (`topic`, `partition`, `recoverable`, `error_type`).               |
| **Processing**                        |           |                                                                                       |
| `messaging.process.duration`          | Histogram | Handler processing duration (`topic`, `error_type`).                                  |
| `xkafka.handler.records`              | Counter   | Records successfully processed by the handler (`topic`).                              |
| **Settlements & Share Groups**        |           |                                                                                       |
| `messaging.client.operation.duration` | Histogram | Offset commit or Share Group ack duration (`operation`: `commit\|ack`, `error_type`). |
| `xkafka.share.ack.records`            | Counter   | Share Group ack outcomes (`outcome`: `accept\|release\|reject`).                      |
| **Transactions**                      |           |                                                                                       |
| `xkafka.transaction.duration`         | Histogram | Transaction attempt duration (`type`, `outcome`, `error_type`).                       |

### Getting started

To start using `otelxkafka` for metrics, you will need to:

1. Set up a meter provider.
2. Configure any desired meter options.
3. Create a new `otelxkafka` meter.
4. Create a new `otelxkafka` service.
5. Create a new xkafka client and pass in its hooks.

Here's an example of how you might do this:

<!-- @formatter:off -->

```go
// Initialize meter provider.
meterProvider, err := initMeterProvider()

// Create a new otelxkafka meter.
meterOpts := []otelxkafka.MeterOpt{
    otelxkafka.MeterProvider(meterProvider),
}
meter := otelxkafka.NewMeter(meterOpts...)

// Pass the meter to NewKotel hook.
telemetryOpts := []otelxkafka.Opt{
    otelxkafka.WithMeter(meter),
}

// Create a new otelxkafka service.
telemetry := otelxkafka.NewKotel(telemetryOpts...)

// Create a new xkafka client.
client, err := xkafka.NewClient(
    // Pass in the otelxkafka hooks.
    xkafka.WithHooks(telemetry.Hooks()...),
    // ... other opts.
)
```

<!-- @formatter:on -->
