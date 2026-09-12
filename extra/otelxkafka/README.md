# otelxkafka

`otelxkafka` is an OpenTelemetry instrumentation package for
[xkafka](https://github.com/mkbeh/xkafka). It provides
[tracing](https://pkg.go.dev/go.opentelemetry.io/otel/trace) and
[metrics](https://pkg.go.dev/go.opentelemetry.io/otel/metric) through
[xkafka.Hook](https://pkg.go.dev/github.com/mkbeh/xkafka#Hook)
implementations. With `otelxkafka`, you can trace synchronous produce operations,
consumer batch processing, offset commits, Share Group acknowledgement flushes,
and transactions, propagate trace context through Kafka records, and collect
runtime metrics for producing, fetching, processing, settlements, Share Group
acknowledgements, and transactions.

`otelxkafka` can be used alongside
[franz-go/plugin/kotel](https://github.com/twmb/franz-go/tree/master/plugin/kotel)
to collect native franz-go client metrics. When using `otelxkafka.Tracer`,
configure franz-go instrumentation with metrics only to avoid duplicate tracing.
See the usage sections below and the
[OpenTelemetry documentation](https://opentelemetry.io/docs) for more
information.

## Tracing

`Tracer` provides OpenTelemetry tracing for xkafka. It creates spans for
synchronous produce operations, consumer batch processing, offset commits,
Share Group acknowledgement flushes, and transactions, and propagates trace
context through Kafka records.

### How it works

The `otelxkafka` tracer uses hooks to automatically create and close `send`,
`process`, `commit`, `ack`, and `xkafka.transaction` spans as operations flow
through xkafka.

`ProduceSync` is traced as a single `send` operation, while `Produce` and
`TryProduce` only propagate trace context into Kafka records without creating
`send` spans. Consumer `process` spans link to unique sampled message creation
contexts extracted from Kafka record headers. Offset commit and Share Group
acknowledgement flush attempts are recorded as `commit` and `ack` spans after
the operations complete.

The following table provides a visual representation of the tracer hook
lifecycle:

| Hook                   | Operation            | State  |
| ---------------------- | -------------------- | ------ |
| `HookProduceStart`     | `send`               | Start  |
| `HookProduceRecord`    | Propagation          | Inject |
| `HookProduceEnd`       | `send`               | End    |
| `HookHandleStart`      | `process`            | Start  |
| `HookHandleEnd`        | `process`            | End    |
| `HookOffsetCommit`     | `commit`             | Record |
| `HookShareAckFlush`    | `ack`                | Record |
| `HookTransactionStart` | `xkafka.transaction` | Start  |
| `HookTransactionEnd`   | `xkafka.transaction` | End    |

### Getting started

To start using `otelxkafka` for tracing, you will need to:

1. Set up a tracer provider.
2. Configure any desired tracer options.
3. Create a new `otelxkafka` tracer.
4. Create a new `otelxkafka.Kotel`.
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

// Create a new otelxkafka Kotel.
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
the `send` span becomes its child.

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

Consumer processing is automatically traced by the registered `Tracer`. A
`process` span is created for each batch handler invocation and ended when the
handler returns. Sampled message creation contexts extracted from Kafka record
headers are linked to the `process` span rather than used as its parent.

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

`Meter` provides OpenTelemetry metrics for xkafka. It records runtime metrics
for producing, fetching, handler processing, offset commits, Share Group
acknowledgements, and transactions. These metrics are counters and histograms
recorded under the following names:

| Metric                                | Type      | Description                                                                                       |
| ------------------------------------- | --------- | ------------------------------------------------------------------------------------------------- |
| **Producing and fetching**            |           |                                                                                                   |
| `xkafka.produce.errors`               | Counter   | Failed produce records (`topic`, `error.type`).                                                   |
| `xkafka.fetch.errors`                 | Counter   | Kafka fetch errors (`topic`, `partition`, `recoverable`, `error.type`).                           |
| **Processing**                        |           |                                                                                                   |
| `messaging.process.duration`          | Histogram | Handler processing duration (`topic`, `error.type`).                                              |
| `xkafka.handler.records`              | Counter   | Successfully processed records (`topic`).                                                         |
| **Settlements and Share Groups**      |           |                                                                                                   |
| `messaging.client.operation.duration` | Histogram | Offset commit or Share Group acknowledgement duration (`operation`: `commit\|ack`, `error.type`). |
| `xkafka.share.ack.records`            | Counter   | Share Group acknowledgement outcomes (`outcome`: `accept\|release\|reject`).                      |
| **Transactions**                      |           |                                                                                                   |
| `xkafka.transaction.duration`         | Histogram | Transaction attempt duration (`type`, `outcome`, `error.type`).                                   |

Common attributes such as the client ID, consumer group or Share Group, and
custom labels are included when configured.

### Getting started

To start using `otelxkafka` for metrics, you will need to:

1. Set up a meter provider.
2. Configure any desired meter options.
3. Create a new `otelxkafka` meter.
4. Create a new `otelxkafka.Kotel`.
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

// Create a new otelxkafka Kotel.
telemetry := otelxkafka.NewKotel(telemetryOpts...)

// Create a new xkafka client.
client, err := xkafka.NewClient(
    // Pass in the otelxkafka hooks.
    xkafka.WithHooks(telemetry.Hooks()...),
    // ... other opts.
)
```

<!-- @formatter:on -->
