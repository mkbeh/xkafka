# OpenTelemetry Observability Example

This example combines native franz-go telemetry with xkafka runtime telemetry in one runnable setup.

**This example demonstrates:**

* exporting native Kafka client metrics through `franz-go/plugin/kotel`;
* exporting xkafka runtime metrics through `extra/otelxkafka` hooks;
* tracing franz-go record operations and xkafka handler/settlement operations;
* sharing one OpenTelemetry `MeterProvider` and `TracerProvider` across both instrumentation layers;
* sharing one trace propagator across both tracers;
* exposing metrics through a Prometheus `/metrics` endpoint;
* exporting spans and Kafka/xkafka logs to stderr;
* producing and consuming a record to generate telemetry.

The observability layers are independent and complementary:

```text
xkafka hooks ───── otelxkafka.Meter ──┐
                                      ├── OTel MeterProvider ── Prometheus /metrics
franz-go hooks ─── kotel.Meter ────────┘

xkafka hooks ───── otelxkafka.Tracer ──┐
                                       ├── OTel TracerProvider ── stderr
franz-go hooks ─── kotel.Tracer ────────┘

xkafka + franz-go logs ── WithLogger ─────────────────────────── stderr
```

`kotel.Meter` owns native franz-go client metrics such as broker, byte, and record telemetry. `otelxkafka.Meter` owns wrapper-level behavior such as handler processing, settlement operations, Share Group acknowledgement outcomes, and transactions.

`kotel.Tracer` instruments Kafka record produce/receive operations and propagates trace context through record headers. `otelxkafka.Tracer` adds xkafka process, settlement, and transaction spans on top of that record-level trace context.

OpenTelemetry instrumentation and logging are opt-in. The example attaches xkafka telemetry with `WithHooks`, native franz-go telemetry with `kgo.WithHooks` through `WithKafkaOptions`, and logging explicitly through `WithLogger`.

## Local Kafka setup

From the repository root:

```shell
docker compose -f examples/docker-compose.yml up -d
```

Or from this example directory:

```shell
docker compose -f ../docker-compose.yml up -d
```

Kafka is available at:

```text
localhost:29092
```

Redpanda Console is available at:

```text
http://localhost:18080
```

## Run

From this directory:

```shell
go run .
```

Or from the repository root:

```shell
go run ./examples/otel
```

The HTTP server listens on:

```text
http://localhost:8080
```

Prometheus metrics are available at:

```text
http://localhost:8080/metrics
```

Traces, Kafka/xkafka logs, and the example's consume output are written to stderr.

## Produce and consume

Produce one record:

```shell
curl -i -X POST 'http://localhost:8080/produce'
```

Expected response:

```text
HTTP 204
```

The same client consumes the record and logs it to stderr:

```text
consume: topic=sample-otel-topic partition=0 offset=0 key="otel" msg={ID:42 Text:hello from xkafka otel example}
```

Partition and offset values depend on the Kafka topic state.

## Metrics

The example uses one `MeterProvider` for both instrumentation layers:

```go
kafkaTelemetry := kotel.NewKotel(
    kotel.WithMeter(
        kotel.NewMeter(
            kotel.MeterProvider(meterProvider),
        ),
    ),
)

xkafkaTelemetry := otelxkafka.NewKotel(
    otelxkafka.WithMeter(
        otelxkafka.NewMeter(
            otelxkafka.MeterProvider(meterProvider),
        ),
    ),
)
```

The xkafka instrumentation exports:

```text
xkafka.produce.errors
xkafka.fetch.errors
messaging.process.duration
xkafka.handler.records
messaging.client.operation.duration
xkafka.share.ack.records
xkafka.transaction.duration
```

For regular consumer processing, the most useful pair is:

```text
messaging.process.duration
    handler attempt duration by consumer group and error.type

xkafka.handler.records
    successfully processed records by topic and consumer group
```

`messaging.client.operation.duration` is used for settlement operations such as offset `commit` and Share Group `ack`. `xkafka.transaction.duration` records xkafka transaction attempts by transaction type and outcome.

`WithName("otel")` is exported as `messaging.client.id`. The consumer group is exported as `messaging.consumer.group.name`.

Prometheus normalizes OpenTelemetry metric names for exposition, so dots in instrument names appear as underscores in `/metrics`.

## Tracing

The example configures both tracers with the same propagator:

```go
propagator := propagation.NewCompositeTextMapPropagator(
    propagation.TraceContext{},
    propagation.Baggage{},
)
```

Native franz-go telemetry is attached through:

```go
kgo.WithHooks(kafkaTelemetry.Hooks()...)
```

xkafka runtime telemetry is attached separately through:

```go
xkafka.WithHooks(xkafkaTelemetry.Hooks()...)
```

`kotel.Tracer` creates Kafka record produce/receive spans and propagates trace context through record headers. `otelxkafka.Tracer` uses that creation context for handler process links and adds settlement and transaction spans where xkafka owns the higher-level operation.

## Logging

The example enables one logger for both xkafka and franz-go:

```go
xkafka.WithLogger(
    kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil),
)
```

Logging is disabled by default unless `WithLogger` is configured.

## Stop

Press `Ctrl+C` to stop the HTTP server and Kafka client and shut down the OpenTelemetry `MeterProvider` and `TracerProvider`.
