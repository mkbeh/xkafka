# OpenTelemetry Observability Example

This example exports wrapper-level `xkafka` metrics, native `franz-go` metrics, and Kafka traces through OpenTelemetry.

**This example demonstrates:**

* exporting lightweight `xkafka` statistics through `extra/otelxkafka`;
* exporting native Kafka client metrics through `franz-go/plugin/kotel`;
* attaching native `kotel` metrics and tracing explicitly through `WithKafkaOptions`;
* using one OpenTelemetry `MeterProvider` for both metric layers;
* exporting metrics and Kafka spans as JSON to stdout;
* producing and consuming a record to generate telemetry.

The observability layers are independent:

```text
xkafka Stats() ── extra/otelxkafka ──┐
                                     ├── OTel MeterProvider ── stdout
franz-go hooks ───── kotel.Meter ─────┘

franz-go record hooks ── kotel.Tracer ── OTel TracerProvider ── stdout
```

`extra/otelxkafka` exports wrapper-level behavior such as handler calls, handler errors, transaction outcomes, and
wrapper errors. `kotel.Meter` exports native Kafka client operational metrics, while `kotel.Tracer` creates Kafka
produce and fetch spans and propagates trace context through record headers.

OpenTelemetry instrumentation is not enabled automatically by `xkafka`. The example attaches the native `kotel` hooks
explicitly through `WithKafkaOptions`, so applications that do not configure them have no `kotel` hook overhead.

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

Metrics are exported to stdout every five seconds. Kafka spans are also exported to stdout as JSON.

## Produce and consume

Produce one record:

```shell
curl -i -X POST 'http://localhost:8080/produce'
```

Expected response:

```text
HTTP 204
```

The same client consumes the record and prints it:

```text
consume: topic=sample-otel-topic partition=0 offset=0 key="otel" msg={ID:42 Text:hello from xkafka otel example}
```

Partition and offset values depend on the Kafka topic state.

The `kotel` tracer injects trace context into produced record headers and extracts it again when the record is
consumed.

## Telemetry

The stdout output contains two metric layers:

```text
xkafka wrapper metrics   extra/otelxkafka metrics derived from Client.Stats()
franz-go native metrics  messaging.kafka.* metrics produced by kotel.Meter
```

Tracing uses the same explicit native-hook model through `kotel.Tracer`.

`WithName("otel")` sets the native Kafka client ID. The same value is passed explicitly to `kotel.ClientID("otel")` for
tracing attributes.

The stdout exporters are intended for runnable examples and local debugging. In production, replace them with the
OpenTelemetry exporters appropriate for your observability backend.

## Stop

Press `Ctrl+C` to stop the HTTP server and Kafka client and shut down the OpenTelemetry `MeterProvider` and
`TracerProvider`.
