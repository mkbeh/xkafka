# OpenTelemetry Observability Example

This example combines wrapper-level `xkafka` metrics, native `franz-go` metrics, and Kafka tracing.

**This example demonstrates:**

* exporting lightweight `xkafka` statistics through `extra/otelxkafka`;
* exporting native Kafka client metrics through `franz-go/plugin/kprom`;
* attaching `franz-go/plugin/kotel` tracing explicitly through `WithKafkaOptions`;
* using one Prometheus registry for both metric layers;
* exporting Kafka spans as JSON to stdout;
* producing and consuming a record to generate metrics and traces.

The observability layers are independent:

```text
xkafka Stats() ── extra/otelxkafka ── OpenTelemetry metrics ──┐
                                                             ├── Prometheus registry ── /metrics
franz-go hooks ─────────── kprom ─────────────────────────────┘

franz-go record hooks ──── kotel ─── OpenTelemetry tracing ───── stdout
```

`extra/otelxkafka` exports wrapper-level behavior such as handler calls, handler errors, transaction outcomes, and
wrapper errors. `kprom` exports Kafka client operational metrics such as produced and fetched records, bytes, buffered
records, broker I/O, batches, request timings, and throttling. `kotel` creates Kafka produce and fetch spans and
propagates trace context through record headers.

Tracing is not enabled automatically by `xkafka`. The example attaches the native `kotel` tracer explicitly through
`WithKafkaOptions`, so applications that do not configure tracing have no tracing hooks installed by `xkafka`.

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
http://localhost:9464
```

## Produce and consume

Produce one record:

```shell
curl -i -X POST 'http://localhost:9464/produce'
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

The `kotel` tracer also writes Kafka spans as JSON to stdout. Trace context is injected into produced record headers and
extracted again when the record is consumed.

## Metrics

Open the Prometheus endpoint:

```shell
curl 'http://localhost:9464/metrics'
```

Show only Kafka-related metrics:

```shell
curl -s 'http://localhost:9464/metrics' \
  | grep -E '^(xkafka_client_|kafka_)'
```

The output contains two groups:

```text
xkafka_client_*   wrapper-level metrics exported from Client.Stats()
kafka_*           native franz-go metrics exported by kprom
```

The example enables detailed `kprom` produce/fetch metrics for records, batches, compressed and uncompressed bytes, as
well as request, read, write, and throttling histograms.

`WithName("otel")` sets the native Kafka client ID used by `kprom.WithClientLabel()`. The same value is passed explicitly
to `kotel.ClientID("otel")` for tracing attributes.

For production, enable only the `kprom` details and histograms you need to keep metric cardinality and collection cost
appropriate for your workload, and replace the stdout trace exporter with your preferred OpenTelemetry exporter.

## Stop

Press `Ctrl+C` to stop the HTTP server, Kafka client, metrics registration, OpenTelemetry `MeterProvider`, and
OpenTelemetry `TracerProvider`.
