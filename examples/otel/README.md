# OpenTelemetry Example

This example demonstrates how to add OpenTelemetry metrics and distributed tracing to `xkafka` while also collecting
native `franz-go` client metrics.

**This example demonstrates:**

* **Collecting native client metrics** from the underlying `franz-go` client
* **Exporting xkafka runtime metrics** through OpenTelemetry
* **Tracing message processing** across synchronous producing and batch consumption
* **Propagating trace context** through Kafka records
* **Observing processing failures and retries** through metrics and traces

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

Jaeger UI is available at:

```text
http://localhost:16686
```

The example uses the `sample-otel-topic-a` and `sample-otel-topic-b` topics
created by the local Kafka setup.

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

Prometheus metrics are exposed at:

```text
http://localhost:8080/metrics
```

## Produce a mixed-topic batch

`POST /produce` publishes ten records in one synchronous produce operation:
five records to each example topic.

```shell
curl -i -X POST 'http://localhost:8080/produce'
```

Expected response:

```text
HTTP 202
published 10 records: 5 to sample-otel-topic-a and 5 to sample-otel-topic-b
```

Example log:

```text
consume: topic=sample-otel-topic-a key="1" msg={ID:1 Text:otel message 1}
...
consume: topic=sample-otel-topic-b key="6" msg={ID:6 Text:otel message 6}
...
```

The produce operation creates one `send` span and propagates its trace context
through the records. Each batch handler invocation creates one `process` span.
Sampled message creation contexts propagated through the records are linked to
that `process` span.

## Observe a handler error and retry

`POST /produce-error` publishes record `888`. The consumer handler intentionally
fails on the first processing attempt and succeeds on the configured retry.

```shell
curl -i -X POST 'http://localhost:8080/produce-error'
```

Expected response:

```text
HTTP 202
published record that fails once in the consumer handler
```

The failed and successful processing attempts can then be inspected in the
exported metrics and traces.

## Metrics

Open the Prometheus endpoint:

```shell
curl http://localhost:8080/metrics
```

The endpoint exposes both native franz-go client metrics and xkafka runtime
metrics through the OpenTelemetry Prometheus exporter.

## Traces

Open Jaeger:

```text
http://localhost:16686
```

Select service:

```text
xkafka-otel-example
```

The example exposes producer `send` spans, batch `process` spans, settlement
spans for offset commits, and propagated trace context across Kafka records.

## Stop services

From the repository root:

```shell
docker compose -f examples/docker-compose.yml down --remove-orphans -v
```

Or from this example directory:

```shell
docker compose -f ../docker-compose.yml down --remove-orphans -v
```