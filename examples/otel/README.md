# OpenTelemetry Example

This example demonstrates how to add OpenTelemetry metrics and distributed tracing to `xkafka` while also collecting
native `franz-go` client metrics.

**This example demonstrates:**

* **Collecting native client metrics** from the underlying `franz-go` client
* **Exporting `xkafka` runtime metrics** through OpenTelemetry
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

The `POST /produce` endpoint publishes 10 records in one synchronous produce operation, split equally between the two
example topics.

```shell
curl -i -X POST 'http://localhost:8080/produce'
```

### Expected response

```http
HTTP/1.1 202 Accepted

published 10 records: 5 to sample-otel-topic-a and 5 to sample-otel-topic-b
```

### Example log

The consumer processes records from both topics and produces logs similar to the following:

```text
consume: topic=sample-otel-topic-a key="1" msg={ID:1 Text:otel message 1}
...
consume: topic=sample-otel-topic-b key="6" msg={ID:6 Text:otel message 6}
...
```

### Tracing

This workflow creates one `send` span for all 10 records. Each record carries its trace context, which is attached to
the corresponding consumer `process` span as an OpenTelemetry span link.

## Observe a handler error and retry

The `POST /produce-error` endpoint publishes 1 record with ID `888`. The consumer handler intentionally fails during the
first processing attempt and succeeds on the second attempt.

```shell
curl -i -X POST 'http://localhost:8080/produce-error'
```

### Expected response

```http
HTTP/1.1 202 Accepted

published record that fails once in the consumer handler
```

### Observability

After processing completes, the failed attempt and successful retry can be inspected through traces and metrics:

* **Traces:** The first `process` span is marked as `Error`, while the second `process` span completes successfully.
  Both attempts link to the propagated producer context from the same Kafka record.
* **Metrics:** `messaging.process.duration` records both processing attempts, with `error.type` set on the failed
  attempt. `xkafka.handler.records` records the successfully processed record after the retry.

## Metrics

Open the Prometheus endpoint:

```shell
curl http://localhost:8080/metrics
```

The endpoint exposes native `franz-go` client metrics together with `xkafka` runtime metrics through the OpenTelemetry
Prometheus exporter.

Depending on the exercised workflow, `xkafka` exports the following runtime metrics:

| Metric                                | Type      | Description / Attributes                                                            |
|---------------------------------------|-----------|-------------------------------------------------------------------------------------|
| **Producing and fetching**            |           |                                                                                     |
| `xkafka.produce.errors`               | Counter   | Failed outgoing records (`topic`, `error_type`).                                    |
| `xkafka.fetch.errors`                 | Counter   | Kafka fetch errors (`topic`, `partition`, `recoverable`, `error_type`).             |
| **Processing**                        |           |                                                                                     |
| `messaging.process.duration`          | Histogram | Handler processing duration (`topic`, `error_type`).                                |
| `xkafka.handler.records`              | Counter   | Successfully processed records (`topic`).                                           |
| **Settlements**                       |           |                                                                                     |
| `messaging.client.operation.duration` | Histogram | Offset commit and Share Group acknowledgement duration (`operation`, `error_type`). |
| `xkafka.share.ack.records`            | Counter   | Share Group acknowledgement outcomes (`outcome`: `accept`, `release`, or `reject`). |
| **Transactions**                      |           |                                                                                     |
| `xkafka.transaction.duration`         | Histogram | Transaction attempt duration (`type`, `outcome`, `error_type`).                     |

Common client and consumer attributes, such as the client ID and consumer group, are added automatically when
configured.

## Traces

To inspect the distributed traces generated by the application, open the Jaeger UI:

```text
http://localhost:16686
```

Select the `xkafka-otel-example` service from the dropdown menu.

The exported spans cover the message processing lifecycle:

| Span      | Description                                                                                      |
|-----------|--------------------------------------------------------------------------------------------------|
| `send`    | Synchronous produce operations, including batch size and destination attributes when applicable. |
| `process` | Consumer batch handler invocations, including error status on failed attempts.                   |
| `commit`  | Consumer offset commit operations and their latency.                                             |

Producer trace context is propagated through Kafka record headers, allowing consumer `process` spans to use
OpenTelemetry span links to associate processed records with their message creation contexts.

## Stop services

From the repository root:

```shell
docker compose -f examples/docker-compose.yml down --remove-orphans -v
```

Or from this example directory:

```shell
docker compose -f ../docker-compose.yml down --remove-orphans -v
```
