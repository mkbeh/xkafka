# Tracing Example

This example shows how to use `xkafka` with OpenTelemetry tracing.

**This example demonstrates:**

* configuring an OpenTelemetry `TracerProvider`;
* exporting traces through OTLP HTTP;
* passing `TracerProvider` and `TextMapPropagator` to `xkafka`;
* producing records with trace context;
* consuming records with propagated trace context;
* viewing spans in any OTLP-compatible tracing backend;
* Prometheus metrics.

## Configuration

Configure Kafka connection and tracing using environment variables:

```text
KAFKA_BROKERS=localhost:29092

KAFKA_TRACING_TOPIC=sample-tracing-topic
KAFKA_TRACING_GROUP=sample-tracing-group

OTEL_SERVICE_NAME=xkafka-tracing-example
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
```

The tracing backend is not tied to this example. Any OTLP-compatible backend can be used.

For the local docker-compose setup, Jaeger is available at:

```text
http://localhost:16686
```

## Local Kafka setup

Examples can use the local Kafka setup from `examples/docker-compose.yml`.

From the repository root:

```shell
docker compose -f examples/docker-compose.yml up -d
````

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
go run main.go
```

Or from the repository root:

```shell
go run ./examples/tracing
```

The HTTP server starts on:

```text
localhost:8080
```

## Produce tracing record

Sends one record to Kafka with OpenTelemetry trace context.

```shell
curl -X POST 'localhost:8080/produce' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": 100
  }'
```

Expected result:

```text
HTTP 202
message is published and trace spans are exported through OTLP
```

Example log:

```text
consume: topic=sample-tracing-topic, partition=0, offset=0, msg={ID:100}
```

## Produce error record

Sends one record that intentionally fails in the consumer handler.

```shell
curl -X POST 'localhost:8080/produce-error'
```

Expected result:

```text
HTTP 202
message is published, the consumer handler returns an error, and error spans are exported through OTLP
```

Example log:

```text
ERROR error handling records error="forced tracing handler error"
```

The same record may be redelivered because offsets are not committed when the handler returns an error.

In the tracing backend, the processing span should be marked as failed.

## View traces

Open your tracing backend and search for the service name:

```text
xkafka-tracing-example
```

For the local Jaeger setup:

```text
http://localhost:16686
```

Expected spans include producer, consumer, and processing activity around the Kafka record flow.

## Metrics

Prometheus metrics are available at:

```shell
curl 'http://localhost:8080/metrics'
```

Useful metrics for this example include:

```text
kafka_produce_records_total
kafka_produce_errors_total
kafka_consume_handle_duration_seconds
kafka_consume_errors_total
kafka_fetch_records_total
```

Check produced records:

```shell
curl -s 'http://localhost:8080/metrics' | grep 'kafka_produce_records_total'
```

Check records observed by the consumer handler:

```shell
curl -s 'http://localhost:8080/metrics' | grep 'kafka_consume_handle_duration_seconds_count'
```
