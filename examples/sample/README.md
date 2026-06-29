# Sample REST API

This example shows how to use `xkafka` in a simple REST API service.

**This example demonstrates:**

* sync and async message publishing;
* regular Kafka consumer group consumption;
* Prometheus metrics.

## Configuration

Configure Kafka connection using environment variables:

```text
KAFKA_BROKERS=localhost:29092
KAFKA_TOPICS=sample-topic
KAFKA_GROUP=sample-group
```

## Run

From this directory:

```shell
go run main.go
```

Or from the repository root:

```shell
go run ./examples/sample
```

The HTTP server starts on:

```text
localhost:8080
```

## Produce sync

Sends one message and waits until Kafka acknowledges the write.

```shell
curl -X POST 'localhost:8080/sync' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": 1
  }'
```

Expected result:

```text
HTTP 200
message is visible to the consumer
```

## Produce async

Sends one message without waiting for the Kafka result in the HTTP handler.

```shell
curl -X POST 'localhost:8080/async' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": 10
  }'
```

Expected result:

```text
HTTP 202
message is later visible to the consumer if Kafka publishing succeeds
```

Async publishing errors are logged by the producer callback.

## Produce panic record

Sends one record that intentionally panics in the consumer handler.

```shell
curl -X POST 'localhost:8080/panic'
```

Expected result:

```text
HTTP 202
the consumer handler panics, the panic is recovered, and offsets are not committed
```

Example log:

```text
ERROR error handling records error="kafka: batch handler panic: forced consumer handler panic"
```

The same record may be redelivered because offsets are not committed when the handler panics.

In a real system, poison records should eventually be handled with retry limits, a dead-letter topic, or another
recovery policy.

## Metrics

Prometheus metrics are available at:

```shell
curl 'http://localhost:8080/metrics'
```

Useful metrics for this example:

```text
kafka_produce_errors_total
kafka_consume_handle_duration_seconds
kafka_consume_errors_total
```

Check produce errors:

```shell
curl -s 'http://localhost:8080/metrics' | grep 'kafka_produce_records_total'
```

Check records observed by the consumer handler:

```shell
curl -s 'http://localhost:8080/metrics' | grep 'kafka_consume_handle_duration_seconds_count'
```

Check low-level Kafka fetch records:

```shell
curl -s 'http://localhost:8080/metrics' | grep 'kafka_fetch_records_total'
```

`kafka_fetch_records_total` is collected at the Kafka client fetch layer. It can differ from the number of records
delivered to the handler.

Use `kafka_consume_handle_duration_seconds_count` to estimate how many records were observed by the consumer handler.

