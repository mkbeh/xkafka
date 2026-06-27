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
````

Example:

```shell
export KAFKA_BROKERS=localhost:29092
export KAFKA_TOPICS=sample-topic
export KAFKA_GROUP=sample-group
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

## Metrics

Prometheus metrics are available at:

```shell
curl 'http://localhost:8080/metrics'
```

Useful metrics for this example:

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

Check low-level Kafka fetch records:

```shell
curl -s 'http://localhost:8080/metrics' | grep 'kafka_fetch_records_total'
```

`kafka_fetch_records_total` is collected at the Kafka client fetch layer. It can differ from the number of records
delivered to the handler.

Use `kafka_consume_handle_duration_seconds_count` to estimate how many records were observed by the consumer handler.

