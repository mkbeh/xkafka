# Sample REST API

This example shows how to use `xkafka` in a simple REST API service.

**It demonstrates:**

- synchronous message production;
- asynchronous message production;
- transactional message production;
- message consumption;
- Prometheus metrics endpoint.

The service uses Kafka through `franz-go` under the hood.

## Configuration

Configure Kafka connection using environment variables:

```text
KAFKA_BROKERS=localhost:29092
KAFKA_TOPICS=test-topic
KAFKA_GROUP=test-group
````

Example:

```shell
export KAFKA_BROKERS=localhost:29092
export KAFKA_TOPICS=test-topic
export KAFKA_GROUP=test-group
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

## Produce synchronously

Sends one message and waits until Kafka acknowledges the write.

```shell
curl -X POST 'localhost:8080/sync' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": 1
  }'
```

## Produce asynchronously

Sends one message without waiting for the result in the HTTP handler.

```shell
curl -X POST 'localhost:8080/async' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": 10
  }'
```

## Produce transactionally

Runs message production inside a Kafka transaction.

```shell
curl -X POST 'localhost:8080/tx' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": 100
  }'
```

The sample uses a dedicated transactional producer configured with `TransactionalID`.
The consumer is configured with `read_committed` isolation level, so aborted transactional messages are not processed.

## Metrics

Prometheus metrics are available at:

```shell
curl 'http://localhost:8080/metrics'
```

## Notes

A producer configured with `TransactionalID` should produce records inside `RunInTx`.

For regular sync and async production, use a producer without `TransactionalID`.
For transactional production, use a separate producer configured with `TransactionalID`.
