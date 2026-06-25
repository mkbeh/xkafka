# Sample REST API

This example shows how to use `xkafka` in a simple REST API service.

**It demonstrates:**

* synchronous message publishing;
* asynchronous message publishing;
* transactional message publishing;
* transaction error handling;
* transaction panic recovery;
* message consumption;
* Prometheus metrics endpoint.

The service uses Kafka through `franz-go` under the hood.

## Configuration

Configure Kafka connection using environment variables:

```text
KAFKA_BROKERS=localhost:29092
KAFKA_TOPICS=sample-topic
KAFKA_GROUP=sample-group
```

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

## Produce synchronously

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

## Produce asynchronously

Sends one message without waiting for the result in the HTTP handler.

```shell
curl -X POST 'localhost:8080/async' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": 10
  }'
```

Expected result:

```text
HTTP 200
message is eventually visible to the consumer if Kafka publish succeeds
```

Async publish errors are logged by the producer callback.

## Produce transactionally

Runs message publishing inside a Kafka transaction.

```shell
curl -X POST 'localhost:8080/tx' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": 100
  }'
```

Expected result:

```text
HTTP 202
message is visible to the read_committed consumer
transaction outcome is commit
```

In franz-go logs, the transaction should end with:

```text
commit=true
```

## Transaction error

Runs transactional publishing and then returns an error from the transaction function.

```shell
curl -v -X POST 'localhost:8080/tx-error' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": 300
  }'
```

Expected result:

```text
HTTP 500
message is not visible to the read_committed consumer
transaction outcome is error
```

In franz-go logs, the transaction should end with:

```text
commit=false
```

## Transaction panic

Runs transactional publishing and then panics inside the transaction function.

```shell
curl -v -X POST 'localhost:8080/tx-panic' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": 400
  }'
```

Expected result:

```text
HTTP 500
message is not visible to the read_committed consumer
transaction outcome is error
```

The transaction is aborted before the panic is re-thrown to the HTTP handler.

In franz-go logs, the transaction should end with:

```text
commit=false
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

Useful metrics for this example:

```text
kafka_produce_records_total
kafka_produce_errors_total
kafka_transactions_total
kafka_transaction_duration_seconds
kafka_consume_handle_timing
kafka_fetch_records_total
```

Transaction outcomes:

```text
kafka_transactions_total{outcome="commit"}
kafka_transactions_total{outcome="error"}
```

Check transaction metrics:

```shell
curl -s 'http://localhost:8080/metrics' | grep 'kafka_transactions_total'
```

Check transaction duration metrics:

```shell
curl -s 'http://localhost:8080/metrics' | grep 'kafka_transaction_duration_seconds'
```

Check records observed by the consumer handler:

```shell
curl -s 'http://localhost:8080/metrics' | grep 'kafka_consume_handle_timing_count'
```

Check low-level Kafka fetch records:

```shell
curl -s 'http://localhost:8080/metrics' | grep 'kafka_fetch_records_total'
```

`kafka_fetch_records_total` is collected at the Kafka client fetch layer. It can differ from the number of records
delivered to the handler, especially with transactions and `read_committed` isolation level.

Use `kafka_consume_handle_timing_count` to estimate how many records were observed by the consumer handler.

## Notes

A producer configured with `TransactionalID` should publish records inside `RunInTx`.

For regular sync and async message publishing, use a producer without `TransactionalID`.

For transactional message publishing, use a separate producer configured with `TransactionalID`.

Inside `RunInTx`, prefer `ProduceSync` when the transaction should wait for Kafka produce results before commit.

`RunInTx` flushes before committing because `EndTransaction` does not flush buffered records by itself.

If the transaction function returns `nil`, `RunInTx` tries to commit the transaction.

If the transaction function returns an error or panics, `RunInTx` aborts the transaction.

The consumer is configured with `read_committed` isolation level, so aborted transactional messages are not processed.
