# Transactions Example

This example shows how to use `xkafka` producer transactions.

**This example demonstrates:**

* transactional message publishing;
* transaction commit;
* transaction abort on error;
* transaction abort on panic;
* `read_committed` consumer behavior;
* Prometheus metrics.

## Configuration

Configure Kafka connection using environment variables:

```text
KAFKA_BROKERS=localhost:29092
KAFKA_TX_TOPIC=sample-tx-topic
KAFKA_TX_GROUP=sample-tx-group
KAFKA_TRANSACTIONAL_ID=sample-tx-producer
```

## Run

From this directory:

```shell
go run main.go
```

Or from the repository root:

```shell
go run ./examples/transactions
```

The HTTP server starts on:

```text
localhost:8080
```

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

In `franz-go` logs, the transaction should end with:

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

In `franz-go` logs, the transaction should end with:

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

In `franz-go` logs, the transaction should end with:

```text
commit=false
```

## Metrics

Prometheus metrics are available at:

```shell
curl 'http://localhost:8080/metrics'
```

Useful metrics for this example include:

```text
kafka_produce_errors_total
kafka_transactions_total
kafka_transaction_duration_seconds
kafka_consume_handle_duration_seconds
kafka_consume_errors_total
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
curl -s 'http://localhost:8080/metrics' | grep 'kafka_consume_handle_duration_seconds_count'
```