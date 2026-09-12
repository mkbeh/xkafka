# Transactions Example

This example shows how to use Kafka producer transactions and consume only committed records.

**This example demonstrates:**

* Creating a dedicated transactional producer
* Committing records in a transaction
* Aborting transactions when processing returns an error
* Aborting transactions before re-throwing a panic
* Consuming only records from committed transactions

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

The example uses the `sample-tx-topic` topic created by the local Kafka setup.

## Run

From this directory:

```shell
go run .
```

Or from the repository root:

```shell
go run ./examples/transactions
```

The HTTP server listens on:

```text
http://localhost:8080
```

## Commit a transaction

`POST /tx` publishes the record and commits the transaction.

```shell
curl -i -X POST 'http://localhost:8080/tx' \
  -H 'Content-Type: application/json' \
  -d '{"id":100}'
```

Expected response:

```text
HTTP 202
transaction committed
```

Example log:

```text
consume committed transaction: topic=sample-tx-topic key="100" msg={ID:100}
```

## Abort on error

`POST /tx-error` produces a record and then returns an error from the transaction callback.
The transaction is aborted, so the record is not visible to the read-committed consumer.

```shell
curl -i -X POST 'http://localhost:8080/tx-error' \
  -H 'Content-Type: application/json' \
  -d '{"id":300}'
```

Expected response:

```text
HTTP 500
forced transaction error
```

No record with key `300` should appear in the consumer log.

## Abort on panic

`POST /tx-panic` produces a record and then panics inside the transaction callback.
The transaction is aborted before the panic is re-thrown. The HTTP handler recovers
the panic only so the example server can return a response and keep running.

```shell
curl -i -X POST 'http://localhost:8080/tx-panic' \
  -H 'Content-Type: application/json' \
  -d '{"id":400}'
```

Expected response:

```text
HTTP 500
transaction panic: forced transaction panic
```

No record with key `400` should appear in the consumer log.

## Stop services

From the repository root:

```shell
docker compose -f examples/docker-compose.yml down --remove-orphans -v
```

Or from this example directory:

```shell
docker compose -f ../docker-compose.yml down --remove-orphans -v
```