# Transactions Example

This example demonstrates how to use Kafka transactions for atomic record publishing and consume only committed records.

**This example demonstrates:**

* **Creating a transactional producer** for atomic write operations
* **Committing records atomically** within a transaction
* **Aborting transactions on errors** returned from transactional processing
* **Handling panics safely** by aborting the transaction before re-throwing the panic
* **Reading only committed records** from a downstream consumer

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

The `POST /tx` endpoint publishes a record and commits it in a Kafka transaction.

```shell
curl -i -X POST 'http://localhost:8080/tx' \
  -H 'Content-Type: application/json' \
  -d '{"id":100}'
```

### Expected response

```http
HTTP/1.1 202 Accepted

transaction committed
```

### Example log

Once the transaction is committed, the downstream consumer reads the committed record:

```text
consume committed transaction: topic=sample-tx-topic key="100" msg={ID:100}
```

## Abort a transaction on error

The `POST /tx-error` endpoint publishes a record inside a Kafka transaction and then returns an error from the
transaction callback. The transaction is automatically aborted, so the record is not visible to `read_committed`
consumers.

```shell
curl -i -X POST 'http://localhost:8080/tx-error' \
  -H 'Content-Type: application/json' \
  -d '{"id":300}'
```

### Expected response

```http
HTTP/1.1 500 Internal Server Error

forced transaction error
```

> **Verification:** Because the transaction was aborted, no record with key `"300"` appears in the downstream consumer
> logs.

## Abort a transaction on panic

The `POST /tx-panic` endpoint publishes a record inside a Kafka transaction and then panics from the transaction
callback. The transaction is automatically aborted before the panic is re-thrown. The HTTP handler recovers from the
panic to return an error response and keep the example application running.

```shell
curl -i -X POST 'http://localhost:8080/tx-panic' \
  -H 'Content-Type: application/json' \
  -d '{"id":400}'
```

### Expected response

```http
HTTP/1.1 500 Internal Server Error

transaction panic: forced transaction panic
```

> **Verification:** Because the transaction was aborted, no record with key `"400"` appears in the downstream consumer
> logs.

## Stop services

From the repository root:

```shell
docker compose -f examples/docker-compose.yml down --remove-orphans -v
```

Or from this example directory:

```shell
docker compose -f ../docker-compose.yml down --remove-orphans -v
```