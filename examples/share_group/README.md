# Share Group Example

This example demonstrates how to use Kafka Share Groups (KIP-932) to distribute and process records concurrently with
queue-like delivery semantics.

**This example demonstrates:**

* **Scaling concurrent consumption** across multiple consumers in a single Share Group
* **Accepting successfully processed records** so they are not delivered again
* **Redelivering failed records** so another processing attempt can be made
* **Handling poison records** by rejecting them after repeated processing failures

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

The example uses the `sample-share-topic` topic created by the local Kafka setup.

## Run

From this directory:

```shell
go run .
```

Or from the repository root:

```shell
go run ./examples/share_group
```

The HTTP server listens on:

```text
http://localhost:8080
```

Start the example before publishing records so the Share Group consumers are
already polling when new records arrive.

## Process records via Share Group

The `POST /share` endpoint publishes 12 records starting from the provided ID. The records are then distributed across
multiple consumers in the same Kafka Share Group.

```shell
curl -i -X POST 'http://localhost:8080/share' \
  -H 'Content-Type: application/json' \
  -d '{"id":700}'
```

### Expected response

```http
HTTP/1.1 202 Accepted

published 12 share records
```

### Example log

In this example, 3 active consumers process records from the Share Group in batches of up to 2 records.

Because records can be distributed across consumers independently, consumer assignment and delivery order can vary
between runs:

```text
share consume: client=share-consumer-2 records=2
  record: topic=sample-share-topic delivery_count=1 key="701" msg={ID:701}
  record: topic=sample-share-topic delivery_count=1 key="709" msg={ID:709}
...
```

## Release and redeliver on error

The `POST /share-error` endpoint publishes a single record with ID `888`. The consumer handler intentionally returns an
error during processing, causing the record to be released for redelivery.

```shell
curl -i -X POST 'http://localhost:8080/share-error'
```

### Expected response

```http
HTTP/1.1 202 Accepted

published share record id=888
```

### Example log

When the handler returns an error, records that have not reached the configured delivery threshold are released back to
the Share Group and can be delivered again with an incremented delivery count.

In this example, released acknowledgements are flushed after a one-second delay. The record is retried until its
delivery count reaches the configured threshold of three:

```text
share consume: client=share-consumer-2 records=1
  record: topic=sample-share-topic delivery_count=1 key="888" msg={ID:888}
share consume: client=share-consumer-1 records=1
  record: topic=sample-share-topic delivery_count=2 key="888" msg={ID:888}
share consume: client=share-consumer-3 records=1
  record: topic=sample-share-topic delivery_count=3 key="888" msg={ID:888}
```

Consumer assignment can vary between redeliveries.

> **Note:** On the third failed delivery, the record is rejected instead of being released again.

## Release and redeliver on panic

The `POST /share-panic` endpoint publishes a record with ID `444`. The consumer handler intentionally panics while
processing the record.

```shell
curl -i -X POST 'http://localhost:8080/share-panic'
```

### Expected response

```http
HTTP/1.1 202 Accepted

published share record id=444
```

### Behavior

`xkafka` recovers from the handler panic and routes the affected batch through the same failure path as a handler error.

**The record follows the same delivery lifecycle:**

* **Below the delivery threshold:** The record is released back to the Share Group and can be delivered again
* **At the delivery threshold:** The record is rejected instead of being released again

## Stop services

From the repository root:

```shell
docker compose -f examples/docker-compose.yml down --remove-orphans -v
```

Or from this example directory:

```shell
docker compose -f ../docker-compose.yml down --remove-orphans -v
```