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

## Process records successfully

`POST /share` publishes 12 records starting at the supplied ID.

```shell
curl -i -X POST 'http://localhost:8080/share' \
  -H 'Content-Type: application/json' \
  -d '{"id":700}'
```

Expected response:

```text
HTTP 202
published 12 share records
```

The three consumers process records from the same Share Group in batches of up
to two records.

Example log:

```text
share consume: client=share-consumer-2 records=2
  record: topic=sample-share-topic delivery_count=1 key="701" msg={ID:701}
  record: topic=sample-share-topic delivery_count=1 key="709" msg={ID:709}
...
```

Consumer assignment and record order can vary between runs.

## Release and redeliver on error

`POST /share-error` publishes a record with ID `888`. The handler intentionally
returns an error while processing this record.

```shell
curl -i -X POST 'http://localhost:8080/share-error'
```

Expected response:

```text
HTTP 202
published share record id=888
```

A handler error releases the batch. Records that have not reached the delivery
limit are redelivered and their delivery counts increase.

Example log:

```text
share consume: client=share-consumer-2 records=1
  record: topic=sample-share-topic delivery_count=1 key="888" msg={ID:888}
share consume: client=share-consumer-1 records=1
  record: topic=sample-share-topic delivery_count=2 key="888" msg={ID:888}
share consume: client=share-consumer-3 records=1
  record: topic=sample-share-topic delivery_count=3 key="888" msg={ID:888}
...
```

After the third failed delivery, the record is rejected instead of released
again. Released acknowledgements are flushed after a one-second delay before
the next redelivery.

## Release and redeliver on panic

`POST /share-panic` publishes a record with ID `444`. The handler intentionally
panics while processing this record.

```shell
curl -i -X POST 'http://localhost:8080/share-panic'
```

Expected response:

```text
HTTP 202
published share record id=444
```

xkafka recovers the handler panic and routes the batch through the same failure
path. Records below the delivery limit are released for redelivery, while
records at the limit are rejected.

## Stop services

From the repository root:

```shell
docker compose -f examples/docker-compose.yml down --remove-orphans -v
```

Or from this example directory:

```shell
docker compose -f ../docker-compose.yml down --remove-orphans -v
```