# Share Consumer Example

This example shows how to use `xkafka` with Kafka Share Groups.

**This example demonstrates:**

* single-record share consumer handler;
* batch share consumer handler;
* `AckAccept` on successful handling;
* `AckRelease` on handler errors;
* `AckReject` after delivery count limit;
* release timeout before redelivery;
* multiple share consumers processing records from the same topic;
* Prometheus metrics.

## Configuration

Configure Kafka connection using environment variables:

```text
KAFKA_BROKERS=localhost:29092

KAFKA_SHARE_TOPIC=sample-share-topic
KAFKA_SHARE_GROUP=sample-share-group
KAFKA_SHARE_CONSUMERS=4
KAFKA_SHARE_MESSAGES=30

KAFKA_SHARE_BATCH_TOPIC=sample-share-batch-topic
KAFKA_SHARE_BATCH_GROUP=sample-share-batch-group
KAFKA_SHARE_BATCH_CONSUMERS=2
KAFKA_SHARE_BATCH_MESSAGES=30
KAFKA_SHARE_BATCH_MAX_RECORDS=5

KAFKA_SHARE_REJECT_AFTER_DELIVERIES=3
KAFKA_SHARE_RELEASE_TIMEOUT=5s
````

Example:

```shell
export KAFKA_BROKERS=localhost:29092

export KAFKA_SHARE_TOPIC=sample-share-topic
export KAFKA_SHARE_GROUP=sample-share-group
export KAFKA_SHARE_CONSUMERS=4
export KAFKA_SHARE_MESSAGES=30

export KAFKA_SHARE_BATCH_TOPIC=sample-share-batch-topic
export KAFKA_SHARE_BATCH_GROUP=sample-share-batch-group
export KAFKA_SHARE_BATCH_CONSUMERS=2
export KAFKA_SHARE_BATCH_MESSAGES=30
export KAFKA_SHARE_BATCH_MAX_RECORDS=5

export KAFKA_SHARE_REJECT_AFTER_DELIVERIES=3
export KAFKA_SHARE_RELEASE_TIMEOUT=5s
```

## Run

From this directory:

```shell
go run main.go
```

Or from the repository root:

```shell
go run ./examples/share-consumer
```

The HTTP server starts on:

```text
localhost:8080
```

## Produce to ShareGroup topic

Sends messages to a topic consumed by several share consumers.

```shell
curl -X POST 'localhost:8080/share' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": 700
  }'
```

Expected result:

```text
HTTP 202
messages are visible to share consumers
```

The endpoint publishes `KAFKA_SHARE_MESSAGES` records starting from the provided `id`.

Example log:

```text
share consume: client_id=sample-share-client-1, topic=sample-share-topic, partition=2, offset=60, delivery_count=1, msg={ID:700}
share consume: client_id=sample-share-client-2, topic=sample-share-topic, partition=0, offset=44, delivery_count=1, msg={ID:701}
```

Successful records are acknowledged with `AckAccept`.

## Produce failing ShareGroup message

Sends one message with `id=888`.

The share handler intentionally returns an error for this message.

```shell
curl -X POST 'localhost:8080/share-error'
```

Expected result:

```text
HTTP 202
the same record is redelivered until delivery count reaches the reject limit
```

Expected log pattern:

```text
share consume: client_id=sample-share-client-1, topic=sample-share-topic, partition=2, offset=23, delivery_count=1, msg={ID:888}
ERROR error handling share group record error="forced share handler error" ack_status=release

share consume: client_id=sample-share-client-1, topic=sample-share-topic, partition=2, offset=23, delivery_count=2, msg={ID:888}
ERROR error handling share group record error="forced share handler error" ack_status=release

share consume: client_id=sample-share-client-1, topic=sample-share-topic, partition=2, offset=23, delivery_count=3, msg={ID:888}
ERROR error handling share group record error="forced share handler error" ack_status=reject
```

## Produce to ShareGroup batch topic

Sends messages to a separate topic consumed by share batch consumers.

```shell
curl -X POST 'localhost:8080/share-batch' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": 800
  }'
```

Expected result:

```text
HTTP 202
messages are visible to share batch consumers
```

The endpoint publishes `KAFKA_SHARE_BATCH_MESSAGES` records starting from the provided `id`.

Example log:

```text
share batch consume: client_id=sample-share-batch-client-2, records=11
  record: client_id=sample-share-batch-client-2, topic=sample-share-batch-topic, partition=0, offset=0, delivery_count=1, msg={ID:802}
```

The batch handler uses all-or-nothing acknowledgement semantics:

```text
batch handler success -> AckAccept for all records in the batch
batch handler error   -> AckRelease or AckReject for all records in the batch
```

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

Check records observed by share handlers:

```shell
curl -s 'http://localhost:8080/metrics' | grep 'kafka_consume_handle_duration_seconds_count'
```
