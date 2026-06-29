# Share Group Example

This example shows how to use `xkafka` with Kafka Share Groups.

**This example demonstrates:**

* share group consumption with the batch handler API;
* batch size control through `MaxPollRecords` and `ShareMaxRecords`;
* `AckAccept` on successful handling;
* `AckRelease` on handler errors;
* `AckReject` after delivery count limit;
* release timeout before redelivery;
* Prometheus metrics.

## Configuration

Configure Kafka connection using environment variables:

```text
KAFKA_BROKERS=localhost:29092

KAFKA_SHARE_TOPIC=sample-share-topic
KAFKA_SHARE_GROUP=sample-share-group
KAFKA_SHARE_CONSUMERS=4
KAFKA_SHARE_MESSAGES=30
KAFKA_SHARE_MAX_RECORDS=5

KAFKA_SHARE_REJECT_AFTER_DELIVERIES=3
KAFKA_SHARE_RELEASE_TIMEOUT=2s
```

`KAFKA_SHARE_MAX_RECORDS` controls the batch size. Set it to `1` to get single-record style processing through the same
batch handler API.

## Run

From this directory:

```shell
go run main.go
```

Or from the repository root:

```shell
go run ./examples/share_group
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

Example log:

```text
share consume: client_id=sample-share-client-1, records=5
  record: client_id=sample-share-client-1, topic=sample-share-topic, partition=2, offset=60, delivery_count=1, msg={ID:700}
  record: client_id=sample-share-client-1, topic=sample-share-topic, partition=0, offset=44, delivery_count=1, msg={ID:701}

share consume: client_id=sample-share-client-2, records=5
  record: client_id=sample-share-client-2, topic=sample-share-topic, partition=1, offset=12, delivery_count=1, msg={ID:705}
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
share consume: client_id=sample-share-client-1, records=1
  record: client_id=sample-share-client-1, topic=sample-share-topic, partition=2, offset=23, delivery_count=1, msg={ID:888}
ERROR error handling share group records error="forced share handler error"
```

## Produce panic Share Group message

Sends one message with `id=444`.

The share handler intentionally panics for this message.

```shell
curl -X POST 'localhost:8080/share-panic'
```

Expected result:

```text
HTTP 202
the panic is recovered and the record is handled through the share error ack flow
```

Expected log pattern:

```text
share consume: client_id=sample-share-client-1, records=1
  record: client_id=sample-share-client-1, topic=sample-share-topic, partition=2, offset=24, delivery_count=1, msg={ID:444}
ERROR error handling records error="kafka: batch handler panic: forced share handler panic" ack_status=release

share consume: client_id=sample-share-client-1, records=1
  record: client_id=sample-share-client-1, topic=sample-share-topic, partition=2, offset=24, delivery_count=2, msg={ID:444}
ERROR error handling records error="kafka: batch handler panic: forced share handler panic" ack_status=release

share consume: client_id=sample-share-client-1, records=1
  record: client_id=sample-share-client-1, topic=sample-share-topic, partition=2, offset=24, delivery_count=3, msg={ID:444}
ERROR error handling records error="kafka: batch handler panic: forced share handler panic" ack_status=reject
```

The panicking record is released for redelivery until the delivery count reaches the reject limit.

In a real system, poison records should eventually be handled with retry limits, a dead-letter topic, or another
recovery policy.


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
