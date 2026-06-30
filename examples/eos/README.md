# EOS Processing Example

This example shows Kafka-to-Kafka exactly-once processing with `GroupTransactSession`.

**This example demonstrates:**

* producing records to an input topic;
* consuming input records with `GroupTransactSession`;
* producing transformed records to an output topic inside the same Kafka transaction;
* committing produced records and consumed offsets atomically;
* consuming output records with `ReadCommitted`;
* aborting a transaction when the handler returns an error;
* aborting a transaction when the handler panics;
* Prometheus metrics.

## Configuration

Configure Kafka connection using environment variables:

```text
KAFKA_BROKERS=localhost:29092

KAFKA_EOS_INPUT_TOPIC=sample-eos-input-topic
KAFKA_EOS_OUTPUT_TOPIC=sample-eos-output-topic
KAFKA_EOS_GROUP=sample-eos-group
KAFKA_EOS_OUTPUT_GROUP=sample-eos-output-group
KAFKA_EOS_TRANSACTIONAL_ID=sample-eos-session
KAFKA_EOS_MESSAGES=10
```

## Local Kafka setup

Examples can use the local Kafka setup from `examples/docker-compose.yml`.

From the repository root:

```shell
docker compose -f examples/docker-compose.yml up -d
````

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

## Run

From this directory:

```shell
go run main.go
```

Or from the repository root:

```shell
go run ./examples/eos
```

The HTTP server starts on:

```text
localhost:8080
```

## Produce input records

Sends records to the input topic. The group transact session consumes them and produces transformed records to the
output topic transactionally.

```shell
curl -X POST 'localhost:8080/group-tx' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": 100
  }'
```

Expected result:

```text
HTTP 202
records are visible in the output topic after the group transaction commits
```

Example log:

```text
group tx consume batch: records=10
output consume: topic=sample-eos-output-topic, partition=0, offset=0, msg={ID:100 Source:sample-eos-input-topic Processed:true}
output consume: topic=sample-eos-output-topic, partition=0, offset=1, msg={ID:101 Source:sample-eos-input-topic Processed:true}
```

## Produce failing input records

Sends two input records: one valid record followed by one poison record.

The handler produces an output record for the valid input record first, then returns an error on the poison record.
The group transaction is aborted, so produced output records are not committed and remain invisible to `ReadCommitted`
consumers.

```shell
curl -X POST 'localhost:8080/group-tx-error'
```

Expected result:

```text
HTTP 202
the group transaction is aborted and output records are not committed
```

Example log:

```text
group tx consume batch: records=2
ERROR error handling records in group transaction error="forced group transaction handler error"
```

The same input records may be redelivered because consumed offsets are not committed when the transaction aborts.

In a real system, poison records should eventually be handled with retry limits, a dead-letter topic, or another
recovery policy.

## Produce panic input record

Sends one input record that intentionally panics in the group transact session handler.

```shell
curl -X POST 'localhost:8080/group-tx-panic'
```

Expected result:

```text
HTTP 202
the group transaction is aborted and the consumed offset is not committed
```

Example log:

```text
group tx consume batch: records=1
ERROR error handling records in group transaction error="kafka: batch handler panic: forced group transaction handler panic"
```

The same input record may be redelivered because consumed offsets are not committed when the transaction aborts.

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

Check records observed by handlers:

```shell
curl -s 'http://localhost:8080/metrics' | grep 'kafka_consume_handle_duration_seconds_count'
```
