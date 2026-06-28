# EOS Processing Example

This example shows Kafka-to-Kafka exactly-once processing with `GroupTransactSession`.

**This example demonstrates:**

* producing records to an input topic;
* consuming input records with `GroupTransactSession`;
* producing transformed records to an output topic inside the same Kafka transaction;
* committing produced records and consumed offsets atomically;
* consuming output records with `ReadCommitted`;
* aborting a transaction when the handler returns an error;
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

Example:

```shell
export KAFKA_BROKERS=localhost:29092

export KAFKA_EOS_INPUT_TOPIC=sample-eos-input-topic
export KAFKA_EOS_OUTPUT_TOPIC=sample-eos-output-topic
export KAFKA_EOS_GROUP=sample-eos-group
export KAFKA_EOS_OUTPUT_GROUP=sample-eos-output-group
export KAFKA_EOS_TRANSACTIONAL_ID=sample-eos-session
export KAFKA_EOS_MESSAGES=10
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

## Produce failing input record

Sends one record that intentionally fails in the group transact session handler.

```shell
curl -X POST 'localhost:8080/group-tx-error'
```

Expected result:

```text
HTTP 202
the group transaction is aborted and output records are not committed
```

The failing input record may be redelivered because consumed offsets are not committed when the transaction aborts.

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

Check records observed by handlers:

```shell
curl -s 'http://localhost:8080/metrics' | grep 'kafka_consume_handle_duration_seconds_count'
```
