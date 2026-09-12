# Exactly-Once (EOS) Example

This example demonstrates how to build Kafka-to-Kafka exactly-once processing workflows.

**This example demonstrates:**

* **Publishing input records** to an upstream Kafka topic
* **Processing records transactionally** by consuming, transforming, and producing records in a single transaction
* **Committing atomically** so produced records and consumed offsets are committed together
* **Reading only committed output** from downstream consumers
* **Recovering from failures** by aborting transactions and retrying after handler errors or panics

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

The example uses the `sample-eos-input-topic` and `sample-eos-output-topic` topics
created by the local Kafka setup.

## Run

From this directory:

```shell
go run .
```

Or from the repository root:

```shell
go run ./examples/eos
```

The HTTP server listens on:

```text
http://localhost:8080
```

## Process records exactly once (EOS)

The `POST /eos` endpoint publishes 5 input records starting from the provided ID. Each consumed batch is transformed
and written to the output topic within a Kafka transaction. The produced records and corresponding consumed offsets are
committed atomically.

```shell
curl -i -X POST 'http://localhost:8080/eos' \
  -H 'Content-Type: application/json' \
  -d '{"id":100}'
```

### Expected response

```http
HTTP/1.1 202 Accepted

published 5 EOS input records
```

### Example log

The logs show records being processed from the input topic and the committed results appearing on the output topic:

```text
  input: topic=sample-eos-input-topic key="100" id=100 attempt=1
...
eos output: topic=sample-eos-output-topic key="100" msg={ID:100 Source:sample-eos-input-topic Attempt:1}
...
```

## Abort and retry on handler error

The `POST /eos-error` endpoint publishes a single input record with ID `888`. During the first processing attempt, the
handler produces an output record inside the transaction and then intentionally returns an error.

```shell
curl -i -X POST 'http://localhost:8080/eos-error'
```

### Expected response

```http
HTTP/1.1 202 Accepted

published EOS input record id=888
```

### Example log

Because the first attempt fails, `xkafka` automatically aborts the transaction, so neither the output record nor the
consumed offset is committed. The input record is then redelivered and the second processing attempt succeeds:

```text
eos process: records=1
  input: topic=sample-eos-input-topic key="888" id=888 attempt=1
eos process: records=1
  input: topic=sample-eos-input-topic key="888" id=888 attempt=2
eos output: topic=sample-eos-output-topic key="888" msg={ID:888 Source:sample-eos-input-topic Attempt:2}
...
```

> **Verification:** No output with `Attempt:1` appears because the first transaction was aborted and its output record
> is not visible to the `read_committed` output consumer.

## Abort and retry on handler panic

The `POST /eos-panic` endpoint publishes a single input record with ID `444`. During the first processing attempt, the
handler produces an output record inside the transaction and then intentionally panics.

```shell
curl -i -X POST 'http://localhost:8080/eos-panic'
```

### Expected response

```http
HTTP/1.1 202 Accepted

published EOS input record id=444
```

### Example log

`xkafka` automatically recovers from the handler panic and aborts the transaction, so neither the output record nor the
consumed offset is committed. The input record is then redelivered and the second processing attempt succeeds:

```text
eos process: records=1
  input: topic=sample-eos-input-topic key="444" id=444 attempt=1
eos process: records=1
  input: topic=sample-eos-input-topic key="444" id=444 attempt=2
eos output: topic=sample-eos-output-topic key="444" msg={ID:444 Source:sample-eos-input-topic Attempt:2}
...
```

> **Verification:** No output with `Attempt:1` appears because the first transaction was aborted and its output record
> is not visible to the `read_committed` output consumer.

## Stop services

From the repository root:

```shell
docker compose -f examples/docker-compose.yml down --remove-orphans -v
```

Or from this example directory:

```shell
docker compose -f ../docker-compose.yml down --remove-orphans -v
```
