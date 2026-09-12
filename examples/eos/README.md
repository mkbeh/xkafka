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

## Process records exactly once

`POST /eos` publishes five input records starting at the supplied ID. Each consumed
batch is transformed and produced to the output topic in a Kafka transaction.
The produced records and consumed input offsets for that batch are committed
atomically.

```shell
curl -i -X POST 'http://localhost:8080/eos' \
  -H 'Content-Type: application/json' \
  -d '{"id":100}'
```

Expected response:

```text
HTTP 202
published 5 EOS input records
```

Example log:

```text
  input: topic=sample-eos-input-topic key="100" id=100 attempt=1
...
eos output: topic=sample-eos-output-topic key="100" msg={ID:100 Source:sample-eos-input-topic Attempt:1}
...
```

## Abort and retry on handler error

`POST /eos-error` publishes input record `888`. The first processing attempt
produces an output record inside the transaction and then intentionally returns
an error.

```shell
curl -i -X POST 'http://localhost:8080/eos-error'
```

Expected response:

```text
HTTP 202
published EOS input record id=888
```

The first transaction is aborted, so its output record and consumed offset are
not committed. The input record is redelivered and the second attempt succeeds.

Example log:

```text
eos process: records=1
  input: topic=sample-eos-input-topic key="888" id=888 attempt=1
eos process: records=1
  input: topic=sample-eos-input-topic key="888" id=888 attempt=2
eos output: topic=sample-eos-output-topic key="888" msg={ID:888 Source:sample-eos-input-topic Attempt:2}
...
```

There is no output with `Attempt:1` because that record belonged to the aborted
transaction.

## Abort and retry on handler panic

`POST /eos-panic` publishes input record `444`. The first processing attempt
panics after producing its transactional output record.

```shell
curl -i -X POST 'http://localhost:8080/eos-panic'
```

Expected response:

```text
HTTP 202
published EOS input record id=444
```

The transaction is aborted and the input record is redelivered. The second
attempt succeeds, so the output consumer sees only the committed retry:

```text
eos output: topic=sample-eos-output-topic key="444" msg={ID:444 Source:sample-eos-input-topic Attempt:2}
```

## Stop services

From the repository root:

```shell
docker compose -f examples/docker-compose.yml down --remove-orphans -v
```

Or from this example directory:

```shell
docker compose -f ../docker-compose.yml down --remove-orphans -v
```
