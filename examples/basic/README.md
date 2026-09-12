# Basic Example

This example demonstrates the basic `xkafka.Client` workflow for producing and consuming Kafka records.

**This example demonstrates:**

* **Creating a dual-purpose client** for both producing and consuming
* **Checking Kafka connectivity** before starting the application workflow
* **Publishing records synchronously** to Kafka
* **Processing consumed records** in batches
* **Managing the application lifecycle** with polling error handling and graceful shutdown

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

The example uses the `sample-topic` topic created by the local Kafka setup.

## Run

From this directory:

```shell
go run .
```

Or from the repository root:

```shell
go run ./examples/basic
```

The HTTP server listens on:

```text
http://localhost:8080
```

## Produce a record

```shell
curl -i -X POST 'http://localhost:8080/produce'
```

Expected response:

```text
HTTP 204
```

Example log:

```text
consume: topic=sample-topic key="basic" msg={ID:42 Text:hello from xkafka}
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
