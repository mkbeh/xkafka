# Env Configuration Example

This example shows how to load `xkafka.Config` from environment variables using `github.com/caarlos0/env`.

**This example demonstrates:**

* loading Kafka configuration from environment variables;
* creating an xkafka client from the loaded config;
* producing one record to the configured topic;
* keeping env parsing outside the main xkafka module dependencies.

## Configuration

Configure Kafka connection and default produce topic using environment variables:

```text
KAFKA_BROKERS=localhost:29092
KAFKA_DEFAULT_PRODUCE_TOPIC=sample-env-topic
```

## Run

From this directory:

```shell
go run main.go
```

Or from the repository root:

```shell
cd examples/env
go run main.go
```

Expected result:

```text
produced message: topic=sample-env-topic key="example-key" value="hello from xkafka env example"
```
