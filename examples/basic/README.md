# Basic Example

This example demonstrates the core `xkafka.Client` produce and consume workflow without external metrics integrations.

**This example demonstrates:**

* creating one client for producing and consuming;
* explicitly checking broker connectivity with `Ping`;
* synchronously producing a record;
* consuming records through a batch handler;
* inspecting the lightweight `Client.Stats()` snapshot;
* graceful client shutdown.

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

The consumer prints the processed record:

```text
consume: topic=sample-topic partition=0 offset=0 key="basic" msg={ID:42 Text:hello from xkafka}
```

Partition and offset values depend on the Kafka topic state.

## Statistics

`xkafka` keeps lightweight cumulative statistics in the core client.

```shell
curl -s 'http://localhost:8080/stats' | jq
```

The endpoint returns the current `Client.Stats()` snapshot directly as JSON. No Prometheus or OpenTelemetry metrics
integration is required.

After producing and consuming a record, handler counters and cumulative handler duration should increase.

## Stop

Press `Ctrl+C` to stop the HTTP server, polling loop, and Kafka client gracefully.
