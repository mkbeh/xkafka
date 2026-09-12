# Examples

This directory contains runnable examples demonstrating the main features and usage patterns of `xkafka`.

| Example                        | Demonstrates                                                                      |
|--------------------------------|-----------------------------------------------------------------------------------|
| [`basic`](basic)               | Synchronous producing and batch consumption using `xkafka.Client`.                |
| [`transactions`](transactions) | Atomic producer transactions and `read_committed` isolation level.                |
| [`share_group`](share_group)   | Kafka Share Groups with concurrent scaling, record redelivery, and rejection.     |
| [`eos`](eos)                   | Exactly-Once Semantics (EOS) pipelines using `GroupTransactSession`.              |
| [`otel`](otel)                 | OpenTelemetry metrics, distributed tracing, and native `franz-go` client metrics. |

## Running the examples

The examples use Docker Compose to start Kafka and the required supporting services.

From the `examples` directory, start the local environment:

```shell
docker compose up -d
```

Then run an example from its directory:

```shell
cd basic
go run .
```

Refer to the README in the corresponding example directory for the available scenarios, commands, and expected behavior.
