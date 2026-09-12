# Examples

This directory contains runnable examples demonstrating the main features and usage patterns of `xkafka`.

| Example                      | Demonstrates                                                                        |
|------------------------------|-------------------------------------------------------------------------------------|
| [basic](basic)               | Synchronous producing and batch consumption with `xkafka.Client`                    |
| [transactions](transactions) | Atomic producer transactions and consuming only committed records                   |
| [share_group](share_group)   | Kafka Share Groups with concurrent processing, redelivery, and rejection            |
| [eos](eos)                   | Kafka-to-Kafka exactly-once processing with `GroupTransactSession`                  |
| [otel](otel)                 | OpenTelemetry metrics and distributed tracing with native `franz-go` client metrics |

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
