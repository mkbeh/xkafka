<div align="center">

# Kafka toolkit for Go

**Lightweight Kafka wrapper for Go, built on top of [franz-go](https://github.com/twmb/franz-go).**

[![Go Reference](https://pkg.go.dev/badge/github.com/mkbeh/xkafka.svg)](https://pkg.go.dev/github.com/mkbeh/xkafka)
[![Test](https://github.com/mkbeh/xkafka/actions/workflows/test.yml/badge.svg)](https://github.com/mkbeh/xkafka/actions/workflows/test.yml)
[![Coverage](https://codecov.io/gh/mkbeh/xkafka/graph/badge.svg)](https://codecov.io/gh/mkbeh/xkafka)

</div>

`xkafka` preserves the native [franz-go](https://github.com/twmb/franz-go) record model and configuration while adding
a compact runtime layer for handler-driven consumption, retries, Kafka Share Groups, producer transactions, and
Kafka-to-Kafka exactly-once processing (EOS).

Kafka behavior remains fully configurable through franz-go. Rather than introducing a parallel configuration layer,
`xkafka` focuses on the processing lifecycle around it.

## Features

* **Unified Client:** Produce and consume through a single client.
* **Native Configuration:** Use native franz-go options without introducing a parallel Kafka configuration layer.
* **Flexible Producing:** Synchronous, asynchronous, non-blocking, and transactional produce operations.
* **Batch Consumption:** A unified handler model for regular consumer groups and Kafka Share Groups.
* **Retries and Recovery:** Configurable handler retries and backoff with panic recovery through the same processing
  path.
* **Share Groups (KIP-932):** Record acknowledgement, release, redelivery, delivery-count based rejection, and
  acknowledgement flushing.
* **Producer Transactions:** Transactional producing with automatic commit and abort handling.
* **Exactly-Once Semantics (EOS):** Kafka-to-Kafka consume-process-produce transactions with atomic produced records and
  consumed offsets.
* **Runtime Hooks:** Extensible hooks for producing, fetching, processing, offset commits, Share Groups, and
  transactions.
* **OpenTelemetry:** Optional runtime metrics, distributed tracing, and context propagation.

## Installation

This repository contains the core `xkafka` module. The core module is released from the repository root:

```shell
go get github.com/mkbeh/xkafka
```

Optional integrations are released independently under `extra`:

```shell
go get github.com/mkbeh/xkafka/extra/otelxkafka
```

## Getting started

Here's a basic overview of producing and consuming:

<!-- @formatter:off -->

```go
seeds := []string{"localhost:9092"}

// One client can both produce and consume!
client, err := xkafka.NewClient(
    xkafka.WithKafkaOptions(
        kgo.SeedBrokers(seeds...),
        kgo.ConsumeTopics("foo"),
        kgo.ConsumerGroup("my-group-identifier"),
    ),
    xkafka.WithBatchHandler(func(ctx context.Context, records []*kgo.Record) error {
        for _, record := range records {
            fmt.Printf("received: %s\n", record.Value)
        }

        return nil
    }),
)
if err != nil {
    panic(err)
}
defer client.Shutdown(context.Background())

// 1.) Producing a message.
record := &kgo.Record{Topic: "foo", Value: []byte("value")}
if err := client.ProduceSync(context.Background(), record); err != nil {
    panic(err)
}

// 2.) Consuming messages through the configured batch handler.
if err := client.HandleFetches(context.Background()); err != nil {
    panic(err)
}
```

<!-- @formatter:on -->

This only shows producing and consuming in the most basic sense. The sections below cover transactions, Kafka Share
Groups, exactly-once processing, and telemetry in more detail. Check out the [examples](examples) directory for more!

## Transactions

Configure a `TransactionalID` and execute transactional operations using the `Tx` passed to `RunInTx`:

<!-- @formatter:off -->

```go
seeds := []string{"localhost:9092"}

// Initialize a client with a transactional ID.
client, err := xkafka.NewClient(
    xkafka.WithKafkaOptions(
        kgo.SeedBrokers(seeds...),
        kgo.TransactionalID("my-tx-identifier"),
    ),
)
if err != nil {
    panic(err)
}
defer client.Shutdown(context.Background())

// RunInTx manages the transaction lifecycle.
err = client.RunInTx(context.Background(), func(ctx context.Context, tx *xkafka.Tx) error {
    record := &kgo.Record{
        Topic: "foo",
        Value: []byte("value"),
    }

    // Produce transactional records through the provided Tx.
    return tx.ProduceSync(ctx, record)
})
if err != nil {
    panic(err)
}
```

<!-- @formatter:on -->

The transaction function controls how `xkafka` completes the transaction:

| Function Result | Behavior                                                         |
|:---------------:|------------------------------------------------------------------|
| `nil`           | Flushes buffered records and attempts to commit the transaction. |
| `error`         | Attempts to abort the transaction and returns the error.         |
| `panic`         | Recovers the panic, attempts to abort the transaction, and returns an error. |

## Share Groups

Kafka Share Groups use the same batch handler API as standard consumer groups, but processing results are translated
into record-level acknowledgements rather than consumer-group offset semantics.

<!-- @formatter:off -->

```go
seeds := []string{"localhost:9092"}

// Configure a client as a Share Group consumer.
client, err := xkafka.NewClient(
    xkafka.WithKafkaOptions(
        kgo.SeedBrokers(seeds...),
        kgo.ConsumeTopics("foo"),
        kgo.ShareGroup("my-share-group"),
    ),
    xkafka.WithBatchHandler(func(ctx context.Context, records []*kgo.Record) error {
        for _, record := range records {
            // DeliveryCount reports how many times the record has been delivered.
            fmt.Printf("received: %s, delivery_count=%d\n", record.Value, record.DeliveryCount())
        }

        // Returning nil acknowledges all records in the batch with AckAccept.
        return nil
    }),
)
if err != nil {
    panic(err)
}
defer client.Shutdown(context.Background())

// Start the blocking Share Group consumption loop.
if err := client.HandleFetches(context.Background()); err != nil {
    panic(err)
}
```

<!-- @formatter:on -->

For Share Groups, handler outcomes are mapped to record-level Kafka acknowledgements:

| Handler Result | Behavior                                                                                                                                                                                                                                        |
|:--------------:|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `nil`          | Acknowledges all records in the batch with `AckAccept`.                                                                                                                                                                                         |
| `error`        | Uses `AckRelease` for broker redelivery. `WithShareReleaseTimeout` can delay acknowledgement flushing, while `WithShareRejectAfterDeliveries` changes failed records to `AckReject` once their delivery count reaches the configured threshold. |
| `panic`        | Recovers the panic and follows the same failure path as an error, including the configured release delay and delivery-count rejection policy.                                                                                                   |

## Exactly-Once Semantics (EOS)

Use `GroupTransactSession` for Kafka-to-Kafka consume-process-produce workflows where produced records and consumed
offsets must be committed atomically.

The session coordinates the consumer group and transactional producer lifecycles, requiring a consumer group,
`TransactionalID`, and transaction batch handler.

<!-- @formatter:off -->

```go
seeds := []string{"localhost:9092"}

// Initialize a Kafka-to-Kafka exactly-once processing session.
session, err := xkafka.NewGroupTransactSession(
    xkafka.WithKafkaOptions(
        kgo.SeedBrokers(seeds...),
        kgo.ConsumeTopics("foo-input"),
        kgo.ConsumerGroup("my-group-identifier"),
        kgo.TransactionalID("my-tx-identifier"),
    ),
    xkafka.WithGroupTransactSessionBatchHandler(
        func(ctx context.Context, records []*kgo.Record, tx *xkafka.Tx) error {
            for _, record := range records {
                // 1.) Process the input record and produce the result through Tx.
                out := &kgo.Record{
                    Topic: "foo-output",
                    Value: record.Value,
                }

                if err := tx.ProduceSync(ctx, out); err != nil {
                    return err
                }
            }

            // 2.) Returning nil allows the produced records and consumed offsets
            // to be committed atomically in the same Kafka transaction.
            return nil
        },
    ),
)
if err != nil {
    panic(err)
}
defer session.Shutdown(context.Background())

// Start the blocking transactional processing loop.
if err := session.HandleFetches(context.Background()); err != nil {
    panic(err)
}
```

<!-- @formatter:on -->

Each fetched batch is processed inside a single group transaction:

| Handler Result | Behavior                                                                                 |
|:--------------:|------------------------------------------------------------------------------------------|
| `nil`          | Attempts to atomically commit the produced records and consumed offsets.                 |
| `error`        | Attempts to abort the transaction, leaving the input offsets uncommitted for redelivery. |
| `panic`        | Recovers the panic and follows the same abort path as an error.                          |

> [!IMPORTANT]
> Unrecoverable transaction errors are returned from `HandleFetches` and stop the session. Handler errors and panics
> abort the current transaction but do not stop the fetch loop by themselves.

## Telemetry

The core library is telemetry-agnostic, exposing runtime hooks for processing, fetching, producing, offset commits,
Share Groups, and transactions.

Optional OpenTelemetry integration is provided by [extra/otelxkafka](extra/otelxkafka), which adds runtime metrics,
distributed tracing, and context propagation. It can be used alongside
[franz-go/plugin/kotel](https://github.com/twmb/franz-go/tree/master/plugin/kotel) to collect native franz-go client
metrics.

See [extra/otelxkafka](extra/otelxkafka) for the complete setup guide and telemetry reference.

## License

This project is licensed under the [MIT License](LICENSE).
