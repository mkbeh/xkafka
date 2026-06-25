<div align="center">

# xkafka

**Lightweight Kafka wrapper for Go, built on top of [franz-go](https://github.com/twmb/franz-go).**

![Go Version](https://img.shields.io/badge/go-1.26%2B-blue)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

</div>

`xkafka` wraps the excellent [`franz-go`](https://github.com/twmb/franz-go) client with a compact API for common Kafka
workflows: producing messages, consuming records through handlers, committing offsets after successful processing, and
exposing Kafka observability with OpenTelemetry and Prometheus.

## Features

* **Producer**: Synchronous, asynchronous, and transactional message publishing.
* **Consumer**: Per-record and batch handlers for flexible message processing.
* **Commits**: Automatic offset commits only after successful processing.
* **Retries**: Built-in retry handling with configurable backoff strategies.
* **Observability**: OpenTelemetry instrumentation and Prometheus metrics hooks out of the box.
* **Security**: TLS and SASL support, including `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`.
* **Configuration**: Configure via Go structs or environment variables.

## Installation

```bash
go get github.com/mkbeh/xkafka
```

## Quick start

The examples below show the basic sending and receiving flow. For production workloads, tune retries, offset commits,
observability, security, and graceful shutdown behavior for your needs.

### Sending messages

```go
package main

import (
	"context"
	"log"

	kafka "github.com/mkbeh/xkafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	producer, err := kafka.NewProducer(
		kafka.WithProducerConfig(&kafka.ProducerConfig{
			Brokers: "localhost:9092",
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close(context.Background())

	ctx := context.Background()

	// Async (fire-and-forget with logging on error)
	producer.ProduceAsync(ctx, &kgo.Record{
		Topic: "my-topic",
		Value: []byte("hello async"),
	})

	// Sync produce waits until the broker acknowledges the record.
	if err := producer.ProduceSync(ctx, &kgo.Record{
		Topic: "my-topic",
		Value: []byte("hello sync"),
	}); err != nil {
		log.Fatal(err)
	}
}
```

### Receiving messages

```go
package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/mkbeh/xkafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	consumer, err := kafka.NewConsumer(
		kafka.WithConsumerConfig(&kafka.ConsumerConfig{
			Enabled: true,
			Brokers: "localhost:9092",
			Topics:  "my-topic",
			Group:   "my-group",
		}),
		kafka.WithConsumerHandler(func(ctx context.Context, record *kgo.Record) error {
			fmt.Printf("key=%s value=%s\n", record.Key, record.Value)
			return nil
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	if err := consumer.PreRun(ctx); err != nil {
		log.Fatal(err)
	}
	defer consumer.Shutdown(ctx)

	if err := consumer.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

```

### Receiving messages in batches

<!-- @formatter:off -->

```go
consumer, err := kafka.NewConsumer(
    kafka.WithConsumerConfig(&kafka.ConsumerConfig{
        Enabled:        true,
        Brokers:        "localhost:9092",
        Topics:         "orders.created",
        Group:          "orders-batch-worker-group",
        MaxPollRecords: 500,
    }),
    kafka.WithConsumerBatchHandler(func(ctx context.Context, records []*kgo.Record) error {
        for _, record := range records {
            // process record
        }

        return nil
    }),
)
```

<!-- @formatter:on -->

> **Note:** Offsets are committed only after successful handler execution.
> If the handler returns an error, processing is retried after `KAFKA_SUSPEND_PROCESSING_TIMEOUT` (`30s` by default).

## Transactions

To enable transactional publishing, configure `TransactionalID` and use `RunInTx`.

A producer configured with `TransactionalID` should publish records inside `RunInTx`.

For regular sync and async message publishing, use a producer without `TransactionalID`.
For transactional message publishing, use a dedicated producer instance with `TransactionalID`.

<!-- @formatter:off -->
```go
if err := txProducer.RunInTx(ctx, func(ctx context.Context) error {
    if err := txProducer.ProduceSync(ctx, &kgo.Record{
        Topic: "orders.created",
        Key:   []byte("order-1"),
        Value: []byte("created"),
    }); err != nil {
        return err
    }

    if err := txProducer.ProduceSync(ctx, &kgo.Record{
        Topic: "audit.events",
        Key:   []byte("order-1"),
        Value: []byte("order created"),
    }); err != nil {
        return err
    }

    return nil
}); err != nil {
    log.Fatal(err)
}
```
<!-- @formatter:on -->

`RunInTx` opens a Kafka transaction, runs the provided function, and commits the transaction if the function returns
`nil`.

If the function returns an error, the transaction is aborted.

Use `ProduceSync` inside `RunInTx`. `ProduceAsync` is not recommended inside a transaction because the function may
return before the asynchronous produce result is known.

Consumers that should ignore aborted transactional records must use `read_committed` isolation level:

<!-- @formatter:off -->

```go
consumer, err := kafka.NewConsumer(
    kafka.WithConsumerConfig(&kafka.ConsumerConfig{
        Enabled: true,
        Brokers: "localhost:9092",
        Topics:  "orders.created",
        Group:   "orders-worker-group",
    }),
    kafka.WithFetchIsolationLevel(kgo.ReadCommitted()),
    kafka.WithConsumerHandler(handler),
)
```

<!-- @formatter:on -->

## Observability

`xkafka` uses franz-go hooks for Kafka client instrumentation and adds wrapper-level metrics for producer, consumer, and
transaction workflows.

It supports:

* OpenTelemetry metrics and tracing
* Prometheus metrics
* Custom metrics namespace

### Producer instrumentation

<!-- @formatter:off -->

```go
producer, err := kafka.NewProducer(
    kafka.WithProducerConfig(&kafka.ProducerConfig{
        Brokers: "localhost:9092",
    }),
    kafka.WithProducerMeterProvider(meterProvider),
    kafka.WithProducerTracerProvider(tracerProvider),
    kafka.WithProducerMetricsNamespace("orders"),
)
```

<!-- @formatter:on -->

### Consumer instrumentation

<!-- @formatter:off -->

```go
consumer, err := kafka.NewConsumer(
    kafka.WithConsumerConfig(&kafka.ConsumerConfig{
        Enabled: true,
        Brokers: "localhost:9092",
        Topics:  "orders.created",
        Group:   "orders-worker-group",
    }),
    kafka.WithConsumerMeterProvider(meterProvider),
    kafka.WithConsumerTracerProvider(tracerProvider),
    kafka.WithConsumerMetricsNamespace("orders"),
    kafka.WithConsumerHandler(handler),
)
```

<!-- @formatter:on -->

### Metric naming

Prometheus metrics use the following naming pattern:

```text
<namespace>_kafka_<metric_name>
```

For example, with namespace `orders`:

```text
orders_kafka_produce_records_total
orders_kafka_transactions_total
orders_kafka_transaction_duration_seconds
```

If namespace is empty, metrics are exported with the `kafka_` prefix:

```text
kafka_produce_records_total
kafka_transactions_total
kafka_transaction_duration_seconds
```

For the full list of exported Prometheus metrics, see the package comment
in [internal/pkg/kprom/metrics.go](internal/pkg/kprom/kprom.go).

## Configuration

`ProducerConfig` and `ConsumerConfig` can be configured directly as Go structs.
Their fields also include `envconfig` tags, so you can populate them from environment variables using your preferred
configuration layer.

### Common configuration

| ENV                       | Description                                                            |
| ------------------------- | ---------------------------------------------------------------------- |
| `KAFKA_BROKERS`           | Comma-separated seed brokers, for example `kafka-1:9092,kafka-2:9092`. |
| `KAFKA_SASL_MECHANISM`    | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`.          |
| `KAFKA_USER`              | SASL username.                                                         |
| `KAFKA_PASSWORD`          | SASL password.                                                         |
| `KAFKA_REQUEST_RETRIES`   | Number of retries for retryable requests.                              |
| `KAFKA_RETRY_TIMEOUT`     | Total retry time limit.                                                |
| `KAFKA_DIAL_TIMEOUT`      | Broker dial timeout.                                                   |
| `KAFKA_CONN_IDLE_TIMEOUT` | Idle connection timeout.                                               |
| `KAFKA_METADATA_MAX_AGE`  | Maximum age of cached metadata.                                        |
| `KAFKA_METADATA_MIN_AGE`  | Minimum time between metadata refreshes.                               |
| `KAFKA_MAX_WRITE_BYTES`   | Maximum bytes written to a broker connection in one write.             |
| `KAFKA_MAX_READ_BYTES`    | Maximum bytes read from a broker response.                             |

### Producer configuration

| ENV                              | Description                                                  |
| -------------------------------- | ------------------------------------------------------------ |
| `KAFKA_DEFAULT_PRODUCE_TOPIC`    | Default topic used when `kgo.Record.Topic` is empty.         |
| `KAFKA_PRODUCER_BATCH_MAX_BYTES` | Maximum size of a producer batch.                            |
| `KAFKA_MAX_BUFFERED_RECORDS`     | Maximum number of buffered records before producing blocks.  |
| `KAFKA_MAX_BUFFERED_BYTES`       | Maximum number of buffered bytes before producing blocks.    |
| `KAFKA_PRODUCE_REQUEST_TIMEOUT`  | Maximum time a broker has to respond to a produce request.   |
| `KAFKA_RECORD_RETRIES`           | Number of record-level produce retries.                      |
| `KAFKA_PRODUCER_LINGER`          | Time to wait for more records before building a batch.       |
| `KAFKA_RECORD_DELIVERY_TIMEOUT`  | Maximum time a record can remain buffered before timing out. |
| `KAFKA_TRANSACTIONAL_ID`         | Enables transactional producing.                             |
| `KAFKA_TRANSACTION_TIMEOUT`      | Maximum allowed transaction duration.                        |

### Consumer configuration

| ENV                                | Default | Description                                                      |
| ---------------------------------- | ------: | ---------------------------------------------------------------- |
| `KAFKA_ENABLED`                    | `false` | Enables the consumer. If `false`, `PreRun` and `Run` are no-ops. |
| `KAFKA_TOPICS`                     |       — | Comma-separated list of topics to consume.                       |
| `KAFKA_GROUP`                      |       — | Consumer group ID. Required for offset commits.                  |
| `KAFKA_MAX_POLL_RECORDS`           |   `100` | Maximum records returned per poll.                               |
| `KAFKA_POLL_INTERVAL`              | `300ms` | Interval between polls.                                          |
| `KAFKA_SKIP_FATAL_ERRORS`          |  `true` | Continue after non-retryable fetch errors.                       |
| `KAFKA_SUSPEND_PROCESSING_TIMEOUT` |   `30s` | Backoff after handler error.                                     |
| `KAFKA_SUSPEND_COMMITTING_TIMEOUT` |   `10s` | Backoff after offset commit error.                               |
| `KAFKA_INSTANCE_ID`                |       — | Static group member ID.                                          |
| `KAFKA_CONSUME_REGEX`              | `false` | Treat configured topics as regular expressions.                  |
| `KAFKA_DISABLE_FETCH_SESSIONS`     | `false` | Disable Kafka fetch sessions.                                    |
| `KAFKA_SESSION_TIMEOUT`            |   `45s` | Maximum time between heartbeats before rebalance.                |
| `KAFKA_REBALANCE_TIMEOUT`          |       — | Maximum time for members to rejoin during rebalance.             |
| `KAFKA_HEARTBEAT_INTERVAL`         |       — | Heartbeat interval.                                              |
| `KAFKA_REQUIRE_STABLE_FETCH_OFFS`  | `false` | Require stable offsets before consuming.                         |
| `KAFKA_FETCH_MAX_WAIT`             |       — | Maximum broker wait time before returning a fetch response.      |
| `KAFKA_FETCH_MAX_BYTES`            |       — | Maximum bytes per fetch response.                                |
| `KAFKA_FETCH_MIN_BYTES`            |       — | Minimum bytes the broker should accumulate before responding.    |
| `KAFKA_FETCH_MAX_PARTITION_BYTES`  |       — | Maximum bytes per partition in a fetch response.                 |

## License

This project is licensed under the [MIT License](LICENSE).