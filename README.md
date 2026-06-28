<div align="center">

# xkafka

**Lightweight Kafka wrapper for Go, built on top of [franz-go](https://github.com/twmb/franz-go).**

![Go Version](https://img.shields.io/badge/go-1.26%2B-blue)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

</div>

`xkafka` wraps the excellent [`franz-go`](https://github.com/twmb/franz-go) client with a compact API for common Kafka
workflows: producing messages, consuming records through handlers, committing offsets after successful processing, and
exposing Kafka observability with OpenTelemetry and Prometheus.

Runnable examples and local Kafka setup are available in [examples](examples).

## Features

* **Producer**: Synchronous, asynchronous, and transactional message publishing.
* **Consumer**: Per-record and batch handlers for regular consumer groups and share groups.
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

<!-- @formatter:off -->
```go
ctx := context.Background()

// Initialize the Producer
producer, err := kafka.NewProducer(
	kafka.WithConfig(&kafka.Config{
		Brokers: "localhost:9092",
	}),
)
if err != nil {
	log.Fatal(err)
}
defer producer.Close(ctx) // Gracefully close the producer on exit

// Initialize the Consumer
consumer, err := kafka.NewConsumer(
	kafka.WithConfig(&kafka.Config{
		Enabled: true,
		Brokers: "localhost:9092",
		Topics:  "orders.created",
		Group:   "orders-worker-group",
	}),
	kafka.WithConsumerHandler(func(ctx context.Context, record *kgo.Record) error {
		fmt.Printf("received: topic=%s key=%s value=%s\n", record.Topic, record.Key, record.Value)
		return nil // Returning nil automatically acknowledges the record
	}),
)
if err != nil {
	log.Fatal(err)
}

// Connect and verify connectivity to Kafka
if err := consumer.PreRun(ctx); err != nil {
	log.Fatal(err)
}
defer consumer.Shutdown(ctx) // Ensure all updates are flushed and connections closed on exit

// Start the consumer loop in a separate goroutine as it blocks
go func() {
	if err := consumer.Run(ctx); err != nil {
		log.Printf("consumer stopped: %v", err)
	}
}()

// Publish a test message
if err := producer.ProduceSync(ctx, &kgo.Record{
	Topic: "orders.created",
	Key:   []byte("order-1"),
	Value: []byte("created"),
}); err != nil {
	log.Fatal(err)
}
```
<!-- @formatter:on -->

> [!IMPORTANT]
> Records are considered processed only when the handler returns `nil`.
> If the handler returns an error, offsets are not committed and processing resumes after
`KAFKA_SUSPEND_PROCESSING_TIMEOUT`.

## Transactions

To enable transactional publishing, configure `TransactionalID` and use `RunInTx`.

<!-- @formatter:off -->

```go
producer, err := kafka.NewProducer(
    kafka.WithConfig(&kafka.Config{
        Brokers:         "localhost:9092",
        TransactionalID: "orders-tx-producer",
    }),
)
if err != nil {
    log.Fatal(err)
}
defer producer.Close(context.Background())

if err := producer.RunInTx(ctx, func(ctx context.Context) error {
    if err := producer.ProduceSync(ctx, &kgo.Record{
        Topic: "orders.created",
        Key:   []byte("order-1"),
        Value: []byte("created"),
    }); err != nil {
        return err
    }

    if err := producer.ProduceSync(ctx, &kgo.Record{
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

> [!NOTE]
> **Automatic Lifecycle:** `RunInTx` executes your function inside a Kafka transaction — it commits automatically on
`nil` and aborts on any returned error or panic.
>
> **Consumer Configuration:** Consumers that must ignore aborted transactional records should be configured with the
`read_committed` isolation level.

## Observability

`xkafka` uses `franz-go` hooks for Kafka client instrumentation and adds wrapper-level metrics for producer, consumer,
share consumer, and transaction workflows.

* **OpenTelemetry:** Native support for both metrics and distributed tracing.
* **Prometheus:** Built-in metrics hooks for seamless scraping.

Producer options:

<!-- @formatter:off -->
```go
kafka.WithProducerMeterProvider(meterProvider)
kafka.WithProducerTracerProvider(tracerProvider)
kafka.WithProducerMetricsNamespace("orders")
```
<!-- @formatter:on -->

Consumer options:

<!-- @formatter:off -->
```go
kafka.WithConsumerMeterProvider(meterProvider)
kafka.WithConsumerTracerProvider(tracerProvider)
kafka.WithConsumerMetricsNamespace("orders")
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
in [internal/pkg/kprom/metrics.go](internal/pkg/kprom/metrics.go).

## Configuration

`Config` can be configured directly as Go structs.

The structs also include `envconfig` tags, so you can populate them from environment variables using your preferred
configuration layer.

**Common environment variables:**

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

**Producer environment variables:**

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

**Cosnumer environment variables:**

| ENV                                | Default | Description                                                      |
| ---------------------------------- | ------: | ---------------------------------------------------------------- |
| `KAFKA_ENABLED`                    | `false` | Enables the consumer. If `false`, `PreRun` and `Run` are no-ops. |
| `KAFKA_TOPICS`                     |       — | Comma-separated list of topics to consume.                       |
| `KAFKA_GROUP`                      |       — | Consumer group ID. Required for offset commits.                  |
| `KAFKA_SHARE_GROUP`                     |       — | Share group ID. Use with share group handlers.                   |
| `KAFKA_SHARE_MAX_RECORDS`               |       — | Maximum records returned per share fetch.                        |
| `KAFKA_SHARE_MAX_RECORDS_STRICT`        | `false` | Strictly cap records per share fetch.                            |
| `KAFKA_SHARE_REJECT_AFTER_DELIVERIES`   |       — | Reject failed share records after this delivery count.           |
| `KAFKA_SHARE_RELEASE_TIMEOUT`           |       — | Delay before releasing failed share records for redelivery.      |
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
| `KAFKA_FETCH_MAX_WAIT`             |       — | Maximum broker wait time before returning a fetch response.      |
| `KAFKA_FETCH_MAX_BYTES`            |       — | Maximum bytes per fetch response.                                |
| `KAFKA_FETCH_MIN_BYTES`            |       — | Minimum bytes the broker should accumulate before responding.    |
| `KAFKA_FETCH_MAX_PARTITION_BYTES`  |       — | Maximum bytes per partition in a fetch response.                 |

## License

This project is licensed under the [MIT License](LICENSE).
