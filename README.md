<div align="center">

# xkafka

**Lightweight Kafka wrapper for Go, built on top of [franz-go](https://github.com/twmb/franz-go).**

![Go Version](https://img.shields.io/badge/go-1.26%2B-blue)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

</div>

`xkafka` wraps the excellent [`franz-go`](https://github.com/twmb/franz-go) client with a compact API for common Kafka
workflows: producing messages, consuming records through handlers, committing offsets after successful processing,
using Kafka transactions, building Kafka-to-Kafka exactly-once processing with `GroupTransactSession`, and exposing
Kafka observability with OpenTelemetry and Prometheus.

Explore ready-to-run use cases in [examples](examples).

## Features

* **Client**: Unified client for both producing and consuming.
* **Producing**: Synchronous, asynchronous, and transactional publishing.
* **Consuming**: Batch handlers for regular consumer groups.
* **Share Groups**: Batch consumption with flexible Ack management.
* **EOS Processing**: Exactly-once Kafka-to-Kafka processing via `GroupTransactSession`.
* **Commits**: Safe offset commits executing only on success.
* **Observability**: OpenTelemetry and Prometheus support.
* **Security**: Native TLS and SASL (`PLAIN`, `SCRAM-SHA-256/512`).
* **Configuration**: Setup via Go structs or environment variables.

## Installation

```bash
go get github.com/mkbeh/xkafka
```

## Quick start

These examples show basic producing and consuming flows. For production workloads, tune retries, offset commits,
security, and observability for your needs.

<!-- @formatter:off -->
```go
ctx := context.Background()

// Initialize the unified Kafka client.
client, err := xkafka.NewClient(
	xkafka.WithConfig(&xkafka.Config{
		Enabled:             true,
		Brokers:             "localhost:9092",
		DefaultProduceTopic: "orders.created",
		Topics:              "orders.created",
		Group:               "orders-worker-group",
	}),
	xkafka.WithConsumerBatchHandler(func(ctx context.Context, records []*kgo.Record) error {
		for _, record := range records {
			fmt.Printf("received: topic=%s key=%s value=%s\n", record.Topic, record.Key, record.Value)
		}

		return nil // Returning nil commits offsets after successful batch processing.
	}),
)
if err != nil {
	log.Fatal(err)
}
defer func() {
	if err := client.Shutdown(ctx); err != nil {
		log.Printf("shutdown failed: %v", err)
	}
}()

// Start the blocking consumer loop in a separate goroutine.
go func() {
	if err := client.HandleFetches(ctx); err != nil {
		log.Printf("consumer stopped: %v", err)
	}
}()

// Publish a synchronous message using DefaultProduceTopic.
if err := client.ProduceSync(ctx, &kgo.Record{
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
> `KAFKA_SUSPEND_PROCESSING_TIMEOUT`.

## Transactions

To enable transactional publishing, configure `TransactionalID` and use `RunInTx`.

<!-- @formatter:off -->
```go
client, err := xkafka.NewClient(
	xkafka.WithConfig(&xkafka.Config{
		Brokers:         "localhost:9092",
		TransactionalID: "orders-tx-producer",
	}),
)
if err != nil {
	log.Fatal(err)
}
defer func() {
	if err := client.Shutdown(context.Background()); err != nil {
		log.Printf("shutdown failed: %v", err)
	}
}()

if err := client.RunInTx(ctx, func(ctx context.Context, tx *xkafka.Tx) error {
	if err := tx.ProduceSync(ctx, &kgo.Record{
		Topic: "orders.created",
		Key:   []byte("order-1"),
		Value: []byte("created"),
	}); err != nil {
		return err
	}

	if err := tx.ProduceSync(ctx, &kgo.Record{
		Topic: "audit.events",
		Key:   []byte("order-1"),
		Value: []byte("audited" ),
	}); err != nil {
		return err
	}

	return nil // Commits automatically. Any returned error or panic aborts the transaction.
}); err != nil {
	log.Fatal(err)
}
```
<!-- @formatter:on -->

> [!NOTE]
> `RunInTx` executes your function inside a Kafka transaction, automatically committing on `nil` and aborting on returned errors or panics. If a panic occurs, the transaction is aborted, and the original panic is re-thrown.
>
> To ensure consumers ignore these aborted records, configure them with `kgo.ReadCommitted()` using `WithFetchIsolationLevel`.


## Share Groups

Use `WithConsumerShareBatchHandler` with `ShareGroup` to consume through Kafka Share Groups.

<!-- @formatter:off -->
```go
client, err := xkafka.NewClient(
	xkafka.WithConfig(&xkafka.Config{
		Enabled: true,
		Brokers: "localhost:9092",
		Topics:  "orders.created",

		ShareGroup:      "orders-share-group",
		ShareMaxRecords: 5,
		MaxPollRecords:  5,

		ShareRejectAfterDeliveries: 3,
		ShareReleaseTimeout:        5 * time.Second,
	}),
	xkafka.WithConsumerShareBatchHandler(func(ctx context.Context, records []*kgo.Record) error {
		for _, record := range records {
			fmt.Printf("share record: topic=%s value=%s\n", record.Topic, record.Value)
		}

		return nil
	}),
)
if err != nil {
	log.Fatal(err)
}
defer client.Shutdown(ctx)

if err := client.HandleFetches(ctx); err != nil {
	log.Fatal(err)
}
```
<!-- @formatter:on -->

> [!NOTE]
> **Share Group Ack Rules**
>
> * `handler success` - `AckAccept`
> * `handler error` - `AckRelease`
> * `delivery limit` - `AckReject` if `ShareRejectAfterDeliveries > 0`; otherwise records keep being released with
    `AckRelease`

## Exactly-Once Semantics

Use `GroupTransactSession` for exactly-once (Kafka-to-Kafka) processing. It commits consumed offsets and produced
records atomically in the same Kafka transaction.

<!-- @formatter:off -->
```go
session, err := xkafka.NewGroupTransactSession(
	xkafka.WithConfig(&xkafka.Config{
		Enabled:         true,
		Brokers:         "localhost:9092",
		Topics:          "orders.input",
		Group:           "orders-eos-group",
		TransactionalID: "orders-eos-session",
	}),
	xkafka.WithGroupTransactSessionBatchHandler(
		func(ctx context.Context, records []*kgo.Record, tx *xkafka.GroupTransactSession) error {
			for _, record := range records {
				if err := tx.ProduceSync(ctx, &kgo.Record{
					Topic: "orders.output",
					Key:   record.Key,
					Value: record.Value,
				}); err != nil {
					return err
				}
			}

			return nil // Commits consumed offsets and produced records atomically.
		},
	),
)
if err != nil {
	log.Fatal(err)
}
defer func() {
	if err := session.Shutdown(ctx); err != nil {
		log.Printf("shutdown failed: %v", err)
	}
}()

if err := session.HandleFetches(ctx); err != nil {
	log.Fatal(err)
}
```
<!-- @formatter:on -->

> [!NOTE]
> If the handler returns an error, the group transaction is aborted and consumed offsets are not committed.

## Observability

`xkafka` uses `franz-go` hooks for Kafka client instrumentation and adds wrapper-level metrics for producer, consumer,
share group, and transaction workflows.

* **OpenTelemetry:** Metrics and distributed tracing via `franz-go` hooks.
* **Prometheus:** Wrapper-level metrics for produce, consume, share group, and transaction operations.

### Instrumentation Options

```go
xkafka.WithMeterProvider(meterProvider)
xkafka.WithTracerProvider(tracerProvider)
xkafka.WithTracerPropagator(propagator)
xkafka.WithMetricsNamespace("orders")
xkafka.WithMetricLabel("service", "orders-api")
```

`xkafka` does not depend on a specific tracing backend. Provide an OpenTelemetry `TracerProvider` through
`WithTracerProvider`, and export traces using your application or OpenTelemetry Collector pipeline.

A runnable tracing example is available in [examples/tracing](examples/tracing).

### Metric naming

Prometheus metrics follow the `[<namespace>_]kafka_<metric_name>` layout. For example, using the `orders` namespace:

```text
orders_kafka_produce_errors_total
orders_kafka_consume_handle_duration_seconds
orders_kafka_consume_errors_total
orders_kafka_transactions_total
orders_kafka_transaction_duration_seconds
```

> [!NOTE]
> Low-level `franz-go` client metrics and traces are exported through OpenTelemetry hooks. `xkafka` adds wrapper-level
> Prometheus metrics around producer, consumer, share group, and transaction workflows.
>
> For the full list of exported Prometheus metrics, see [internal/pkg/kprom/metrics.go](internal/pkg/kprom/metrics.go).

## Configuration

`Config` can be initialized directly as a Go struct or populated from environment variables by your application
configuration layer.

`Config` uses `env` tags and can be loaded with [caarlos0/env](https://github.com/caarlos0/env).
See [examples/env](examples/env).

### Environment variables

| Variable                            | Default | Description                                                               |
|:------------------------------------|:-------:|:--------------------------------------------------------------------------|
| **Common**                          |         |                                                                           |
| `KAFKA_BROKERS`                     |    —    | Comma-separated seed brokers                                              |
| `KAFKA_SASL_MECHANISM`              |    —    | `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`                              |
| `KAFKA_USER`                        |    —    | SASL authentication username                                              |
| `KAFKA_PASSWORD`                    |    —    | SASL authentication password                                              |
| `KAFKA_REQUEST_TIMEOUT_OVERHEAD`    |    —    | Extra time added while setting request deadlines                          |
| `KAFKA_REQUEST_RETRIES`             |    —    | Maximum number of request retries                                         |
| `KAFKA_RETRY_TIMEOUT`               |    —    | Total retry time limit for requests                                       |
| `KAFKA_DIAL_TIMEOUT`                |    —    | Broker dial timeout                                                       |
| `KAFKA_CONN_IDLE_TIMEOUT`           |    —    | Idle connection timeout                                                   |
| `KAFKA_METADATA_MAX_AGE`            |    —    | Maximum age of cached metadata                                            |
| `KAFKA_METADATA_MIN_AGE`            |    —    | Minimum time between metadata refreshes                                   |
| `KAFKA_MAX_WRITE_BYTES`             |    —    | Maximum bytes written per connection write                                |
| `KAFKA_MAX_READ_BYTES`              |    —    | Maximum bytes read from a broker response                                 |
| `KAFKA_ALWAYS_RETRY_EOF`            | `false` | Retry EOF errors instead of treating them as terminal connection failures |
| **Producer**                        |         |                                                                           |
| `KAFKA_DEFAULT_PRODUCE_TOPIC`       |    —    | Fallback topic if `kgo.Record.Topic` is empty                             |
| `KAFKA_PRODUCER_BATCH_MAX_BYTES`    |    —    | Maximum size of a producer batch                                          |
| `KAFKA_MAX_BUFFERED_RECORDS`        |    —    | Maximum buffered records before producing blocks                          |
| `KAFKA_MAX_BUFFERED_BYTES`          |    —    | Maximum buffered bytes before producing blocks                            |
| `KAFKA_PRODUCE_REQUEST_TIMEOUT`     |    —    | Broker response timeout for produce requests                              |
| `KAFKA_RECORD_RETRIES`              |    —    | Number of record-level produce retries                                    |
| `KAFKA_RECORD_DELIVERY_TIMEOUT`     |    —    | Maximum buffer time for a record before timeout                           |
| `KAFKA_PRODUCER_LINGER`             |    —    | Delay used to wait for more records before building a producer batch      |
| `KAFKA_TRANSACTIONAL_ID`            |    —    | Unique identifier to enable transactional producing                       |
| `KAFKA_TRANSACTION_TIMEOUT`         |    —    | Maximum allowed transaction duration                                      |
| **Consumer**                |         |                                                                           |
| `KAFKA_ENABLED`                     | `true`  | Enables the consumer loop                                                 |
| `KAFKA_TOPICS`                      |    —    | Comma-separated list of topics to consume                                 |
| `KAFKA_GROUP`                       |    —    | Consumer group ID required for offset commits                             |
| `KAFKA_MAX_POLL_RECORDS`            |  `100`  | Maximum records handled per poll iteration                                |
| `KAFKA_POLL_INTERVAL`               |  `1s`   | Interval between poll iterations                                          |
| `KAFKA_SKIP_FATAL_ERRORS`           | `true`  | Continue after non-retryable fetch errors                                 |
| `KAFKA_SUSPEND_PROCESSING_TIMEOUT`  |  `30s`  | Backoff delay after a handler error                                       |
| `KAFKA_SUSPEND_COMMITTING_TIMEOUT`  |  `10s`  | Backoff delay after a commit or ack error                                 |
| `KAFKA_INSTANCE_ID`                 |    —    | Static group membership identifier                                        |
| `KAFKA_CONSUME_REGEX`               | `false` | Treat configured topics as regular expressions                            |
| `KAFKA_DISABLE_FETCH_SESSIONS`      | `false` | Disable Kafka fetch sessions                                              |
| `KAFKA_RACK`                        |    —    | Rack identifier for rack-aware fetching                                   |
| `KAFKA_MAX_CONCURRENT_FETCHES`      |    —    | Maximum concurrent fetches buffered by the client                         |
| `KAFKA_SESSION_TIMEOUT`             |    —    | Maximum time between heartbeats before rebalance                          |
| `KAFKA_REBALANCE_TIMEOUT`           |    —    | Maximum time for members to rejoin on rebalance                           |
| `KAFKA_HEARTBEAT_INTERVAL`          |    —    | Heartbeat interval                                                        |
| `KAFKA_FETCH_MAX_WAIT`              |    —    | Maximum broker wait time for incomplete fetches                           |
| `KAFKA_FETCH_MIN_BYTES`             |    —    | Minimum bytes a broker tries to accumulate before responding              |
| `KAFKA_FETCH_MAX_BYTES`             |    —    | Maximum bytes per fetch response                                          |
| `KAFKA_FETCH_MAX_PARTITION_BYTES`   |    —    | Maximum bytes per partition fetch                                         |
| **Share Groups**             |         |                                                                           |
| `KAFKA_SHARE_GROUP`                 |    —    | Share group identifier                                                    |
| `KAFKA_SHARE_MAX_RECORDS`           |    —    | Maximum records returned per share fetch                                  |
| `KAFKA_SHARE_MAX_RECORDS_STRICT`    | `false` | Strictly cap records per share fetch                                      |
| `KAFKA_SHARE_REJECT_AFTER_DELIVERIES` |    —    | Delivery limit before triggering `AckReject`                              |
| `KAFKA_SHARE_RELEASE_TIMEOUT`       |    —    | Backoff delay before releasing failed records                             |

## License

This project is licensed under the [MIT License](LICENSE).