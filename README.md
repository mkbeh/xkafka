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

These example show basic producing and consuming flows. For production workloads, tune retries, offset commits,
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
> `RunInTx` automatically commits on `nil` and aborts on errors or panics. To prevent consumers from reading aborted
> records, enable `kgo.ReadCommitted()` using `WithFetchIsolationLevel`.

## Share Groups

Use `WithShareGroupBatchHandler` with `ShareGroup` to consume through Kafka Share Groups.

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
	xkafka.WithShareGroupBatchHandler(func(ctx context.Context, records []*kgo.Record) error {
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
> **Share Group Ack Rules:** Successful handlers trigger `AckAccept`, while errors trigger `AckRelease`. If the delivery
> limit is reached, records are marked as `AckReject` (if `ShareRejectAfterDeliveries > 0`), otherwise they continue to
> be
> released via `AckRelease`.

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
		func(ctx context.Context, records []*kgo.Record, tx *xkafka.Tx) error {
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

| Variable | Default | Description |
|---|---|---|
| KAFKA_BROKERS | | Comma-separated seed brokers |
| KAFKA_SASL_MECHANISM | | PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512 |
| KAFKA_USER | | SASL username |
| KAFKA_PASSWORD | | SASL password |
| KAFKA_REQUEST_TIMEOUT_OVERHEAD | | Request deadline overhead |
| KAFKA_REQUEST_RETRIES | | Max request retries |
| KAFKA_RETRY_TIMEOUT | | Total retry time limit |
| KAFKA_DIAL_TIMEOUT | | Broker dial timeout |
| KAFKA_CONN_IDLE_TIMEOUT | | Idle connection timeout |
| KAFKA_METADATA_MAX_AGE | | Max age of cached metadata |
| KAFKA_METADATA_MIN_AGE | | Min time between metadata refreshes |
| KAFKA_MAX_WRITE_BYTES | | Max bytes per connection write |
| KAFKA_MAX_READ_BYTES | | Max bytes per broker response |
| KAFKA_ALWAYS_RETRY_EOF | false | Retry EOF errors instead of failing connection |
| KAFKA_DEFAULT_PRODUCE_TOPIC | | Fallback topic if record topic is empty |
| KAFKA_PRODUCER_BATCH_MAX_BYTES | | Max size of a producer batch |
| KAFKA_MAX_BUFFERED_RECORDS | | Max buffered records before blocking |
| KAFKA_MAX_BUFFERED_BYTES | | Max buffered bytes before blocking |
| KAFKA_PRODUCE_REQUEST_TIMEOUT | | Broker response timeout for produce requests |
| KAFKA_RECORD_RETRIES | | Max record-level produce retries |
| KAFKA_RECORD_DELIVERY_TIMEOUT | | Max record buffering time |
| KAFKA_PRODUCER_LINGER | | Linger delay for batch building |
| KAFKA_TRANSACTIONAL_ID | | Transactional identifier for EOS |
| KAFKA_TRANSACTION_TIMEOUT | | Max transaction duration |
| KAFKA_ENABLED | true | Enable consumer loop |
| KAFKA_TOPICS | | Comma-separated topics to consume |
| KAFKA_GROUP | | Consumer group ID |
| KAFKA_MAX_POLL_RECORDS | 100 | Max records per poll |
| KAFKA_POLL_INTERVAL | 1s | Interval between polls |
| KAFKA_SKIP_FATAL_ERRORS | true | Continue on non-retryable fetch errors |
| KAFKA_SUSPEND_PROCESSING_TIMEOUT | 30s | Backoff delay after handler error |
| KAFKA_SUSPEND_COMMITTING_TIMEOUT | 10s | Backoff delay after commit/ack error |
| KAFKA_INSTANCE_ID | | Static group membership ID |
| KAFKA_CONSUME_REGEX | false | Treat topics as regular expressions |
| KAFKA_DISABLE_FETCH_SESSIONS | false | Disable fetch sessions |
| KAFKA_RACK | | Rack ID for rack-aware fetching |
| KAFKA_MAX_CONCURRENT_FETCHES | | Max concurrent fetches buffered by client |
| KAFKA_SESSION_TIMEOUT | | Rebalance session timeout |
| KAFKA_REBALANCE_TIMEOUT | | Max time for members to rejoin |
| KAFKA_HEARTBEAT_INTERVAL | | Heartbeat interval |
| KAFKA_FETCH_MAX_WAIT | | Max broker wait time for fetches |
| KAFKA_FETCH_MIN_BYTES | | Min bytes broker accumulates before response |
| KAFKA_FETCH_MAX_BYTES | | Max bytes per fetch response |
| KAFKA_FETCH_MAX_PARTITION_BYTES | | Max bytes per partition fetch |
| KAFKA_SHARE_GROUP | | Share group ID |
| KAFKA_SHARE_MAX_RECORDS | | Max records per share fetch |
| KAFKA_SHARE_MAX_RECORDS_STRICT | false | Strictly cap records per share fetch |
| KAFKA_SHARE_REJECT_AFTER_DELIVERIES | | Delivery limit before triggering AckReject |
| KAFKA_SHARE_RELEASE_TIMEOUT | | Backoff delay before releasing failed records |

## License

This project is licensed under the [MIT License](LICENSE).