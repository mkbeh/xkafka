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

This repository contains the core `xkafka` module. The core package is released from the repository root:

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
> `SUSPEND_PROCESSING_TIMEOUT`.

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

### Configuration

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
> For the full list of exported Prometheus metrics, see [internal/pkg/kprom/metrics.go](internal/kprom/metrics.go).

## Configuration

`Config` can be initialized directly as a Go struct or populated from environment variables by your application
configuration layer.

For a complete example of environment-based configuration, see [examples/env](examples/env).
### Environment variables

| Variable | Default | Description |
|---|---|---|
| BROKERS | | Comma-separated seed brokers |
| SASL_MECHANISM | | PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512 |
| USER | | SASL username |
| PASSWORD | | SASL password |
| REQUEST_TIMEOUT_OVERHEAD | | Request deadline overhead |
| REQUEST_RETRIES | | Max request retries |
| RETRY_TIMEOUT | | Total retry time limit |
| DIAL_TIMEOUT | | Broker dial timeout |
| CONN_IDLE_TIMEOUT | | Idle connection timeout |
| METADATA_MAX_AGE | | Max age of cached metadata |
| METADATA_MIN_AGE | | Min time between metadata refreshes |
| MAX_WRITE_BYTES | | Max bytes per connection write |
| MAX_READ_BYTES | | Max bytes per broker response |
| ALWAYS_RETRY_EOF | false | Retry EOF errors instead of failing connection |
| DEFAULT_PRODUCE_TOPIC | | Fallback topic if record topic is empty |
| PRODUCER_BATCH_MAX_BYTES | | Max size of a producer batch |
| MAX_BUFFERED_RECORDS | | Max buffered records before blocking |
| MAX_BUFFERED_BYTES | | Max buffered bytes before blocking |
| PRODUCE_REQUEST_TIMEOUT | | Broker response timeout for produce requests |
| RECORD_RETRIES | | Max record-level produce retries |
| RECORD_DELIVERY_TIMEOUT | | Max record buffering time |
| PRODUCER_LINGER | | Linger delay for batch building |
| TRANSACTIONAL_ID | | Transactional identifier for EOS |
| TRANSACTION_TIMEOUT | | Max transaction duration |
| ENABLED | true | Enable consumer loop |
| TOPICS | | Comma-separated topics to consume |
| GROUP | | Consumer group ID |
| MAX_POLL_RECORDS | 100 | Max records per poll |
| POLL_INTERVAL | 1s | Interval between polls |
| SKIP_FATAL_ERRORS | true | Continue on non-retryable fetch errors |
| SUSPEND_PROCESSING_TIMEOUT | 30s | Backoff delay after handler error |
| SUSPEND_COMMITTING_TIMEOUT | 10s | Backoff delay after commit/ack error |
| INSTANCE_ID | | Static group membership ID |
| CONSUME_REGEX | false | Treat topics as regular expressions |
| DISABLE_FETCH_SESSIONS | false | Disable fetch sessions |
| RACK | | Rack ID for rack-aware fetching |
| MAX_CONCURRENT_FETCHES | | Max concurrent fetches buffered by client |
| SESSION_TIMEOUT | | Rebalance session timeout |
| REBALANCE_TIMEOUT | | Max time for members to rejoin |
| HEARTBEAT_INTERVAL | | Heartbeat interval |
| FETCH_MAX_WAIT | | Max broker wait time for fetches |
| FETCH_MIN_BYTES | | Min bytes broker accumulates before response |
| FETCH_MAX_BYTES | | Max bytes per fetch response |
| FETCH_MAX_PARTITION_BYTES | | Max bytes per partition fetch |
| SHARE_GROUP | | Share group ID |
| SHARE_MAX_RECORDS | | Max records per share fetch |
| SHARE_MAX_RECORDS_STRICT | false | Strictly cap records per share fetch |
| SHARE_REJECT_AFTER_DELIVERIES | | Delivery limit before triggering AckReject |
| SHARE_RELEASE_TIMEOUT | | Backoff delay before releasing failed records |

## License

This project is licensed under the [MIT License](LICENSE).