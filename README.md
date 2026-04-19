# xkafka

![Go Version](https://img.shields.io/badge/go-1.21+-blue)
![License](https://img.shields.io/badge/license-MIT-green)

A Kafka client library built on top of [franz-go](https://github.com/twmb/franz-go) with built-in OpenTelemetry tracing, Prometheus metrics, and a clean API for producing and consuming messages.

## Features

- **Producer**: async, sync, and transactional message sending
- **Consumer**: per-record and batch message handlers with automatic offset management
- **Observability**: OpenTelemetry tracing + Prometheus metrics out of the box
- **SASL**: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
- **TLS** support
- Configuration via struct or environment variables

## Installation

```bash
go get github.com/mkbeh/xkafka
```

## Quick Start

### Producer

```go
package main

import (
    "context"
    "log"

    "github.com/mkbeh/xkafka"
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
    record := &kgo.Record{Topic: "my-topic", Value: []byte("hello")}

    // Async (fire-and-forget with logging on error)
    producer.ProduceAsync(ctx, record)

    // Sync (blocks until broker confirms)
    if err := producer.ProduceSync(ctx, record); err != nil {
        log.Fatal(err)
    }
}
```

### Consumer

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/mkbeh/xkafka"
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

> **Note:** If the handler returns an error, the consumer will retry the record after `KAFKA_SUSPEND_PROCESSING_TIMEOUT` (default 30s). Offsets are committed only after successful processing.

## Serialization

The library provides ready-to-use marshal/unmarshal helpers:

```go
// JSON
data, err := kafka.JSONMarshal(myStruct)
err = kafka.JSONUnmarshal(data, &myStruct)

// Protobuf
data, err := kafka.ProtoMarshal(myProtoMessage)
err = kafka.ProtoUnmarshal(data, &myProtoMessage)
```

## Observability

Metrics and tracing are enabled automatically using the global OpenTelemetry providers. To use custom providers:

```go
kafka.NewProducer(
    kafka.WithProducerTracerProvider(myTracerProvider),
    kafka.WithProducerMeterProvider(myMeterProvider),
    kafka.WithProducerMetricsNamespace("myapp"),
)
```

The following labels are attached to all metrics automatically: `client_id`, `client_kind` (`producer` / `consumer`), and `consumer_group` (for consumers).

## Configuration

Both `ProducerConfig` and `ConsumerConfig` can be populated from environment variables using [envconfig](https://github.com/kelseyhightower/envconfig) or set directly as struct fields.

### Producer

| ENV | DEFAULT | DESCRIPTION |
|-----|---------|-------------|
| `KAFKA_BROKERS` | — | Seed brokers (comma-separated), e.g. `host1:9092,host2:9092` |
| `KAFKA_SASL_MECHANISM` | — | `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512` |
| `KAFKA_USER` | — | SASL username |
| `KAFKA_PASSWORD` | — | SASL password |
| `KAFKA_DEFAULT_PRODUCE_TOPIC` | — | Default topic if record has no topic set |
| `KAFKA_PRODUCER_BATCH_MAX_BYTES` | — | Max size of a record batch |
| `KAFKA_MAX_BUFFERED_RECORDS` | — | Max records buffered before blocking |
| `KAFKA_MAX_BUFFERED_BYTES` | — | Max bytes buffered before blocking |
| `KAFKA_PRODUCE_REQUEST_TIMEOUT` | 10s | Max time broker has to respond to a produce request |
| `KAFKA_RECORD_RETRIES` | — | Number of retries for producing a record |
| `KAFKA_PRODUCER_LINGER` | — | Time to wait for more records before sending a batch |
| `KAFKA_RECORD_DELIVERY_TIMEOUT` | — | Max time a record can sit in a batch before timeout |
| `KAFKA_TRANSACTIONAL_ID` | — | Enables exactly-once semantics via transactions |
| `KAFKA_TRANSACTION_TIMEOUT` | — | Max duration allowed for a transaction |
| `KAFKA_REQUEST_RETRIES` | — | Number of retries for retryable requests |
| `KAFKA_RETRY_TIMEOUT` | — | Total time limit for request retries |
| `KAFKA_REQUEST_TIMEOUT_OVERHEAD` | — | Extra time added to request deadlines |
| `KAFKA_CONN_IDLE_TIMEOUT` | — | Idle connection timeout |
| `KAFKA_DIAL_TIMEOUT` | — | Dial timeout |
| `KAFKA_MAX_WRITE_BYTES` | — | Max bytes per broker write |
| `KAFKA_MAX_READ_BYTES` | — | Max bytes per broker response |
| `KAFKA_METADATA_MAX_AGE` | — | Max age of cached metadata |
| `KAFKA_METADATA_MIN_AGE` | — | Min time between metadata refreshes |

### Consumer

| ENV | DEFAULT | DESCRIPTION |
|-----|---------|-------------|
| `KAFKA_BROKERS` | — | Seed brokers (comma-separated) |
| `KAFKA_SASL_MECHANISM` | — | `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512` |
| `KAFKA_USER` | — | SASL username |
| `KAFKA_PASSWORD` | — | SASL password |
| `KAFKA_ENABLED` | `false` | Enable the consumer; `PreRun`/`Run` are no-ops if `false` |
| `KAFKA_TOPICS` | — | Topics to consume (comma-separated) |
| `KAFKA_GROUP` | — | Consumer group ID |
| `KAFKA_MAX_POLL_RECORDS` | `100` | Max records fetched per poll |
| `KAFKA_POLL_INTERVAL` | `300ms` | Interval between polls |
| `KAFKA_SKIP_FATAL_ERRORS` | `true` | Continue on non-retriable fetch errors |
| `KAFKA_SUSPEND_PROCESSING_TIMEOUT` | `30s` | Backoff after a handler error before retrying |
| `KAFKA_SUSPEND_COMMITTING_TIMEOUT` | `10s` | Backoff after a failed offset commit |
| `KAFKA_INSTANCE_ID` | — | Static member ID (disables dynamic rebalancing) |
| `KAFKA_CONSUME_REGEX` | `false` | Treat topic names as regular expressions |
| `KAFKA_DISABLE_FETCH_SESSIONS` | `false` | Disable Kafka fetch sessions (Kafka 1.0+) |
| `KAFKA_SESSION_TIMEOUT` | 45s | Max time between heartbeats before rebalance |
| `KAFKA_REBALANCE_TIMEOUT` | — | Max time for group members to rejoin after rebalance |
| `KAFKA_HEARTBEAT_INTERVAL` | — | Interval between heartbeats |
| `KAFKA_REQUIRE_STABLE_FETCH_OFFS` | `false` | Require stable offsets before consuming |
| `KAFKA_FETCH_MAX_WAIT` | — | Max time broker waits before returning a fetch response |
| `KAFKA_FETCH_MAX_BYTES` | — | Max bytes per fetch response |
| `KAFKA_FETCH_MIN_BYTES` | — | Min bytes broker waits to accumulate before responding |
| `KAFKA_FETCH_MAX_PARTITION_BYTES` | — | Max bytes per partition in a fetch |
| `KAFKA_REQUEST_RETRIES` | — | Number of retries for retryable requests |
| `KAFKA_RETRY_TIMEOUT` | — | Total time limit for request retries |
| `KAFKA_REQUEST_TIMEOUT_OVERHEAD` | — | Extra time added to request deadlines |
| `KAFKA_CONN_IDLE_TIMEOUT` | — | Idle connection timeout |
| `KAFKA_DIAL_TIMEOUT` | — | Dial timeout |
| `KAFKA_MAX_WRITE_BYTES` | — | Max bytes per broker write |
| `KAFKA_MAX_READ_BYTES` | — | Max bytes per broker response |
| `KAFKA_METADATA_MAX_AGE` | — | Max age of cached metadata |
| `KAFKA_METADATA_MIN_AGE` | — | Min time between metadata refreshes |

## License

[MIT](LICENSE)