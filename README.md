# Kafka Library

This library provides an API for working with Kafka, using franz-go
and integration with OpenTelemetry for tracing and metrics.
The library implements messages producing and consuming.

## Features

- Support for asynchronous and synchronous message sending
- Transactions
- Customizable message handlers for consumers
- Observability

## Getting started

Here's a basic overview of producing and consuming:

```go
package main

import (
	"context"
	"fmt"
	"github.com/mkbeh/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	// One client can only produce!
	producer, err := kafka.NewProducer()
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	ctx := context.Background()

	// Producing a message
	record := &kgo.Record{Topic: "foo", Value: []byte("bar")}
	producer.ProduceAsync(ctx, record)

	// Alternatively, ProduceSync exists to synchronously produce a batch of records.
	err = producer.ProduceSync(ctx, record)
	if err != nil {
		panic(err)
	}

	// Consuming messages from a topic
	consumer, err := kafka.NewConsumer(
		kafka.WithConsumerConfig(&kafka.ConsumerConfig{Topics: "foo"}),
		kafka.WithConsumerHandler(func(ctx context.Context, msg *kgo.Record) error {
			fmt.Printf("received msg: %+v\n", msg)
			return nil
		}),
	)
	if err != nil {
		panic(err)
	}

	err = consumer.PreRun(ctx)
	if err != nil {
		panic(err)
	}

	err = consumer.Run(ctx)
	if err != nil {
		panic(err)
	}

	consumer.Shutdown(ctx)
}

```

## Options

Available producer and consumer configuration.

### Producer configuration

Producer env variables:

| ENV                            | DEFAULT | DESCRIPTION                                                                                                                                                                                                   |
|--------------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| KAFKA_BROKERS                  | -       | **Brokers** sets the seed brokers for the client to use, overriding the, default 127.0.0.1:9092                                                                                                               |
| KAFKA_SASL_MECHANISM           | -       | **SaslMechanism** SASL mechanism to use for authentication. Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. NOTE: Despite the name only one mechanism must be configured.                                     |
| KAFKA_USER                     | -       | **User** sasl username for use with the PLAIN and SASL-SCRAM-.. mechanisms.                                                                                                                                   |
| KAFKA_PASSWORD                 | -       | **Password** sasl password for use with the PLAIN and SASL-SCRAM-.. mechanism.                                                                                                                                |
| KAFKA_REQUEST_TIMEOUT_OVERHEAD | -       | **RequestTimeoutOverhead** uses the given time as overhead while deadlining requests.                                                                                                                         |
| KAFKA_CONN_IDLE_TIMEOUT        | -       | **ConnIdleTimeout** is a rough amount of time to allow connections to idle before they are closed.                                                                                                            |
| KAFKA_DIAL_TIMEOUT             | -       | **DialTimeout** sets the dial timeout.                                                                                                                                                                        |
| KAFKA_REQUEST_RETRIES          | -       | **RequestRetries** sets the number of tries that retryable requests are allowed.                                                                                                                              |
| KAFKA_RETRY_TIMEOUT            | -       | **RetryTimeout** sets the upper limit on how long we allow a request to be issued and then reissued on failure. That is, this control the total end-to-end maximum time we allow for trying a request.        |
| KAFKA_MAX_WRITE_BYTES          | -       | **BrokerMaxWriteBytes** upper bounds the number of bytes written to a broker connection in a single write.                                                                                                    |
| KAFKA_MAX_READ_BYTES           | -       | **BrokerMaxReadBytes** sets the maximum response size that can be read from Kafka.                                                                                                                            |
| KAFKA_METADATA_MAX_AGE         | -       | **MetadataMaxAge** sets the maximum age for the client's cached metadata.                                                                                                                                     |
| KAFKA_METADATA_MIN_AGE         | -       | **MetadataMinAge** sets the minimum time between metadata queries.                                                                                                                                            |
| KAFKA_DEFAULT_PRODUCE_TOPIC    | -       | **DefaultProduceTopic** sets the default topic to produce to if the topic field is empty in a Record.                                                                                                         |
| KAFKA_PRODUCER_BATCH_MAX_BYTES | -       | **ProducerBatchMaxBytes** upper bounds the size of a record batch.                                                                                                                                            |
| KAFKA_MAX_BUFFERED_RECORDS     | -       | **MaxBufferedRecords** sets the max amount of records the client will buffer, blocking produces until records are finished if this limit is reached.                                                          |
| KAFKA_MAX_BUFFERED_BYTES       | -       | **MaxBufferedBytes** sets the max amount of bytes that the client will buffer while producing, blocking produces until records are finished if this limit is reached.                                         |
| KAFKA_PRODUCE_REQUEST_TIMEOUT  | -       | **ProduceRequestTimeout** sets how long Kafka broker's are allowed to respond to produce requests, overriding the default 10s. If a broker exceeds this duration, it will reply with a request timeout error. |
| KAFKA_RECORD_RETRIES           | -       | **RecordRetries** sets the number of tries for producing records.                                                                                                                                             |
| KAFKA_PRODUCER_LINGER          | -       | **ProducerLinger** sets how long individual topic partitions will linger waiting for more records before triggering a request to be built.                                                                    |
| KAFKA_RECORD_DELIVERY_TIMEOUT  | -       | **RecordDeliveryTimeout** sets a rough time of how long a record can sit around in a batch before timing out.                                                                                                 |
| KAFKA_TRANSACTIONAL_ID         | -       | **TransactionalID** sets a transactional ID for the client, ensuring that records are produced transactionally under this ID (exactly once semantics).                                                        |
| KAFKA_TRANSACTION_TIMEOUT      | -       | **TransactionTimeout** sets the allowed for a transaction.                                                                                                                                                    |

### Consumer configuration

Consumer env variables:

| ENV                              | DEFAULT | DESCRIPTION                                                                                                                                                                                                                                   |
|----------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| KAFKA_BROKERS                    | -       | **Brokers** sets the seed brokers for the client to use, overriding the, default 127.0.0.1:9092                                                                                                                                               |
| KAFKA_SASL_MECHANISM             | -       | **SaslMechanism** SASL mechanism to use for authentication. Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. NOTE: Despite the name only one mechanism must be configured.                                                                     |
| KAFKA_USER                       | -       | **User** sasl username for use with the PLAIN and SASL-SCRAM-.. mechanisms.                                                                                                                                                                   |
| KAFKA_PASSWORD                   | -       | **Password** sasl password for use with the PLAIN and SASL-SCRAM-.. mechanism.                                                                                                                                                                |
| KAFKA_ENABLED                    | false   | **Enabled** custom option: detecting enabling.                                                                                                                                                                                                |
| KAFKA_SKIP_FATAL_ERRORS          | true    | **SkipFatalErrors** custom option: skip fatal errors while fetching records, otherwise leave the process.                                                                                                                                     |
| KAFKA_CONSUME_REGEX              | false   | **ConsumeRegex** sets the client to parse all topics passed to Topics as regular expressions.                                                                                                                                                 |
| KAFKA_TOPICS                     | -       | **Topics** adds topics to use for consuming.                                                                                                                                                                                                  |
| KAFKA_GROUP                      | -       | **Group** sets the consumer group for the client to join and consume in. This option is required if using any other group options.                                                                                                            |
| KAFKA_MAX_POLL_RECORDS           | 100     | **MaxPollRecords** maximum of maxPollRecords total across all fetches.                                                                                                                                                                        |
| KAFKA_INSTANCE_ID                | -       | **InstanceID** sets the group consumer's instance ID, switching the group member from "dynamic" to "static".                                                                                                                                  |
| KAFKA_POLL_INTERVAL              | 300ms   | **PollInterval** interval between handle batches.                                                                                                                                                                                             |
| KAFKA_SUSPEND_PROCESSING_TIMEOUT | 30s     | **SuspendProcessingTimeout** waiting timeout after batch processing failed (custom property).                                                                                                                                                 |
| KAFKA_SUSPEND_COMMITTING_TIMEOUT | 10s     | **SuspendCommitingTimeout** waiting timeout after committing failed (custom property).                                                                                                                                                        |
| KAFKA_FETCH_MAX_WAIT             | -       | **FetchMaxWait** sets the maximum amount of time a broker will wait for a fetch response to hit the minimum number of required bytes before returning.                                                                                        |
| KAFKA_FETCH_MAX_BYTES            | -       | **FetchMaxBytes** sets the maximum amount of bytes a broker will try to send during a fetch.                                                                                                                                                  |
| KAFKA_FETCH_MIN_BYTES            | -       | **FetchMinBytes** sets the minimum amount of bytes a broker will try to send during a fetch.                                                                                                                                                  |
| KAFKA_FETCH_MAX_PARTITION_BYTES  | -       | **FetchMaxPartitionBytes** sets the maximum amount of bytes that will be consumed for a single partition in a fetch request.                                                                                                                  |
| KAFKA_DISABLE_FETCH_SESSIONS     | -       | **DisableFetchSessions** sets the client to not use fetch sessions (Kafka 1.0+).                                                                                                                                                              |
| KAFKA_SESSION_TIMEOUT            | -       | **SessionTimeout** sets how long a member in the group can go between heartbeats, overriding the default 45,000ms. If a member does not heartbeat in this timeout, the broker will remove the member from the group and initiate a rebalance. |
| KAFKA_REBALANCE_TIMEOUT          | -       | **RebalanceTimeout** sets how long group members are allowed to take when a rebalance has begun.                                                                                                                                              |
| KAFKA_HEARTBEAT_INTERVAL         | -       | **HeartbeatInterval** sets how long a group member goes between heartbeats to Kafka.                                                                                                                                                          |
| KAFKA_REQUIRE_STABLE_FETCH_OFFS  | -       | **RequireStableFetchOffsets** sets the group consumer to require "stable" fetch offsets before consuming from the group.                                                                                                                      |
| KAFKA_REQUEST_TIMEOUT_OVERHEAD   | -       | **RequestTimeoutOverhead** uses the given time as overhead while deadlining requests.                                                                                                                                                         |
| KAFKA_CONN_IDLE_TIMEOUT          | -       | **ConnIdleTimeout** is a rough amount of time to allow connections to idle before they are closed.                                                                                                                                            |
| KAFKA_DIAL_TIMEOUT               | -       | **DialTimeout** sets the dial timeout.                                                                                                                                                                                                        |
| KAFKA_REQUEST_RETRIES            | -       | **RequestRetries** sets the number of tries that retryable requests are allowed.                                                                                                                                                              |
| KAFKA_RETRY_TIMEOUT              | -       | **RetryTimeout** sets the upper limit on how long we allow a request to be issued and then reissued on failure. That is, this control the total end-to-end maximum time we allow for trying a request.                                        |
| KAFKA_MAX_WRITE_BYTES            | -       | **BrokerMaxWriteBytes** upper bounds the number of bytes written to a broker connection in a single write.                                                                                                                                    |
| KAFKA_MAX_READ_BYTES             | -       | **BrokerMaxReadBytes** sets the maximum response size that can be read from Kafka.                                                                                                                                                            |
| KAFKA_METADATA_MAX_AGE           | -       | **MetadataMaxAge** sets the maximum age for the client's cached metadata.                                                                                                                                                                     |
| KAFKA_METADATA_MIN_AGE           | -       | **MetadataMinAge** sets the minimum time between metadata queries.                                                                                                                                                                            |