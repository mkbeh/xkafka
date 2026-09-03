package xkafka

import (
	"maps"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Opt configures xkafka client behavior.
type Opt interface {
	apply(*client)
}

type clientOpt struct {
	fn func(*client)
}

func (opt clientOpt) apply(c *client) {
	opt.fn(c)
}

// WithKafkaOptions appends native franz-go client options.
func WithKafkaOptions(opts ...kgo.Opt) Opt {
	opts = append([]kgo.Opt(nil), opts...)

	return clientOpt{fn: func(c *client) {
		c.kafkaOpts = append(c.kafkaOpts, opts...)
	}}
}

// WithName sets a stable client name used for observability and as the Kafka client ID.
func WithName(name string) Opt {
	name = strings.TrimSpace(name)

	return clientOpt{fn: func(c *client) {
		c.name = name
	}}
}

// WithLabel adds or replaces one observability label.
func WithLabel(key, value string) Opt {
	return clientOpt{fn: func(c *client) {
		if key != "" {
			c.labels[key] = value
		}
	}}
}

// WithLabels merges observability labels into the client metadata.
//
// Labels are defensively copied. When the same key is configured more than
// once, the last value wins.
func WithLabels(labels map[string]string) Opt {
	labels = maps.Clone(labels)

	return clientOpt{fn: func(c *client) {
		for key, value := range labels {
			if key != "" {
				c.labels[key] = value
			}
		}
	}}
}

// WithMetrics attaches one metrics implementation to the client or group transaction session.
//
// Metrics are registered during creation and unregistered automatically during Shutdown.
func WithMetrics(metrics Metrics) Opt {
	return clientOpt{fn: func(c *client) {
		c.metrics = metrics
	}}
}

// WithLogger sets the logger used by xkafka and franz-go.
//
// Logging is disabled by default.
func WithLogger(logger kgo.Logger) Opt {
	return clientOpt{fn: func(c *client) {
		c.logger = logger
	}}
}

// WithProducePromise sets the default callback for asynchronous produce operations.
func WithProducePromise(promise PromiseFunc) Opt {
	return clientOpt{fn: func(c *client) {
		c.promiseFunc = promise
	}}
}

// WithBatchHandler sets the batch handler for regular consumers and Share Groups.
//
// When kgo.ShareGroup is configured, Share Group acknowledgement semantics are used.
func WithBatchHandler(handler BatchHandlerFunc) Opt {
	return clientOpt{fn: func(c *client) {
		c.batchHandler = handler
	}}
}

// WithGroupTransactSessionBatchHandler sets the group transaction session batch handler.
func WithGroupTransactSessionBatchHandler(handler BatchTxHandlerFunc) Opt {
	return clientOpt{fn: func(c *client) {
		c.sessionHandler = handler
	}}
}

// WithMaxPollRecords sets the maximum number of records handled in one poll iteration.
//
// Values less than or equal to zero return all currently buffered records.
func WithMaxPollRecords(maxPollRecords int) Opt {
	return clientOpt{fn: func(c *client) {
		c.maxPollRecords = maxPollRecords
	}}
}

// WithPollInterval sets the interval between consumer poll iterations.
func WithPollInterval(interval time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if interval > 0 {
			c.pollInterval = interval
		}
	}}
}

// WithSuspendProcessingTimeout sets the wait time after handler errors.
func WithSuspendProcessingTimeout(timeout time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if timeout >= 0 {
			c.suspendProcessingTimeout = timeout
		}
	}}
}

// WithHandlerMaxRetries sets the maximum number of retries after regular consumer handler errors.
//
// A value of zero keeps the default unlimited retry behavior.
func WithHandlerMaxRetries(retries int) Opt {
	return clientOpt{fn: func(c *client) {
		if retries >= 0 {
			c.maxHandlerRetries = retries
		}
	}}
}

// WithSuspendCommittingTimeout sets the wait time after offset commit or ack errors.
func WithSuspendCommittingTimeout(timeout time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if timeout >= 0 {
			c.suspendCommittingTimeout = timeout
		}
	}}
}

// WithShareRejectAfterDeliveries rejects failed Share Group records after the given delivery count.
//
// By default, failed records are released for redelivery.
func WithShareRejectAfterDeliveries(deliveries int32) Opt {
	return clientOpt{fn: func(c *client) {
		if deliveries >= 0 {
			c.shareRejectAfterDeliveries = deliveries
		}
	}}
}

// WithShareReleaseTimeout delays AckRelease after Share Group handler errors.
func WithShareReleaseTimeout(timeout time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if timeout >= 0 {
			c.shareReleaseTimeout = timeout
		}
	}}
}
