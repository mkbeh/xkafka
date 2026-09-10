package xkafka

import (
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

// WithHooks adds hooks for xkafka runtime events.
//
// A hook may implement any number of the hook interfaces defined by this
// package. Hooks are called in registration order.
func WithHooks(hooks ...Hook) Opt {
	hooks = append([]Hook(nil), hooks...)

	return clientOpt{fn: func(c *client) {
		c.hooks = append(c.hooks, hooks...)
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

// WithMaxRetries sets the maximum number of retries after a regular batch handler error.
//
// A value of zero disables retries. By default, retries are unlimited.
// This option does not apply to Share Groups or GroupTransactSession handlers.
func WithMaxRetries(maxRetries int) Opt {
	return clientOpt{fn: func(c *client) {
		if maxRetries >= 0 {
			c.maxHandlerRetries = maxRetries
		}
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
