package xkafka

import (
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Opt configures Client and GroupTransactSession behavior.
type Opt interface {
	apply(*client)
}

type clientOpt struct {
	fn func(*client)
}

func (opt clientOpt) apply(c *client) {
	opt.fn(c)
}

// WithKafkaOptions adds native franz-go options to the underlying Kafka client.
func WithKafkaOptions(opts ...kgo.Opt) Opt {
	opts = append([]kgo.Opt(nil), opts...)

	return clientOpt{fn: func(c *client) {
		c.kafkaOpts = append(c.kafkaOpts, opts...)
	}}
}

// WithName sets the logical client name used for observability and as the
// Kafka client ID.
//
// Leading and trailing whitespace is removed. A non-empty name overrides a
// client ID configured through [WithKafkaOptions].
func WithName(name string) Opt {
	name = strings.TrimSpace(name)

	return clientOpt{fn: func(c *client) {
		c.name = name
	}}
}

// WithHooks registers hooks for xkafka runtime events.
//
// See [Hook] for hook behavior and concurrency requirements.
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

// WithProducePromise sets the default callback for asynchronous produce
// operations.
//
// The callback is used when an asynchronous produce call is made with a nil
// promise.
func WithProducePromise(promise PromiseFunc) Opt {
	return clientOpt{fn: func(c *client) {
		c.promiseFunc = promise
	}}
}

// WithBatchHandler sets the batch handler used by [Client.HandleFetches].
//
// For regular consumers, handler errors are retried according to
// [WithMaxRetries] and [WithSuspendProcessingTimeout]. Share Group handler
// errors use release or rejection semantics.
func WithBatchHandler(handler BatchHandlerFunc) Opt {
	return clientOpt{fn: func(c *client) {
		c.batchHandler = handler
	}}
}

// WithGroupTransactSessionBatchHandler sets the batch handler used by
// [GroupTransactSession.HandleFetches].
//
// A handler is required when creating a [GroupTransactSession].
func WithGroupTransactSessionBatchHandler(handler BatchTxHandlerFunc) Opt {
	return clientOpt{fn: func(c *client) {
		c.sessionHandler = handler
	}}
}

// WithMaxPollRecords sets the maximum number of buffered records processed in
// one poll iteration.
//
// The default is 100. Values less than or equal to zero process all currently
// buffered records.
func WithMaxPollRecords(maxPollRecords int) Opt {
	return clientOpt{fn: func(c *client) {
		c.maxPollRecords = maxPollRecords
	}}
}

// WithMaxRetries sets the maximum number of retries after the initial regular
// batch handler attempt.
//
// The default is unlimited retries. A value of zero disables retries. Negative
// values are ignored.
//
// This option does not apply to Share Groups or GroupTransactSession handlers.
func WithMaxRetries(maxRetries int) Opt {
	return clientOpt{fn: func(c *client) {
		if maxRetries >= 0 {
			c.maxHandlerRetries = maxRetries
		}
	}}
}

// WithPollInterval sets the interval between consumer poll iterations.
//
// The default is 1 second. Non-positive values are ignored.
func WithPollInterval(interval time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if interval > 0 {
			c.pollInterval = interval
		}
	}}
}

// WithSuspendProcessingTimeout sets the delay after a handler failure.
//
// The delay applies between regular consumer retries and after
// GroupTransactSession handler errors. The default is 30 seconds.
//
// A value of zero disables the delay. Negative values are ignored.
func WithSuspendProcessingTimeout(timeout time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if timeout >= 0 {
			c.suspendProcessingTimeout = timeout
		}
	}}
}

// WithSuspendCommittingTimeout sets the delay between manual offset commit
// retries.
//
// The default is 10 seconds. A value of zero retries immediately. Negative
// values are ignored.
func WithSuspendCommittingTimeout(timeout time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if timeout >= 0 {
			c.suspendCommittingTimeout = timeout
		}
	}}
}

// WithShareRejectAfterDeliveries rejects a failed Share Group record when its
// delivery count reaches deliveries.
//
// The default is zero, which disables rejection and releases failed records for
// redelivery. Negative values are ignored.
func WithShareRejectAfterDeliveries(deliveries int32) Opt {
	return clientOpt{fn: func(c *client) {
		if deliveries >= 0 {
			c.shareRejectAfterDeliveries = deliveries
		}
	}}
}

// WithShareReleaseTimeout sets the delay before flushing Share Group
// acknowledgements when records are released for redelivery.
//
// The default is zero. Negative values are ignored.
func WithShareReleaseTimeout(timeout time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if timeout >= 0 {
			c.shareReleaseTimeout = timeout
		}
	}}
}
