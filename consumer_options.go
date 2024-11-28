package kafka

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type ConsumerOption interface {
	apply(c *Consumer)
}

type consumerOptionFunc func(c *Consumer)

func (f consumerOptionFunc) apply(c *Consumer) {
	f(c)
}

func WithConsumerConfig(cfg *ConsumerConfig) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if cfg == nil {
			return
		}

		opts := []ConsumerOption{
			withEnabledFlag(cfg.Enabled),
			withConsumerSkipFatalErrorsFlag(cfg.SkipFatalErrors),
			withConsumerBrokers(strings.Split(cfg.Brokers, ",")...),
			withConsumerSASL(cfg),
			withMaxPollRecords(cfg.MaxPollRecords),
			withPollInterval(cfg.PollInterval),
			withSuspendProcessingTimeout(cfg.SuspendProcessingTimeout),
			withSuspendCommitingTimeout(cfg.SuspendCommitingTimeout),
			withConsumeRegex(cfg.ConsumeRegex),
			withConsumeTopics(strings.Split(cfg.Topics, ",")...),
			withConsumerGroup(cfg.Group),
			withConsumerInstanceID(cfg.InstanceID),
			withConsumerDisableFetchSessions(cfg.DisableFetchSessions),
			withSessionTimeout(cfg.SessionTimeout),
			withRebalanceTimeout(cfg.RebalanceTimeout),
			withHeartbeatInterval(cfg.HeartbeatInterval),
			withRequireStableFetchOffsets(cfg.RequireStableFetchOffsets),
			withConsumerRequestTimeoutOverhead(cfg.RequestTimeoutOverhead),
			withConsumerConnIdleTimeout(cfg.ConnIdleTimeout),
			withConsumerDialTimeout(cfg.DialTimeout),
			withConsumerRequestRetries(cfg.RequestRetries),
			withConsumerRetryTimeout(cfg.RetryTimeout),
			withConsumerBrokerMaxWriteBytes(cfg.BrokerMaxWriteBytes),
			withConsumerBrokerMaxReadBytes(cfg.BrokerMaxReadBytes),
			withConsumerMetadataMaxAge(cfg.MetadataMaxAge),
			withConsumerMetadataMinAge(cfg.MetadataMinAge),
		}

		for _, opt := range opts {
			opt.apply(c)
		}
	})
}

func WithConsumerClientID(v string) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v != "" {
			c.addClientOption(kgo.ClientID(v))
			c.addTracerOption(kotel.ClientID(v))
			c.id = fmt.Sprintf("%s-%s", v, GenerateUUID())
		}
	})
}

func WithConsumerLogger(v *slog.Logger) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		c.logger = v
	})
}

// WithConsumerTLS opts into dialing brokers with the given TLS config with a
// 10s dial timeout.
//
// Every dial, the input config is cloned. If the config's ServerName is not
// specified, this function uses net.SplitHostPort to extract the host from the
// broker being dialed and sets the ServerName. In short, it is not necessary
// to set the ServerName.
func WithConsumerTLS(tls *tls.Config) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		c.addClientOption(kgo.DialTLSConfig(tls))
	})
}

// WithConsumerGroupBalancers sets the group balancers to use for dividing topic partitions
// among group members, overriding the current default [cooperative-sticky].
// This option is equivalent to Kafka's partition.assignment.strategies option.
//
// For balancing, Kafka chooses the first protocol that all group members agree
// to support.
//
// Note that if you opt into cooperative-sticky rebalancing, cooperative group
// balancing is incompatible with eager (classical) rebalancing and requires a
// careful rollout strategy (see KIP-429).
func WithConsumerGroupBalancers(balancers ...kgo.GroupBalancer) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		c.addClientOption(kgo.Balancers(balancers...))
	})
}

// WithConsumerBatchHandler set batch message handler
func WithConsumerBatchHandler(handlerFunc BatchHandlerFunc) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if handlerFunc != nil {
			c.handleFetches = c.handleFetchesBatch(handlerFunc)
		}
	})
}

// WithConsumerHandler set message handler
func WithConsumerHandler(handlerFunc HandlerFunc) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if handlerFunc != nil {
			c.handleFetches = c.handleFetchesEach(handlerFunc)
		}
	})
}

// WithConsumeResetOffset sets the offset to start consuming from, or if
// OffsetOutOfRange is seen while fetching, to restart consuming from.
func WithConsumeResetOffset(offset kgo.Offset) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		c.addClientOption(kgo.ConsumeResetOffset(offset))
	})
}

// WithFetchIsolationLevel sets the "isolation level" used for fetching
// records.
func WithFetchIsolationLevel(level kgo.IsolationLevel) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		c.addClientOption(kgo.FetchIsolationLevel(level))
	})
}

// --- metrics ---

func WithConsumerMeterProvider(provider metric.MeterProvider) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		c.addMeterOption(kotel.MeterProvider(provider))
	})
}

func WithConsumerMetricsNamespace(ns string) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if ns != "" {
			c.namespace = ns
		}
	})
}

// --- tracing ---

func WithConsumerTracerProvider(provider trace.TracerProvider) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		c.addTracerOption(kotel.TracerProvider(provider))
	})
}

func WithConsumerTracerPropagator(propagator propagation.TextMapPropagator) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		c.addTracerOption(kotel.TracerPropagator(propagator))
	})
}

type ConsumerConfig struct {
	// SeedBrokers sets the seed brokers for the client to use, overriding the
	// default 127.0.0.1:9092.
	//
	// Any seeds that are missing a port use the default Kafka port 9092.
	Brokers string `envconfig:"KAFKA_BROKERS"`

	// SaslMechanism SASL mechanism to use for authentication.
	// Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
	// NOTE: Despite the name only one mechanism must be configured.
	SASLMechanism string `envconfig:"KAFKA_SASL_MECHANISM"`
	// User sasl username for use with the PLAIN and SASL-SCRAM-.. mechanisms.
	User string `envconfig:"KAFKA_USER"`
	// Password sasl password for use with the PLAIN and SASL-SCRAM-.. mechanism.
	Password string `envconfig:"KAFKA_PASSWORD"`

	// Enabled custom option: detecting enabling.
	Enabled bool `envconfig:"KAFKA_ENABLED"`
	// SkipFatalErrors custom option: skip fatal errors while fetching records,
	// otherwise leave the process.
	SkipFatalErrors bool `envconfig:"KAFKA_SKIP_FATAL_ERRORS"`
	// ConsumeRegex sets the client to parse all topics passed to Topics as
	// regular expressions.
	ConsumeRegex bool `envconfig:"KAFKA_CONSUME_REGEX"`
	// Topics adds topics to use for consuming.
	Topics string `envconfig:"KAFKA_TOPICS"`
	// Group sets the consumer group for the client to join and consume in.
	// This option is required if using any other group options.
	Group string `envconfig:"KAFKA_GROUP"`
	// MaxPollRecords maximum of maxPollRecords total across all fetches.
	MaxPollRecords int `envconfig:"KAFKA_MAX_POLL_RECORDS"`
	// InstanceID sets the group consumer's instance ID, switching the group member
	// from "dynamic" to "static".
	InstanceID string `envconfig:"KAFKA_INSTANCE_ID"`

	// PollInterval interval between handle batches.
	PollInterval time.Duration `envconfig:"KAFKA_POLL_INTERVAL"`
	// SuspendProcessingTimeout waiting timeout after batch processing failed (custom property).
	SuspendProcessingTimeout time.Duration `envconfig:"KAFKA_SUSPEND_PROCESSING_TIMEOUT"`
	// SuspendCommitingTimeout waiting timeout after committing failed (custom property).
	SuspendCommitingTimeout time.Duration `envconfig:"KAFKA_SUSPEND_COMMITTING_TIMEOUT"`

	// FetchMaxWait sets the maximum amount of time a broker will wait for a
	// fetch response to hit the minimum number of required bytes before returning.
	FetchMaxWait time.Duration `envconfig:"KAFKA_FETCH_MAX_WAIT"`
	// FetchMaxBytes sets the maximum amount of bytes a broker will try to send
	// during a fetch.
	FetchMaxBytes int32 `envconfig:"KAFKA_FETCH_MAX_BYTES"`
	// FetchMinBytes sets the minimum amount of bytes a broker will try to send
	// during a fetch.
	FetchMinBytes int32 `envconfig:"KAFKA_FETCH_MIN_BYTES"`
	// FetchMaxPartitionBytes sets the maximum amount of bytes that will be
	// consumed for a single partition in a fetch request.
	FetchMaxPartitionBytes int32 `envconfig:"KAFKA_FETCH_MAX_PARTITION_BYTES"`
	// DisableFetchSessions sets the client to not use fetch sessions (Kafka 1.0+).
	DisableFetchSessions bool `envconfig:"KAFKA_DISABLE_FETCH_SESSIONS"`
	// SessionTimeout sets how long a member in the group can go between
	// heartbeats, overriding the default 45,000ms. If a member does not heartbeat
	// in this timeout, the broker will remove the member from the group and
	// initiate a rebalance.
	SessionTimeout time.Duration `envconfig:"KAFKA_SESSION_TIMEOUT"`
	// RebalanceTimeout sets how long group members are allowed to take when a
	// rebalance has begun.
	RebalanceTimeout time.Duration `envconfig:"KAFKA_REBALANCE_TIMEOUT"`
	// HeartbeatInterval sets how long a group member goes between heartbeats to
	// Kafka.
	HeartbeatInterval time.Duration `envconfig:"KAFKA_HEARTBEAT_INTERVAL"`
	// RequireStableFetchOffsets sets the group consumer to require "stable" fetch
	// offsets before consuming from the group.
	RequireStableFetchOffsets bool `envconfig:"KAFKA_REQUIRE_STABLE_FETCH_OFFS"`

	// RequestTimeoutOverhead uses the given time as overhead while deadlining
	// requests.
	RequestTimeoutOverhead time.Duration `envconfig:"KAFKA_REQUEST_TIMEOUT_OVERHEAD"`
	// ConnIdleTimeout is a rough amount of time to allow connections to idle
	// before they are closed.
	ConnIdleTimeout time.Duration `envconfig:"KAFKA_CONN_IDLE_TIMEOUT"`
	// DialTimeout sets the dial timeout.
	DialTimeout time.Duration `envconfig:"KAFKA_DIAL_TIMEOUT"`
	// RequestRetries sets the number of tries that retryable requests are allowed.
	RequestRetries *int `envconfig:"KAFKA_REQUEST_RETRIES"`
	// RetryTimeout sets the upper limit on how long we allow a request to be
	// issued and then reissued on failure. That is, this control the total
	// end-to-end maximum time we allow for trying a request.
	RetryTimeout time.Duration `envconfig:"KAFKA_RETRY_TIMEOUT"`
	// BrokerMaxWriteBytes upper bounds the number of bytes written to a broker
	// connection in a single write.
	BrokerMaxWriteBytes *int32 `envconfig:"KAFKA_MAX_WRITE_BYTES"`
	// BrokerMaxReadBytes sets the maximum response size that can be read from
	// Kafka.
	BrokerMaxReadBytes *int32 `envconfig:"KAFKA_MAX_READ_BYTES"`
	// MetadataMaxAge sets the maximum age for the client's cached metadata.
	MetadataMaxAge time.Duration `envconfig:"KAFKA_METADATA_MAX_AGE"`
	// MetadataMinAge sets the minimum time between metadata queries.
	MetadataMinAge time.Duration `envconfig:"KAFKA_METADATA_MIN_AGE"`
}

func (cfg *ConsumerConfig) getLogin() string {
	return cfg.User
}

func (cfg *ConsumerConfig) getPassword() string {
	return cfg.Password
}

func (cfg *ConsumerConfig) getSASLMechanism() string {
	return cfg.SASLMechanism
}

func withEnabledFlag(v bool) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		c.enabled = v
	})
}

func withConsumerSkipFatalErrorsFlag(v bool) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v {
			c.skipFatalErrors = true
		}
	})
}

func withConsumerBrokers(brokers ...string) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if len(brokers) > 0 {
			c.addClientOption(kgo.SeedBrokers(brokers...))
		}
	})
}

func withConsumerSASL(cfg *ConsumerConfig) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if cfg.SASLMechanism == "" {
			return
		}

		mechanism, err := getSASLMechanism(cfg)
		if err != nil {
			return
		}

		c.addClientOption(kgo.SASL(mechanism))
	})
}

func withMaxPollRecords(v int) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v > 0 {
			c.batchSize = v
		}
	})
}

func withPollInterval(timeout time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if timeout > 0 {
			c.pollInterval = timeout
		}
	})
}

func withSuspendProcessingTimeout(timeout time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if timeout > 0 {
			c.suspendProcessingTimeout = timeout
		}
	})
}

func withSuspendCommitingTimeout(timeout time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if timeout > 0 {
			c.suspendCommittingTimeout = timeout
		}
	})
}

func withConsumeRegex(flag bool) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if flag {
			c.addClientOption(kgo.ConsumeRegex())
		}
	})
}

func withConsumeTopics(topics ...string) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if len(topics) > 0 {
			c.addClientOption(kgo.ConsumeRegex())
			c.addClientOption(kgo.ConsumeTopics(topics...))
		}
	})
}

func withConsumerGroup(group string) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if group != "" {
			c.groupSpecified = true

			c.addClientOption(kgo.ConsumerGroup(group))
			c.addClientOption(kgo.DisableAutoCommit())
			c.setMetricLabel("consumer_group", group)
		}
	})
}

func withConsumerInstanceID(v string) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v != "" {
			c.addClientOption(kgo.InstanceID(v))
		}
	})
}

func withConsumerDisableFetchSessions(flag bool) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if flag {
			c.addClientOption(kgo.DisableFetchSessions())
		}
	})
}

func withSessionTimeout(timeout time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if timeout > 0 {
			c.addClientOption(kgo.SessionTimeout(timeout))
		}
	})
}

func withRebalanceTimeout(timeout time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if timeout > 0 {
			c.addClientOption(kgo.RebalanceTimeout(timeout))
		}
	})
}

func withHeartbeatInterval(interval time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if interval > 0 {
			c.addClientOption(kgo.HeartbeatInterval(interval))
		}
	})
}

func withRequireStableFetchOffsets(flag bool) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if flag {
			c.addClientOption(kgo.RequireStableFetchOffsets())
		}
	})
}

func withConsumerRequestTimeoutOverhead(overhead time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if overhead > 0 {
			c.addClientOption(kgo.RequestTimeoutOverhead(overhead))
		}
	})
}

func withConsumerConnIdleTimeout(timeout time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if timeout > 0 {
			c.addClientOption(kgo.ConnIdleTimeout(timeout))
		}
	})
}

func withConsumerDialTimeout(timeout time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if timeout > 0 {
			c.addClientOption(kgo.DialTimeout(timeout))
		}
	})
}

func withConsumerRequestRetries(n *int) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if n != nil {
			c.addClientOption(kgo.RequestRetries(*n))
		}
	})
}

func withConsumerRetryTimeout(timeout time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if timeout > 0 {
			c.addClientOption(kgo.RetryTimeout(timeout))
		}
	})
}

func withConsumerBrokerMaxWriteBytes(v *int32) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v != nil {
			c.addClientOption(kgo.BrokerMaxWriteBytes(*v))
		}
	})
}

func withConsumerBrokerMaxReadBytes(v *int32) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v != nil {
			c.addClientOption(kgo.BrokerMaxReadBytes(*v))
		}
	})
}

func withConsumerMetadataMaxAge(age time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if age > 0 {
			c.addClientOption(kgo.MetadataMaxAge(age))
		}
	})
}

func withConsumerMetadataMinAge(age time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if age > 0 {
			c.addClientOption(kgo.MetadataMinAge(age))
		}
	})
}
