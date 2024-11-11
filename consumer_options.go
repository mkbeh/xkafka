package kafka

import (
	"crypto/tls"
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
	return consumerOptionFunc(func(p *Consumer) {
		if cfg == nil {
			return
		}

		opts := []ConsumerOption{
			withEnabledFlag(cfg.Enabled),
			withConsumerBrokers(strings.Split(cfg.Brokers, ",")...),
			withConsumerSASL(cfg),
			withMaxPollRecords(cfg.MaxPollRecords),
			withPollInterval(cfg.PollInterval),
			withSuspendProcessingTimeout(cfg.SuspendProcessingTimeout),
			withSuspendCommitingTimeout(cfg.SuspendCommitingTimeout),
			withConsumeTopics(strings.Split(cfg.Topics, ",")...),
			withConsumerGroup(cfg.Group),
		}

		for _, opt := range opts {
			opt.apply(p)
		}
	})
}

func WithConsumerClientID(v string) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v != "" {
			c.addClientOptions(kgo.ClientID(v))
			c.addTracerOption(kotel.ClientID(v))
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
	return consumerOptionFunc(func(p *Consumer) {
		p.addClientOption(kgo.DialTLSConfig(tls))
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

// WithConsumerInstanceID sets the group consumer's instance ID, switching the group member
// from "dynamic" to "static".
func WithConsumerInstanceID(v string) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v != "" {
			c.addClientOption(kgo.InstanceID(v))
		}
	})
}

// --- metrics ---

func WithConsumerMeterProvider(provider metric.MeterProvider) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		c.addMeterOption(kotel.MeterProvider(provider))
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

	// Enabled custom option for detecting enabling.
	Enabled bool `envconfig:"KAFKA_ENABLED"`
	// Topics adds topics to use for consuming.
	Topics string `envconfig:"KAFKA_TOPICS"`
	// Group sets the consumer group for the client to join and consume in.
	// This option is required if using any other group options.
	Group string `envconfig:"KAFKA_GROUP"`
	// MaxPollRecords maximum of maxPollRecords total across all fetches.
	MaxPollRecords int `envconfig:"KAFKA_MAX_POLL_RECORDS"`

	// PollInterval interval between handle batches.
	PollInterval time.Duration `envconfig:"KAFKA_POLL_INTERVAL"`
	// SuspendProcessingTimeout waiting timeout after batch processing failed (custom property).
	SuspendProcessingTimeout time.Duration `envconfig:"KAFKA_SUSPEND_PROCESSING_TIMEOUT"`
	// SuspendCommitingTimeout waiting timeout after committing failed (custom property).
	SuspendCommitingTimeout time.Duration `envconfig:"KAFKA_SUSPEND_COMMITTING_TIMEOUT"`
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

func withConsumeTopics(topics ...string) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if len(topics) > 0 {
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
		}
	})
}
