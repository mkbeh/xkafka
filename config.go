package kafka

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	/////////////////////
	// GENERAL SECTION //
	/////////////////////

	// Brokers sets the seed brokers for the client to use.
	//
	// Any seed without a port uses the default Kafka port 9092.
	Brokers string `envconfig:"KAFKA_BROKERS"`

	// SASLMechanism is the SASL mechanism used for authentication.
	// Supported values: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
	SASLMechanism string `envconfig:"KAFKA_SASL_MECHANISM"`

	// User is the SASL username.
	User string `envconfig:"KAFKA_USER"`

	// Password is the SASL password.
	Password string `envconfig:"KAFKA_PASSWORD"`

	// RequestTimeoutOverhead adds extra time while setting request deadlines.
	RequestTimeoutOverhead time.Duration `envconfig:"KAFKA_REQUEST_TIMEOUT_OVERHEAD"`

	// ConnIdleTimeout controls how long idle broker connections are kept open.
	ConnIdleTimeout time.Duration `envconfig:"KAFKA_CONN_IDLE_TIMEOUT"`

	// DialTimeout sets the timeout for opening broker connections.
	DialTimeout time.Duration `envconfig:"KAFKA_DIAL_TIMEOUT"`

	// RequestRetries sets how many times retryable requests may be retried.
	RequestRetries *int `envconfig:"KAFKA_REQUEST_RETRIES"`

	// RetryTimeout limits the total time allowed for retrying a request.
	RetryTimeout time.Duration `envconfig:"KAFKA_RETRY_TIMEOUT"`

	// BrokerMaxWriteBytes limits how many bytes may be written to a broker in one write.
	BrokerMaxWriteBytes *int32 `envconfig:"KAFKA_MAX_WRITE_BYTES"`

	// BrokerMaxReadBytes limits the maximum response size read from a broker.
	BrokerMaxReadBytes *int32 `envconfig:"KAFKA_MAX_READ_BYTES"`

	// MetadataMaxAge sets the maximum age of cached Kafka metadata.
	MetadataMaxAge time.Duration `envconfig:"KAFKA_METADATA_MAX_AGE"`

	// MetadataMinAge sets the minimum time between metadata refreshes.
	MetadataMinAge time.Duration `envconfig:"KAFKA_METADATA_MIN_AGE"`

	// AlwaysRetryEOF retries EOF errors instead of treating them as terminal connection failures.
	AlwaysRetryEOF bool `envconfig:"KAFKA_ALWAYS_RETRY_EOF"`

	//////////////////////
	// PRODUCER SECTION //
	//////////////////////

	// DefaultProduceTopic sets the default topic for records without an explicit topic.
	DefaultProduceTopic string `envconfig:"KAFKA_DEFAULT_PRODUCE_TOPIC"`

	// ProducerBatchMaxBytes limits the maximum size of a produced record batch.
	ProducerBatchMaxBytes int32 `envconfig:"KAFKA_PRODUCER_BATCH_MAX_BYTES"`

	// MaxBufferedRecords limits how many records the producer may buffer.
	MaxBufferedRecords int `envconfig:"KAFKA_MAX_BUFFERED_RECORDS"`

	// MaxBufferedBytes limits how many bytes the producer may buffer.
	MaxBufferedBytes int `envconfig:"KAFKA_MAX_BUFFERED_BYTES"`

	// ProduceRequestTimeout limits how long brokers may take to respond to produce requests.
	ProduceRequestTimeout time.Duration `envconfig:"KAFKA_PRODUCE_REQUEST_TIMEOUT"`

	// RecordRetries sets how many times producing records may be retried.
	RecordRetries int `envconfig:"KAFKA_RECORD_RETRIES"`

	// ProducerLinger sets how long records may wait for batching before a produce request is built.
	ProducerLinger *time.Duration `envconfig:"KAFKA_PRODUCER_LINGER"`

	// RecordDeliveryTimeout limits how long a record may remain buffered before timing out.
	RecordDeliveryTimeout time.Duration `envconfig:"KAFKA_RECORD_DELIVERY_TIMEOUT"`

	// TransactionalID identifies the transactional producer.
	//
	// It is used by producer transactions and group transact sessions.
	TransactionalID string `envconfig:"KAFKA_TRANSACTIONAL_ID"`

	// TransactionTimeout limits how long a Kafka transaction may remain open.
	TransactionTimeout time.Duration `envconfig:"KAFKA_TRANSACTION_TIMEOUT"`

	//////////////////////
	// CONSUMER SECTION //
	//////////////////////

	// ConsumeRegex treats configured topics as regular expressions.
	ConsumeRegex bool `envconfig:"KAFKA_CONSUME_REGEX"`

	// Topics contains Kafka topics consumed by the consumer or group transact session.
	Topics string `envconfig:"KAFKA_TOPICS"`

	// MaxPollRecords limits how many records are handled in one poll iteration.
	MaxPollRecords int `envconfig:"KAFKA_MAX_POLL_RECORDS"`

	// FetchMaxWait limits how long a broker may wait before returning a fetch response.
	FetchMaxWait time.Duration `envconfig:"KAFKA_FETCH_MAX_WAIT"`

	// FetchMaxBytes limits the maximum bytes returned in one fetch response.
	FetchMaxBytes int32 `envconfig:"KAFKA_FETCH_MAX_BYTES"`

	// FetchMinBytes sets the minimum bytes a broker tries to return in one fetch response.
	FetchMinBytes int32 `envconfig:"KAFKA_FETCH_MIN_BYTES"`

	// FetchMaxPartitionBytes limits how many bytes may be fetched from one partition.
	FetchMaxPartitionBytes int32 `envconfig:"KAFKA_FETCH_MAX_PARTITION_BYTES"`

	// DisableFetchSessions disables Kafka fetch sessions.
	DisableFetchSessions bool `envconfig:"KAFKA_DISABLE_FETCH_SESSIONS"`

	// Rack identifies the client rack for rack-aware fetching and consumer assignment.
	Rack string `envconfig:"KAFKA_RACK"`

	// MaxConcurrentFetches limits how many fetch requests may be in flight or buffered at once.
	//
	// It can be used together with FetchMaxBytes to bound consumer memory usage.
	MaxConcurrentFetches *int `envconfig:"KAFKA_MAX_CONCURRENT_FETCHES"`

	////////////////////////////
	// CONSUMER GROUP SECTION //
	////////////////////////////

	// Group identifies the Kafka consumer group.
	Group string `envconfig:"KAFKA_GROUP"`

	// InstanceID sets a static consumer group member ID.
	InstanceID string `envconfig:"KAFKA_INSTANCE_ID"`

	// SessionTimeout controls how long a group member may go without heartbeating.
	SessionTimeout time.Duration `envconfig:"KAFKA_SESSION_TIMEOUT"`

	// RebalanceTimeout controls how long group members may take during a rebalance.
	RebalanceTimeout time.Duration `envconfig:"KAFKA_REBALANCE_TIMEOUT"`

	// HeartbeatInterval controls how often a group member sends heartbeats.
	HeartbeatInterval time.Duration `envconfig:"KAFKA_HEARTBEAT_INTERVAL"`

	/////////////////////////
	// SHARE GROUP SECTION //
	/////////////////////////

	// ShareGroup identifies the Kafka share group.
	//
	// If set, share group consuming is used instead of a regular consumer group.
	ShareGroup string `envconfig:"KAFKA_SHARE_GROUP"`

	// ShareMaxRecords limits how many records ShareFetch may return.
	//
	// If zero, the franz-go default is used.
	ShareMaxRecords int32 `envconfig:"KAFKA_SHARE_MAX_RECORDS"`

	// ShareMaxRecordsStrict asks the broker to strictly cap records per ShareFetch.
	ShareMaxRecordsStrict bool `envconfig:"KAFKA_SHARE_MAX_RECORDS_STRICT"`

	// ShareRejectAfterDeliveries controls when failed share group records are rejected.
	//
	// If zero, failed records are released for redelivery.
	ShareRejectAfterDeliveries int32 `envconfig:"KAFKA_SHARE_REJECT_AFTER_DELIVERIES"`

	// ShareReleaseTimeout delays AckRelease after share handler errors.
	//
	// If zero, failed records are released immediately.
	ShareReleaseTimeout time.Duration `envconfig:"KAFKA_SHARE_RELEASE_TIMEOUT"`

	/////////////////////
	// SERVICE SECTION //
	/////////////////////

	// Enabled enables this Kafka component.
	Enabled bool `envconfig:"KAFKA_ENABLED"`

	// SkipFatalErrors allows the consumer to continue after non-retriable fetch errors.
	SkipFatalErrors bool `envconfig:"KAFKA_SKIP_FATAL_ERRORS"`

	// PollInterval sets the interval between consumer poll iterations.
	PollInterval time.Duration `envconfig:"KAFKA_POLL_INTERVAL"`

	// SuspendProcessingTimeout sets the wait time after handler errors.
	SuspendProcessingTimeout time.Duration `envconfig:"KAFKA_SUSPEND_PROCESSING_TIMEOUT"`

	// SuspendCommittingTimeout sets the wait time after offset commit or ack errors.
	SuspendCommittingTimeout time.Duration `envconfig:"KAFKA_SUSPEND_COMMITTING_TIMEOUT"`
}

////////////////////////////////////////////////////////
// Option interfaces
////////////////////////////////////////////////////////

type Option interface {
	ProducerOption
	ConsumerOption
	GroupTransactSessionOption
}

type ProducerOption interface {
	applyProducer(*Producer)
}

type ConsumerOption interface {
	applyConsumer(*Consumer)
}

type GroupTransactSessionOption interface {
	applyGroupTransactSession(*GroupTransactSession)
}

type producerOptionFunc func(*Producer)

func (f producerOptionFunc) applyProducer(p *Producer) {
	f(p)
}

type consumerOptionFunc func(*Consumer)

func (f consumerOptionFunc) applyConsumer(c *Consumer) {
	f(c)
}

type groupTransactSessionOptionFunc func(*GroupTransactSession)

func (f groupTransactSessionOptionFunc) applyGroupTransactSession(s *GroupTransactSession) {
	f(s)
}

////////////////////////////////////////////////////////
// Common options
////////////////////////////////////////////////////////

type configOption struct {
	config *Config
}

func WithConfig(config *Config) Option {
	return configOption{config: config}
}

func (o configOption) applyProducer(p *Producer) {
	if o.config == nil {
		return
	}

	applyProducerConfig(p, o.config)
}

func (o configOption) applyConsumer(c *Consumer) {
	if o.config == nil {
		return
	}

	applyConsumerConfig(c, o.config)
}

func (o configOption) applyGroupTransactSession(s *GroupTransactSession) {
	if o.config == nil {
		return
	}

	applyGroupTransactSessionConfig(s, o.config)
}

////////////////////////////////////////////////////////
// Producer options
////////////////////////////////////////////////////////

func WithProducerClientID(v string) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if v != "" {
			p.addClientOption(kgo.ClientID(v))
			p.addTracerOption(kotel.ClientID(v))
			p.id = fmt.Sprintf("%s-%s", v, GenerateUUID())
		}
	})
}

func WithProducerLogger(v *slog.Logger) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if v != nil {
			p.logger = v
		}
	})
}

func WithProducerPartitioner(partitioner kgo.Partitioner) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if partitioner != nil {
			p.addClientOption(kgo.RecordPartitioner(partitioner))
		}
	})
}

// WithProducerTLS enables dialing brokers with the given TLS config.
//
// Every dial clones the input config. If ServerName is not specified,
// franz-go derives it from the broker address.
func WithProducerTLS(tlsConfig *tls.Config) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if tlsConfig != nil {
			p.addClientOption(kgo.DialTLSConfig(tlsConfig))
		}
	})
}

// WithProducerCB sets a callback called after each async produce attempt.
func WithProducerCB(cb func(record *kgo.Record, err error)) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if cb != nil {
			p.promiseFunc = cb
		}
	})
}

func WithProducerRequiredAcks(acks kgo.Acks) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		p.addClientOption(kgo.RequiredAcks(acks))
	})
}

func WithProducerBatchCompression(preference ...kgo.CompressionCodec) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if len(preference) > 0 {
			p.addClientOption(kgo.ProducerBatchCompression(preference...))
		}
	})
}

func WithProducerAllowIdempotentProduceCancellation() ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		p.addClientOption(kgo.AllowIdempotentProduceCancellation())
	})
}

// WithProducerBatchMaxBytesFn sets a per-topic maximum record batch size.
//
// Use this when different topics have different broker-side max.message.bytes values.
func WithProducerBatchMaxBytesFn(fn func(topic string) int32) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if fn != nil {
			p.addClientOption(kgo.ProducerBatchMaxBytesFn(fn))
		}
	})
}

////////////////////////////////////////////////////////
// Producer metrics options
////////////////////////////////////////////////////////

func WithProducerMeterProvider(provider metric.MeterProvider) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if provider != nil {
			p.addMeterOption(kotel.MeterProvider(provider))
		}
	})
}

func WithProducerMetricsNamespace(ns string) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if ns != "" {
			p.namespace = ns
		}
	})
}

////////////////////////////////////////////////////////
// Producer tracing options
////////////////////////////////////////////////////////

func WithProducerTracerProvider(provider trace.TracerProvider) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if provider != nil {
			p.addTracerOption(kotel.TracerProvider(provider))
		}
	})
}

func WithProducerTracerPropagator(propagator propagation.TextMapPropagator) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if propagator != nil {
			p.addTracerOption(kotel.TracerPropagator(propagator))
		}
	})
}

////////////////////////////////////////////////////////
// Consumer options
////////////////////////////////////////////////////////

func WithConsumerClientID(v string) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v != "" {
			c.addClientOption(kgo.ClientID(v))
			c.addTracerOption(kotel.ClientID(v))
			c.id = fmt.Sprintf("%s-%s", v, GenerateUUID())
		}
	})
}

// WithConsumerTLS enables dialing brokers with the given TLS config.
//
// Every dial clones the input config. If ServerName is not specified,
// franz-go derives it from the broker address.
func WithConsumerTLS(tlsConfig *tls.Config) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if tlsConfig != nil {
			c.addClientOption(kgo.DialTLSConfig(tlsConfig))
		}
	})
}

func WithConsumerLogger(v *slog.Logger) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v != nil {
			c.logger = v
		}
	})
}

// WithConsumerGroupBalancers sets the group balancers to use for dividing topic
// partitions among group members.
//
// This option is equivalent to Kafka's partition.assignment.strategies option.
func WithConsumerGroupBalancers(balancers ...kgo.GroupBalancer) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if len(balancers) > 0 {
			c.addClientOption(kgo.Balancers(balancers...))
		}
	})
}

// WithConsumerBatchHandler sets a batch record handler.
func WithConsumerBatchHandler(handlerFunc BatchHandlerFunc) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if handlerFunc != nil {
			c.handleFetches = c.handleFetchesBatch(handlerFunc)
		}
	})
}

// WithConsumerHandler sets a record handler.
func WithConsumerHandler(handlerFunc HandlerFunc) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if handlerFunc != nil {
			c.handleFetches = c.handleFetchesEach(handlerFunc)
		}
	})
}

// WithConsumerShareHandler sets a share group record handler.
func WithConsumerShareHandler(handlerFunc HandlerFunc) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if handlerFunc != nil {
			c.handleFetches = c.handleShareFetchesEach(handlerFunc)
		}
	})
}

// WithConsumerShareBatchHandler sets a share group batch record handler.
func WithConsumerShareBatchHandler(handlerFunc BatchHandlerFunc) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if handlerFunc != nil {
			c.handleFetches = c.handleShareFetchesBatch(handlerFunc)
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

// WithFetchIsolationLevel sets the isolation level used for fetching records.
func WithFetchIsolationLevel(level kgo.IsolationLevel) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		c.addClientOption(kgo.FetchIsolationLevel(level))
	})
}

// WithConsumerShareAckCallback sets a callback for share group acknowledgement results.
func WithConsumerShareAckCallback(fn func(*kgo.Client, kgo.ShareAckResults)) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if fn != nil {
			c.addClientOption(kgo.ShareAckCallback(fn))
		}
	})
}

////////////////////////////////////////////////////////
// Consumer metrics options
////////////////////////////////////////////////////////

func WithConsumerMeterProvider(provider metric.MeterProvider) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if provider != nil {
			c.addMeterOption(kotel.MeterProvider(provider))
		}
	})
}

func WithConsumerMetricsNamespace(ns string) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if ns != "" {
			c.namespace = ns
		}
	})
}

////////////////////////////////////////////////////////
// Consumer tracing options
////////////////////////////////////////////////////////

func WithConsumerTracerProvider(provider trace.TracerProvider) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if provider != nil {
			c.addTracerOption(kotel.TracerProvider(provider))
		}
	})
}

func WithConsumerTracerPropagator(propagator propagation.TextMapPropagator) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if propagator != nil {
			c.addTracerOption(kotel.TracerPropagator(propagator))
		}
	})
}

////////////////////////////////////////////////////////
// Group transact session options
////////////////////////////////////////////////////////

func WithGroupTransactSessionClientID(v string) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if v != "" {
			s.addClientOption(kgo.ClientID(v))
			s.addTracerOption(kotel.ClientID(v))
			s.id = fmt.Sprintf("%s-%s", v, GenerateUUID())
		}
	})
}

func WithGroupTransactSessionLogger(v *slog.Logger) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if v != nil {
			s.logger = v
		}
	})
}

func WithGroupTransactSessionHandler(handler BatchGroupTransactSessionFunc) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if handler != nil {
			s.handleFetches = s.handleFetchesBatch(handler)
		}
	})
}

// WithGroupTransactSessionTLS enables dialing brokers with the given TLS config.
//
// Every dial clones the input config. If ServerName is not specified,
// franz-go derives it from the broker address.
func WithGroupTransactSessionTLS(tlsConfig *tls.Config) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if tlsConfig != nil {
			s.addClientOption(kgo.DialTLSConfig(tlsConfig))
		}
	})
}

// WithGroupTransactSessionGroupBalancers sets the group balancers used for partition assignment.
//
// This option is equivalent to Kafka's partition.assignment.strategies option.
func WithGroupTransactSessionGroupBalancers(
	balancers ...kgo.GroupBalancer,
) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if len(balancers) > 0 {
			s.addClientOption(kgo.Balancers(balancers...))
		}
	})
}

// WithGroupTransactSessionFetchIsolationLevel sets the isolation level used for fetching records.
//
// For EOS processing, ReadCommitted is the recommended default.
func WithGroupTransactSessionFetchIsolationLevel(
	level kgo.IsolationLevel,
) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		s.addClientOption(kgo.FetchIsolationLevel(level))
	})
}

func WithGroupTransactSessionPartitioner(partitioner kgo.Partitioner) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if partitioner != nil {
			s.addClientOption(kgo.RecordPartitioner(partitioner))
		}
	})
}

// WithGroupTransactSessionCB sets a callback called after each async produce attempt.
func WithGroupTransactSessionCB(cb func(record *kgo.Record, err error)) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if cb != nil {
			s.promiseFunc = cb
		}
	})
}

func WithGroupTransactSessionRequiredAcks(acks kgo.Acks) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		s.addClientOption(kgo.RequiredAcks(acks))
	})
}

func WithGroupTransactSessionBatchCompression(
	preference ...kgo.CompressionCodec,
) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if len(preference) > 0 {
			s.addClientOption(kgo.ProducerBatchCompression(preference...))
		}
	})
}

func WithGroupTransactSessionAllowIdempotentProduceCancellation() GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		s.addClientOption(kgo.AllowIdempotentProduceCancellation())
	})
}

// WithGroupTransactSessionBatchMaxBytesFn sets a per-topic maximum record batch size.
//
// Use this when different topics have different broker-side max.message.bytes values.
func WithGroupTransactSessionBatchMaxBytesFn(
	fn func(topic string) int32,
) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if fn != nil {
			s.addClientOption(kgo.ProducerBatchMaxBytesFn(fn))
		}
	})
}

////////////////////////////////////////////////////////
// Group transact session metrics options
////////////////////////////////////////////////////////

func WithGroupTransactSessionMeterProvider(
	provider metric.MeterProvider,
) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if provider != nil {
			s.addMeterOption(kotel.MeterProvider(provider))
		}
	})
}

func WithGroupTransactSessionMetricsNamespace(ns string) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if ns != "" {
			s.namespace = ns
		}
	})
}

////////////////////////////////////////////////////////
// Group transact session tracing options
////////////////////////////////////////////////////////

func WithGroupTransactSessionTracerProvider(
	provider trace.TracerProvider,
) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if provider != nil {
			s.addTracerOption(kotel.TracerProvider(provider))
		}
	})
}

func WithGroupTransactSessionTracerPropagator(
	propagator propagation.TextMapPropagator,
) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if propagator != nil {
			s.addTracerOption(kotel.TracerPropagator(propagator))
		}
	})
}

////////////////////////////////////////////////////////
// Producer config application
////////////////////////////////////////////////////////

func applyProducerConfig(p *Producer, cfg *Config) {
	opts := []ProducerOption{
		withProducerBrokers(splitCSV(cfg.Brokers)...),
		withProducerSASL(cfg),
		withProducerRequestTimeoutOverhead(cfg.RequestTimeoutOverhead),
		withProducerConnIdleTimeout(cfg.ConnIdleTimeout),
		withProducerDialTimeout(cfg.DialTimeout),
		withProducerRequestRetries(cfg.RequestRetries),
		withProducerRetryTimeout(cfg.RetryTimeout),
		withProducerBrokerMaxWriteBytes(cfg.BrokerMaxWriteBytes),
		withProducerBrokerMaxReadBytes(cfg.BrokerMaxReadBytes),
		withProducerMetadataMaxAge(cfg.MetadataMaxAge),
		withProducerMetadataMinAge(cfg.MetadataMinAge),
		withProducerAlwaysRetryEOF(cfg.AlwaysRetryEOF),
		withDefaultProduceTopic(cfg.DefaultProduceTopic),
		withProducerBatchMaxBytes(cfg.ProducerBatchMaxBytes),
		withProducerMaxBufferedRecords(cfg.MaxBufferedRecords),
		withProducerMaxBufferedBytes(cfg.MaxBufferedBytes),
		withProduceRequestTimeout(cfg.ProduceRequestTimeout),
		withProducerRecordRetries(cfg.RecordRetries),
		withProducerLinger(cfg.ProducerLinger),
		withProducerRecordDeliveryTimeout(cfg.RecordDeliveryTimeout),
		withProducerTransactionalID(cfg.TransactionalID),
		withProducerTransactionTimeout(cfg.TransactionTimeout),
	}

	for _, opt := range opts {
		opt.applyProducer(p)
	}
}

func withProducerBrokers(brokers ...string) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if len(brokers) > 0 {
			p.addClientOption(kgo.SeedBrokers(brokers...))
		}
	})
}

func withProducerSASL(cfg *Config) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if cfg.SASLMechanism == "" {
			return
		}

		mechanism, err := getSASLMechanism(cfg)
		if err != nil || mechanism == nil {
			return
		}

		p.addClientOption(kgo.SASL(mechanism))
	})
}

func withProducerRequestTimeoutOverhead(overhead time.Duration) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if overhead > 0 {
			p.addClientOption(kgo.RequestTimeoutOverhead(overhead))
		}
	})
}

func withProducerConnIdleTimeout(timeout time.Duration) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if timeout > 0 {
			p.addClientOption(kgo.ConnIdleTimeout(timeout))
		}
	})
}

func withProducerDialTimeout(timeout time.Duration) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if timeout > 0 {
			p.addClientOption(kgo.DialTimeout(timeout))
		}
	})
}

func withProducerRequestRetries(n *int) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if n != nil {
			p.addClientOption(kgo.RequestRetries(*n))
		}
	})
}

func withProducerRetryTimeout(timeout time.Duration) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if timeout > 0 {
			p.addClientOption(kgo.RetryTimeout(timeout))
		}
	})
}

func withProducerBrokerMaxWriteBytes(v *int32) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if v != nil {
			p.addClientOption(kgo.BrokerMaxWriteBytes(*v))
		}
	})
}

func withProducerBrokerMaxReadBytes(v *int32) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if v != nil {
			p.addClientOption(kgo.BrokerMaxReadBytes(*v))
		}
	})
}

func withProducerMetadataMaxAge(age time.Duration) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if age > 0 {
			p.addClientOption(kgo.MetadataMaxAge(age))
		}
	})
}

func withProducerMetadataMinAge(age time.Duration) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if age > 0 {
			p.addClientOption(kgo.MetadataMinAge(age))
		}
	})
}

func withProducerAlwaysRetryEOF(enabled bool) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if enabled {
			p.addClientOption(kgo.AlwaysRetryEOF())
		}
	})
}

func withDefaultProduceTopic(topic string) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if topic != "" {
			p.addClientOption(kgo.DefaultProduceTopic(topic))
		}
	})
}

func withProducerBatchMaxBytes(v int32) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if v != 0 {
			p.addClientOption(kgo.ProducerBatchMaxBytes(v))
		}
	})
}

func withProducerMaxBufferedRecords(n int) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if n != 0 {
			p.addClientOption(kgo.MaxBufferedRecords(n))
		}
	})
}

func withProducerMaxBufferedBytes(n int) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if n != 0 {
			p.addClientOption(kgo.MaxBufferedBytes(n))
		}
	})
}

func withProduceRequestTimeout(timeout time.Duration) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if timeout > 0 {
			p.addClientOption(kgo.ProduceRequestTimeout(timeout))
		}
	})
}

func withProducerRecordRetries(n int) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if n > 0 {
			p.addClientOption(kgo.RecordRetries(n))
		}
	})
}

func withProducerLinger(linger *time.Duration) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if linger != nil {
			p.addClientOption(kgo.ProducerLinger(*linger))
		}
	})
}

func withProducerRecordDeliveryTimeout(timeout time.Duration) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if timeout > 0 {
			p.addClientOption(kgo.RecordDeliveryTimeout(timeout))
		}
	})
}

func withProducerTransactionalID(id string) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if id != "" {
			p.addClientOption(kgo.TransactionalID(id))
		}
	})
}

func withProducerTransactionTimeout(timeout time.Duration) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if timeout > 0 {
			p.addClientOption(kgo.TransactionTimeout(timeout))
		}
	})
}

////////////////////////////////////////////////////////
// Consumer config application
////////////////////////////////////////////////////////

func applyConsumerConfig(c *Consumer, cfg *Config) {
	opts := []ConsumerOption{
		withEnabledFlag(cfg.Enabled),
		withConsumerSkipFatalErrorsFlag(cfg.SkipFatalErrors),
		withConsumerBrokers(splitCSV(cfg.Brokers)...),
		withConsumerSASL(cfg),
		withMaxPollRecords(cfg.MaxPollRecords),
		withPollInterval(cfg.PollInterval),
		withSuspendProcessingTimeout(cfg.SuspendProcessingTimeout),
		withSuspendCommittingTimeout(cfg.SuspendCommittingTimeout),
		withConsumeRegex(cfg.ConsumeRegex),
		withConsumeTopics(splitCSV(cfg.Topics)...),
		withConsumerGroup(cfg.Group),
		withConsumerShareGroup(cfg.ShareGroup),
		withShareMaxRecords(cfg.ShareMaxRecords),
		withShareMaxRecordsStrict(cfg.ShareMaxRecordsStrict),
		withShareRejectAfterDeliveries(cfg.ShareRejectAfterDeliveries),
		withShareReleaseTimeout(cfg.ShareReleaseTimeout),
		withConsumerInstanceID(cfg.InstanceID),
		withConsumerDisableFetchSessions(cfg.DisableFetchSessions),
		withConsumerFetchMaxWait(cfg.FetchMaxWait),
		withConsumerFetchMaxBytes(cfg.FetchMaxBytes),
		withConsumerFetchMinBytes(cfg.FetchMinBytes),
		withConsumerFetchMaxPartitionBytes(cfg.FetchMaxPartitionBytes),
		withSessionTimeout(cfg.SessionTimeout),
		withRebalanceTimeout(cfg.RebalanceTimeout),
		withHeartbeatInterval(cfg.HeartbeatInterval),
		withConsumerRequestTimeoutOverhead(cfg.RequestTimeoutOverhead),
		withConsumerConnIdleTimeout(cfg.ConnIdleTimeout),
		withConsumerDialTimeout(cfg.DialTimeout),
		withConsumerRequestRetries(cfg.RequestRetries),
		withConsumerRetryTimeout(cfg.RetryTimeout),
		withConsumerBrokerMaxWriteBytes(cfg.BrokerMaxWriteBytes),
		withConsumerBrokerMaxReadBytes(cfg.BrokerMaxReadBytes),
		withConsumerMetadataMaxAge(cfg.MetadataMaxAge),
		withConsumerMetadataMinAge(cfg.MetadataMinAge),
		withConsumerAlwaysRetryEOF(cfg.AlwaysRetryEOF),
		withConsumerRack(cfg.Rack),
		withConsumerMaxConcurrentFetches(cfg.MaxConcurrentFetches),
	}

	for _, opt := range opts {
		opt.applyConsumer(c)
	}
}

func withEnabledFlag(v bool) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		c.enabled = v
	})
}

func withConsumerSkipFatalErrorsFlag(v bool) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v {
			c.skipFatalErrors = v
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

func withConsumerSASL(cfg *Config) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if cfg.SASLMechanism == "" {
			return
		}

		mechanism, err := getSASLMechanism(cfg)
		if err != nil || mechanism == nil {
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

func withSuspendCommittingTimeout(timeout time.Duration) ConsumerOption {
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

func withConsumerShareGroup(group string) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if group != "" {
			c.setMetricLabel("consumer_group", group)
			c.addClientOption(kgo.ShareGroup(group))
		}
	})
}

func withShareMaxRecords(n int32) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if n > 0 {
			c.addClientOption(kgo.ShareMaxRecords(n))
		}
	})
}

func withShareMaxRecordsStrict(strict bool) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if strict {
			c.addClientOption(kgo.ShareMaxRecordsStrict())
		}
	})
}

func withShareRejectAfterDeliveries(n int32) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if n > 0 {
			c.shareRejectAfterDeliveries = n
		}
	})
}

func withShareReleaseTimeout(timeout time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if timeout > 0 {
			c.shareReleaseTimeout = timeout
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

func withConsumerFetchMaxWait(timeout time.Duration) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if timeout > 0 {
			c.addClientOption(kgo.FetchMaxWait(timeout))
		}
	})
}

func withConsumerFetchMaxBytes(v int32) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v > 0 {
			c.addClientOption(kgo.FetchMaxBytes(v))
		}
	})
}

func withConsumerFetchMinBytes(v int32) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v > 0 {
			c.addClientOption(kgo.FetchMinBytes(v))
		}
	})
}

func withConsumerFetchMaxPartitionBytes(v int32) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if v > 0 {
			c.addClientOption(kgo.FetchMaxPartitionBytes(v))
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

func withConsumerAlwaysRetryEOF(enabled bool) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if enabled {
			c.addClientOption(kgo.AlwaysRetryEOF())
		}
	})
}

func withConsumerRack(rack string) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if rack != "" {
			c.addClientOption(kgo.Rack(rack))
		}
	})
}

func withConsumerMaxConcurrentFetches(n *int) ConsumerOption {
	return consumerOptionFunc(func(c *Consumer) {
		if n != nil {
			c.addClientOption(kgo.MaxConcurrentFetches(*n))
		}
	})
}

////////////////////////////////////////////////////////
// Group transact session config application
////////////////////////////////////////////////////////

func applyGroupTransactSessionConfig(s *GroupTransactSession, cfg *Config) {
	opts := []GroupTransactSessionOption{
		withGroupTransactSessionEnabledFlag(cfg.Enabled),
		withGroupTransactSessionSkipFatalErrorsFlag(cfg.SkipFatalErrors),

		withGroupTransactSessionBrokers(splitCSV(cfg.Brokers)...),
		withGroupTransactSessionSASL(cfg),
		withGroupTransactSessionRequestTimeoutOverhead(cfg.RequestTimeoutOverhead),
		withGroupTransactSessionConnIdleTimeout(cfg.ConnIdleTimeout),
		withGroupTransactSessionDialTimeout(cfg.DialTimeout),
		withGroupTransactSessionRequestRetries(cfg.RequestRetries),
		withGroupTransactSessionRetryTimeout(cfg.RetryTimeout),
		withGroupTransactSessionBrokerMaxWriteBytes(cfg.BrokerMaxWriteBytes),
		withGroupTransactSessionBrokerMaxReadBytes(cfg.BrokerMaxReadBytes),
		withGroupTransactSessionMetadataMaxAge(cfg.MetadataMaxAge),
		withGroupTransactSessionMetadataMinAge(cfg.MetadataMinAge),
		withGroupTransactSessionAlwaysRetryEOF(cfg.AlwaysRetryEOF),

		withGroupTransactSessionDefaultProduceTopic(cfg.DefaultProduceTopic),
		withGroupTransactSessionProducerBatchMaxBytes(cfg.ProducerBatchMaxBytes),
		withGroupTransactSessionMaxBufferedRecords(cfg.MaxBufferedRecords),
		withGroupTransactSessionMaxBufferedBytes(cfg.MaxBufferedBytes),
		withGroupTransactSessionProduceRequestTimeout(cfg.ProduceRequestTimeout),
		withGroupTransactSessionRecordRetries(cfg.RecordRetries),
		withGroupTransactSessionProducerLinger(cfg.ProducerLinger),
		withGroupTransactSessionRecordDeliveryTimeout(cfg.RecordDeliveryTimeout),
		withGroupTransactSessionTransactionalID(cfg.TransactionalID),
		withGroupTransactSessionTransactionTimeout(cfg.TransactionTimeout),

		withGroupTransactSessionMaxPollRecords(cfg.MaxPollRecords),
		withGroupTransactSessionPollInterval(cfg.PollInterval),
		withGroupTransactSessionSuspendProcessingTimeout(cfg.SuspendProcessingTimeout),
		withGroupTransactSessionConsumeRegex(cfg.ConsumeRegex),
		withGroupTransactSessionConsumeTopics(splitCSV(cfg.Topics)...),
		withGroupTransactSessionGroup(cfg.Group),
		withGroupTransactSessionInstanceID(cfg.InstanceID),
		withGroupTransactSessionDisableFetchSessions(cfg.DisableFetchSessions),
		withGroupTransactSessionFetchMaxWait(cfg.FetchMaxWait),
		withGroupTransactSessionFetchMaxBytes(cfg.FetchMaxBytes),
		withGroupTransactSessionFetchMinBytes(cfg.FetchMinBytes),
		withGroupTransactSessionFetchMaxPartitionBytes(cfg.FetchMaxPartitionBytes),
		withGroupTransactSessionSessionTimeout(cfg.SessionTimeout),
		withGroupTransactSessionRebalanceTimeout(cfg.RebalanceTimeout),
		withGroupTransactSessionHeartbeatInterval(cfg.HeartbeatInterval),
		withGroupTransactSessionRack(cfg.Rack),
		withGroupTransactSessionMaxConcurrentFetches(cfg.MaxConcurrentFetches),
	}

	for _, opt := range opts {
		opt.applyGroupTransactSession(s)
	}
}

func withGroupTransactSessionEnabledFlag(v bool) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		s.enabled = v
	})
}

func withGroupTransactSessionSkipFatalErrorsFlag(v bool) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if v {
			s.skipFatalErrors = v
		}
	})
}

func withGroupTransactSessionBrokers(brokers ...string) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if len(brokers) > 0 {
			s.addClientOption(kgo.SeedBrokers(brokers...))
		}
	})
}

func withGroupTransactSessionSASL(cfg *Config) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if cfg.SASLMechanism == "" {
			return
		}

		mechanism, err := getSASLMechanism(cfg)
		if err != nil || mechanism == nil {
			return
		}

		s.addClientOption(kgo.SASL(mechanism))
	})
}

func withGroupTransactSessionRequestTimeoutOverhead(overhead time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if overhead > 0 {
			s.addClientOption(kgo.RequestTimeoutOverhead(overhead))
		}
	})
}

func withGroupTransactSessionConnIdleTimeout(timeout time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if timeout > 0 {
			s.addClientOption(kgo.ConnIdleTimeout(timeout))
		}
	})
}

func withGroupTransactSessionDialTimeout(timeout time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if timeout > 0 {
			s.addClientOption(kgo.DialTimeout(timeout))
		}
	})
}

func withGroupTransactSessionRequestRetries(n *int) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if n != nil {
			s.addClientOption(kgo.RequestRetries(*n))
		}
	})
}

func withGroupTransactSessionRetryTimeout(timeout time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if timeout > 0 {
			s.addClientOption(kgo.RetryTimeout(timeout))
		}
	})
}

func withGroupTransactSessionBrokerMaxWriteBytes(v *int32) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if v != nil {
			s.addClientOption(kgo.BrokerMaxWriteBytes(*v))
		}
	})
}

func withGroupTransactSessionBrokerMaxReadBytes(v *int32) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if v != nil {
			s.addClientOption(kgo.BrokerMaxReadBytes(*v))
		}
	})
}

func withGroupTransactSessionMetadataMaxAge(age time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if age > 0 {
			s.addClientOption(kgo.MetadataMaxAge(age))
		}
	})
}

func withGroupTransactSessionMetadataMinAge(age time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if age > 0 {
			s.addClientOption(kgo.MetadataMinAge(age))
		}
	})
}

func withGroupTransactSessionAlwaysRetryEOF(enabled bool) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if enabled {
			s.addClientOption(kgo.AlwaysRetryEOF())
		}
	})
}

func withGroupTransactSessionDefaultProduceTopic(topic string) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if topic != "" {
			s.addClientOption(kgo.DefaultProduceTopic(topic))
		}
	})
}

func withGroupTransactSessionProducerBatchMaxBytes(v int32) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if v != 0 {
			s.addClientOption(kgo.ProducerBatchMaxBytes(v))
		}
	})
}

func withGroupTransactSessionMaxBufferedRecords(n int) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if n != 0 {
			s.addClientOption(kgo.MaxBufferedRecords(n))
		}
	})
}

func withGroupTransactSessionMaxBufferedBytes(n int) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if n != 0 {
			s.addClientOption(kgo.MaxBufferedBytes(n))
		}
	})
}

func withGroupTransactSessionProduceRequestTimeout(timeout time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if timeout > 0 {
			s.addClientOption(kgo.ProduceRequestTimeout(timeout))
		}
	})
}

func withGroupTransactSessionRecordRetries(n int) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if n > 0 {
			s.addClientOption(kgo.RecordRetries(n))
		}
	})
}

func withGroupTransactSessionProducerLinger(linger *time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if linger != nil {
			s.addClientOption(kgo.ProducerLinger(*linger))
		}
	})
}

func withGroupTransactSessionRecordDeliveryTimeout(timeout time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if timeout > 0 {
			s.addClientOption(kgo.RecordDeliveryTimeout(timeout))
		}
	})
}

func withGroupTransactSessionTransactionalID(id string) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if id != "" {
			s.addClientOption(kgo.TransactionalID(id))
		}
	})
}

func withGroupTransactSessionTransactionTimeout(timeout time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if timeout > 0 {
			s.addClientOption(kgo.TransactionTimeout(timeout))
		}
	})
}

func withGroupTransactSessionMaxPollRecords(v int) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if v > 0 {
			s.batchSize = v
		}
	})
}

func withGroupTransactSessionPollInterval(timeout time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if timeout > 0 {
			s.pollInterval = timeout
		}
	})
}

func withGroupTransactSessionSuspendProcessingTimeout(timeout time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if timeout > 0 {
			s.suspendProcessingTimeout = timeout
		}
	})
}

func withGroupTransactSessionConsumeRegex(flag bool) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if flag {
			s.addClientOption(kgo.ConsumeRegex())
		}
	})
}

func withGroupTransactSessionConsumeTopics(topics ...string) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if len(topics) > 0 {
			s.addClientOption(kgo.ConsumeTopics(topics...))
		}
	})
}

func withGroupTransactSessionGroup(group string) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if group != "" {
			s.addClientOption(kgo.ConsumerGroup(group))
			s.addClientOption(kgo.DisableAutoCommit())
			s.setMetricLabel("consumer_group", group)
		}
	})
}

func withGroupTransactSessionInstanceID(v string) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if v != "" {
			s.addClientOption(kgo.InstanceID(v))
		}
	})
}

func withGroupTransactSessionDisableFetchSessions(flag bool) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if flag {
			s.addClientOption(kgo.DisableFetchSessions())
		}
	})
}

func withGroupTransactSessionFetchMaxWait(timeout time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if timeout > 0 {
			s.addClientOption(kgo.FetchMaxWait(timeout))
		}
	})
}

func withGroupTransactSessionFetchMaxBytes(v int32) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if v > 0 {
			s.addClientOption(kgo.FetchMaxBytes(v))
		}
	})
}

func withGroupTransactSessionFetchMinBytes(v int32) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if v > 0 {
			s.addClientOption(kgo.FetchMinBytes(v))
		}
	})
}

func withGroupTransactSessionFetchMaxPartitionBytes(v int32) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if v > 0 {
			s.addClientOption(kgo.FetchMaxPartitionBytes(v))
		}
	})
}

func withGroupTransactSessionSessionTimeout(timeout time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if timeout > 0 {
			s.addClientOption(kgo.SessionTimeout(timeout))
		}
	})
}

func withGroupTransactSessionRebalanceTimeout(timeout time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if timeout > 0 {
			s.addClientOption(kgo.RebalanceTimeout(timeout))
		}
	})
}

func withGroupTransactSessionHeartbeatInterval(interval time.Duration) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if interval > 0 {
			s.addClientOption(kgo.HeartbeatInterval(interval))
		}
	})
}

func withGroupTransactSessionRack(rack string) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if rack != "" {
			s.addClientOption(kgo.Rack(rack))
		}
	})
}

func withGroupTransactSessionMaxConcurrentFetches(n *int) GroupTransactSessionOption {
	return groupTransactSessionOptionFunc(func(s *GroupTransactSession) {
		if n != nil {
			s.addClientOption(kgo.MaxConcurrentFetches(*n))
		}
	})
}

////////////////////////////////////////////////////////
// Config helpers
////////////////////////////////////////////////////////

const (
	saslScramSHA512 = "SCRAM-SHA-512"
	saslScramSHA256 = "SCRAM-SHA-256"
	saslPlain       = "PLAIN"
)

func getSASLMechanism(cfg *Config) (sasl.Mechanism, error) {
	mechanism := strings.ToUpper(strings.TrimSpace(cfg.SASLMechanism))

	switch mechanism {
	case "":
		return nil, nil
	case saslPlain:
		auth := plain.Auth{
			User: cfg.User,
			Pass: cfg.Password,
		}

		return auth.AsMechanism(), nil
	case saslScramSHA256:
		auth := scram.Auth{
			User: cfg.User,
			Pass: cfg.Password,
		}

		return auth.AsSha256Mechanism(), nil
	case saslScramSHA512:
		auth := scram.Auth{
			User: cfg.User,
			Pass: cfg.Password,
		}

		return auth.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", cfg.SASLMechanism)
	}
}

func splitCSV(v string) []string {
	if v == "" {
		return nil
	}

	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}

	return out
}
