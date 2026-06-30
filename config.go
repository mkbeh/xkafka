package xkafka

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

type Opt interface {
	apply(*client)
}

// ProducerOpt is a producer-specific option to configure a client.
//
// This is simply a namespaced Opt.
type ProducerOpt interface {
	Opt
	producerOpt()
}

// ConsumerOpt is a consumer-specific option to configure a client.
//
// This is simply a namespaced Opt.
type ConsumerOpt interface {
	Opt
	consumerOpt()
}

// GroupOpt is a consumer group-specific option to configure a client.
//
// This is simply a namespaced Opt.
type GroupOpt interface {
	Opt
	groupOpt()
}

type (
	clientOpt   struct{ fn func(*client) }
	producerOpt struct{ fn func(*client) }
	consumerOpt struct{ fn func(*client) }
	groupOpt    struct{ fn func(*client) }
)

func (opt clientOpt) apply(c *client)   { opt.fn(c) }
func (opt producerOpt) apply(c *client) { opt.fn(c) }
func (opt consumerOpt) apply(c *client) { opt.fn(c) }
func (opt groupOpt) apply(c *client)    { opt.fn(c) }

func (producerOpt) producerOpt() {}
func (consumerOpt) consumerOpt() {}
func (groupOpt) groupOpt()       {}

type Config struct {
	/////////////////////
	// GENERAL SECTION //
	/////////////////////

	// Brokers sets the seed brokers for the client to use.
	//
	// Any seed without a port uses the default Kafka port 9092.
	Brokers string `env:"KAFKA_BROKERS"`

	// SASLMechanism is the SASL mechanism used for authentication.
	// Supported values: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
	SASLMechanism string `env:"KAFKA_SASL_MECHANISM"`

	// User is the SASL username.
	User string `env:"KAFKA_USER"`

	// Password is the SASL password.
	Password string `env:"KAFKA_PASSWORD"`

	// RequestTimeoutOverhead adds extra time while setting request deadlines.
	RequestTimeoutOverhead time.Duration `env:"KAFKA_REQUEST_TIMEOUT_OVERHEAD"`

	// ConnIdleTimeout controls how long idle broker connections are kept open.
	ConnIdleTimeout time.Duration `env:"KAFKA_CONN_IDLE_TIMEOUT"`

	// DialTimeout sets the timeout for opening broker connections.
	DialTimeout time.Duration `env:"KAFKA_DIAL_TIMEOUT"`

	// RequestRetries sets how many times retryable requests may be retried.
	RequestRetries *int `env:"KAFKA_REQUEST_RETRIES"`

	// RetryTimeout limits the total time allowed for retrying a request.
	RetryTimeout time.Duration `env:"KAFKA_RETRY_TIMEOUT"`

	// BrokerMaxWriteBytes limits how many bytes may be written to a broker in one write.
	BrokerMaxWriteBytes *int32 `env:"KAFKA_MAX_WRITE_BYTES"`

	// BrokerMaxReadBytes limits the maximum response size read from a broker.
	BrokerMaxReadBytes *int32 `env:"KAFKA_MAX_READ_BYTES"`

	// MetadataMaxAge sets the maximum age of cached Kafka metadata.
	MetadataMaxAge time.Duration `env:"KAFKA_METADATA_MAX_AGE"`

	// MetadataMinAge sets the minimum time between metadata refreshes.
	MetadataMinAge time.Duration `env:"KAFKA_METADATA_MIN_AGE"`

	// AlwaysRetryEOF retries EOF errors instead of treating them as terminal connection failures.
	AlwaysRetryEOF bool `env:"KAFKA_ALWAYS_RETRY_EOF"`

	//////////////////////
	// PRODUCER SECTION //
	//////////////////////

	// DefaultProduceTopic sets the default topic for records without an explicit topic.
	DefaultProduceTopic string `env:"KAFKA_DEFAULT_PRODUCE_TOPIC"`

	// ProducerBatchMaxBytes limits the maximum size of a produced record batch.
	ProducerBatchMaxBytes int32 `env:"KAFKA_PRODUCER_BATCH_MAX_BYTES"`

	// MaxBufferedRecords limits how many records the producer may buffer.
	MaxBufferedRecords int `env:"KAFKA_MAX_BUFFERED_RECORDS"`

	// MaxBufferedBytes limits how many bytes the producer may buffer.
	MaxBufferedBytes int `env:"KAFKA_MAX_BUFFERED_BYTES"`

	// ProduceRequestTimeout limits how long brokers may take to respond to produce requests.
	ProduceRequestTimeout time.Duration `env:"KAFKA_PRODUCE_REQUEST_TIMEOUT"`

	// RecordRetries sets how many times producing records may be retried.
	RecordRetries int `env:"KAFKA_RECORD_RETRIES"`

	// ProducerLinger sets how long records may wait for batching before a produce request is built.
	ProducerLinger *time.Duration `env:"KAFKA_PRODUCER_LINGER"`

	// RecordDeliveryTimeout limits how long a record may remain buffered before timing out.
	RecordDeliveryTimeout time.Duration `env:"KAFKA_RECORD_DELIVERY_TIMEOUT"`

	// TransactionalID identifies the transactional producer.
	//
	// It is used by producer transactions and group transact sessions.
	TransactionalID string `env:"KAFKA_TRANSACTIONAL_ID"`

	// TransactionTimeout limits how long a Kafka transaction may remain open.
	TransactionTimeout time.Duration `env:"KAFKA_TRANSACTION_TIMEOUT"`

	//////////////////////
	// CONSUMER SECTION //
	//////////////////////

	// ConsumeRegex treats configured topics as regular expressions.
	ConsumeRegex bool `env:"KAFKA_CONSUME_REGEX"`

	// Topics contains Kafka topics consumed by the consumer or group transact session.
	Topics string `env:"KAFKA_TOPICS"`

	// MaxPollRecords limits how many records are handled in one poll iteration.
	MaxPollRecords int `env:"KAFKA_MAX_POLL_RECORDS"`

	// FetchMaxWait limits how long a broker may wait before returning a fetch response.
	FetchMaxWait time.Duration `env:"KAFKA_FETCH_MAX_WAIT"`

	// FetchMaxBytes limits the maximum bytes returned in one fetch response.
	FetchMaxBytes int32 `env:"KAFKA_FETCH_MAX_BYTES"`

	// FetchMinBytes sets the minimum bytes a broker tries to return in one fetch response.
	FetchMinBytes int32 `env:"KAFKA_FETCH_MIN_BYTES"`

	// FetchMaxPartitionBytes limits how many bytes may be fetched from one partition.
	FetchMaxPartitionBytes int32 `env:"KAFKA_FETCH_MAX_PARTITION_BYTES"`

	// DisableFetchSessions disables Kafka fetch sessions.
	DisableFetchSessions bool `env:"KAFKA_DISABLE_FETCH_SESSIONS"`

	// Rack identifies the client rack for rack-aware fetching and consumer assignment.
	Rack string `env:"KAFKA_RACK"`

	// MaxConcurrentFetches limits how many fetch requests may be in flight or buffered at once.
	//
	// It can be used together with FetchMaxBytes to bound consumer memory usage.
	MaxConcurrentFetches *int `env:"KAFKA_MAX_CONCURRENT_FETCHES"`

	////////////////////////////
	// CONSUMER GROUP SECTION //
	////////////////////////////

	// Group identifies the Kafka consumer group.
	Group string `env:"KAFKA_GROUP"`

	// InstanceID sets a static consumer group member ID.
	InstanceID string `env:"KAFKA_INSTANCE_ID"`

	// SessionTimeout controls how long a group member may go without heartbeating.
	SessionTimeout time.Duration `env:"KAFKA_SESSION_TIMEOUT"`

	// RebalanceTimeout controls how long group members may take during a rebalance.
	RebalanceTimeout time.Duration `env:"KAFKA_REBALANCE_TIMEOUT"`

	// HeartbeatInterval controls how often a group member sends heartbeats.
	HeartbeatInterval time.Duration `env:"KAFKA_HEARTBEAT_INTERVAL"`

	/////////////////////////
	// SHARE GROUP SECTION //
	/////////////////////////

	// ShareGroup identifies the Kafka share group.
	//
	// If set, share group consuming is used instead of a regular consumer group.
	ShareGroup string `env:"KAFKA_SHARE_GROUP"`

	// ShareMaxRecords limits how many records ShareFetch may return.
	//
	// If zero, the franz-go default is used.
	ShareMaxRecords int32 `env:"KAFKA_SHARE_MAX_RECORDS"`

	// ShareMaxRecordsStrict asks the broker to strictly cap records per ShareFetch.
	ShareMaxRecordsStrict bool `env:"KAFKA_SHARE_MAX_RECORDS_STRICT"`

	// ShareRejectAfterDeliveries controls when failed share group records are rejected.
	//
	// If zero, failed records are released for redelivery.
	ShareRejectAfterDeliveries int32 `env:"KAFKA_SHARE_REJECT_AFTER_DELIVERIES"`

	// ShareReleaseTimeout delays AckRelease after share handler errors.
	//
	// If zero, failed records are released immediately.
	ShareReleaseTimeout time.Duration `env:"KAFKA_SHARE_RELEASE_TIMEOUT"`

	/////////////////////
	// SERVICE SECTION //
	/////////////////////

	// Enabled enables this Kafka component.
	Enabled bool `env:"KAFKA_ENABLED"`

	// SkipFatalErrors allows the consumer to continue after non-retriable fetch errors.
	SkipFatalErrors bool `env:"KAFKA_SKIP_FATAL_ERRORS"`

	// PollInterval sets the interval between consumer poll iterations.
	PollInterval time.Duration `env:"KAFKA_POLL_INTERVAL"`

	// SuspendProcessingTimeout sets the wait time after handler errors.
	SuspendProcessingTimeout time.Duration `env:"KAFKA_SUSPEND_PROCESSING_TIMEOUT"`

	// SuspendCommittingTimeout sets the wait time after offset commit or ack errors.
	SuspendCommittingTimeout time.Duration `env:"KAFKA_SUSPEND_COMMITTING_TIMEOUT"`
}

////////////////////////////////////////////////////////
// Client options
////////////////////////////////////////////////////////

func WithConfig(config *Config) Opt {
	return clientOpt{fn: func(c *client) {
		if config == nil {
			return
		}

		opts := []Opt{
			withBrokers(config.Brokers),
			withSASL(config),

			withRequestTimeoutOverhead(config.RequestTimeoutOverhead),
			withConnIdleTimeout(config.ConnIdleTimeout),
			withDialTimeout(config.DialTimeout),
			withRequestRetries(config.RequestRetries),
			withRetryTimeout(config.RetryTimeout),
			withBrokerMaxWriteBytes(config.BrokerMaxWriteBytes),
			withBrokerMaxReadBytes(config.BrokerMaxReadBytes),
			withMetadataMaxAge(config.MetadataMaxAge),
			withMetadataMinAge(config.MetadataMinAge),
			withAlwaysRetryEOF(config.AlwaysRetryEOF),

			withDefaultProduceTopic(config.DefaultProduceTopic),
			withProducerBatchMaxBytes(config.ProducerBatchMaxBytes),
			withMaxBufferedRecords(config.MaxBufferedRecords),
			withMaxBufferedBytes(config.MaxBufferedBytes),
			withProduceRequestTimeout(config.ProduceRequestTimeout),
			withRecordRetries(config.RecordRetries),
			withProducerLinger(config.ProducerLinger),
			withRecordDeliveryTimeout(config.RecordDeliveryTimeout),
			withTransactionalID(config.TransactionalID),
			withTransactionTimeout(config.TransactionTimeout),

			withConsumeRegex(config.ConsumeRegex),
			withConsumeTopics(splitCSV(config.Topics)...),
			withMaxPollRecords(config.MaxPollRecords),
			withFetchMaxWait(config.FetchMaxWait),
			withFetchMaxBytes(config.FetchMaxBytes),
			withFetchMinBytes(config.FetchMinBytes),
			withFetchMaxPartitionBytes(config.FetchMaxPartitionBytes),
			withDisableFetchSessions(config.DisableFetchSessions),
			withRack(config.Rack),
			withMaxConcurrentFetches(config.MaxConcurrentFetches),

			withConsumerGroup(config.Group),
			withInstanceID(config.InstanceID),
			withSessionTimeout(config.SessionTimeout),
			withRebalanceTimeout(config.RebalanceTimeout),
			withHeartbeatInterval(config.HeartbeatInterval),

			withShareGroup(config.ShareGroup),
			withShareMaxRecords(config.ShareMaxRecords),
			withShareMaxRecordsStrict(config.ShareMaxRecordsStrict),
			withShareRejectAfterDeliveries(config.ShareRejectAfterDeliveries),
			withShareReleaseTimeout(config.ShareReleaseTimeout),

			withEnabled(config.Enabled),
			withSkipFatalErrors(config.SkipFatalErrors),
			withPollInterval(config.PollInterval),
			withSuspendProcessingTimeout(config.SuspendProcessingTimeout),
			withSuspendCommittingTimeout(config.SuspendCommittingTimeout),
		}

		for _, opt := range opts {
			opt.apply(c)
		}
	}}
}

func WithLogger(logger *slog.Logger) Opt {
	return clientOpt{fn: func(c *client) {
		if logger != nil {
			c.logger = logger
		}
	}}
}

func WithClientID(s string) Opt {
	return clientOpt{fn: func(c *client) {
		if s != "" {
			c.clientID = s
		}
	}}
}

func WithTLS(tlsConfig *tls.Config) Opt {
	return clientOpt{fn: func(c *client) {
		if tlsConfig != nil {
			c.clientOps = append(c.clientOps, kgo.DialTLSConfig(tlsConfig))
		}
	}}
}

func WithMeterProvider(provider metric.MeterProvider) Opt {
	return clientOpt{fn: func(c *client) {
		if provider != nil {
			c.meterOpts = append(c.meterOpts, kotel.MeterProvider(provider))
		}
	}}
}

func WithMetricsNamespace(namespace string) Opt {
	return clientOpt{fn: func(c *client) {
		if namespace != "" {
			c.namespace = namespace
		}
	}}
}

func WithMetricLabel(key, value string) Opt {
	return clientOpt{fn: func(c *client) {
		if key == "" {
			return
		}

		c.setMetricLabel(key, value)
	}}
}

func WithTracerProvider(provider trace.TracerProvider) Opt {
	return clientOpt{fn: func(c *client) {
		if provider != nil {
			c.tracerOpts = append(c.tracerOpts, kotel.TracerProvider(provider))
		}
	}}
}

func WithTracerPropagator(propagator propagation.TextMapPropagator) Opt {
	return clientOpt{fn: func(c *client) {
		if propagator != nil {
			c.tracerOpts = append(c.tracerOpts, kotel.TracerPropagator(propagator))
		}
	}}
}

func withBrokers(brokers string) Opt {
	return clientOpt{fn: func(c *client) {
		seeds := splitCSV(brokers)
		if len(seeds) == 0 {
			return
		}

		c.clientOps = append(c.clientOps, kgo.SeedBrokers(seeds...))
	}}
}

func withSASL(config *Config) Opt {
	return clientOpt{fn: func(c *client) {
		mechanism, err := getSASLMechanism(config)
		if err != nil || mechanism == nil {
			return
		}

		c.clientOps = append(c.clientOps, kgo.SASL(mechanism))
	}}
}

func withRequestTimeoutOverhead(overhead time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if overhead > 0 {
			c.clientOps = append(c.clientOps, kgo.RequestTimeoutOverhead(overhead))
		}
	}}
}

func withConnIdleTimeout(timeout time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if timeout > 0 {
			c.clientOps = append(c.clientOps, kgo.ConnIdleTimeout(timeout))
		}
	}}
}

func withDialTimeout(timeout time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if timeout > 0 {
			c.clientOps = append(c.clientOps, kgo.DialTimeout(timeout))
		}
	}}
}

func withRequestRetries(n *int) Opt {
	return clientOpt{fn: func(c *client) {
		if n != nil {
			c.clientOps = append(c.clientOps, kgo.RequestRetries(*n))
		}
	}}
}

func withRetryTimeout(timeout time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if timeout > 0 {
			c.clientOps = append(c.clientOps, kgo.RetryTimeout(timeout))
		}
	}}
}

func withBrokerMaxWriteBytes(v *int32) Opt {
	return clientOpt{fn: func(c *client) {
		if v != nil {
			c.clientOps = append(c.clientOps, kgo.BrokerMaxWriteBytes(*v))
		}
	}}
}

func withBrokerMaxReadBytes(v *int32) Opt {
	return clientOpt{fn: func(c *client) {
		if v != nil {
			c.clientOps = append(c.clientOps, kgo.BrokerMaxReadBytes(*v))
		}
	}}
}

func withMetadataMaxAge(age time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if age > 0 {
			c.clientOps = append(c.clientOps, kgo.MetadataMaxAge(age))
		}
	}}
}

func withMetadataMinAge(age time.Duration) Opt {
	return clientOpt{fn: func(c *client) {
		if age > 0 {
			c.clientOps = append(c.clientOps, kgo.MetadataMinAge(age))
		}
	}}
}

func withAlwaysRetryEOF(enabled bool) Opt {
	return clientOpt{fn: func(c *client) {
		if enabled {
			c.clientOps = append(c.clientOps, kgo.AlwaysRetryEOF())
		}
	}}
}

////////////////////////////////////////////////////////
// Producer options
////////////////////////////////////////////////////////

func WithRecordPartitioner(partitioner kgo.Partitioner) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if partitioner != nil {
			c.clientOps = append(c.clientOps, kgo.RecordPartitioner(partitioner))
		}
	}}
}

func WithRequiredAcks(acks kgo.Acks) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		c.clientOps = append(c.clientOps, kgo.RequiredAcks(acks))
	}}
}

func WithProducerBatchCompression(preference ...kgo.CompressionCodec) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if len(preference) > 0 {
			c.clientOps = append(c.clientOps, kgo.ProducerBatchCompression(preference...))
		}
	}}
}

func WithAllowIdempotentProduceCancellation() ProducerOpt {
	return producerOpt{fn: func(c *client) {
		c.clientOps = append(c.clientOps, kgo.AllowIdempotentProduceCancellation())
	}}
}

func WithProducerBatchMaxBytesFn(fn func(topic string) int32) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if fn != nil {
			c.clientOps = append(c.clientOps, kgo.ProducerBatchMaxBytesFn(fn))
		}
	}}
}

func WithProducePromise(promiseFunc PromiseFunc) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if promiseFunc != nil {
			c.promiseFunc = promiseFunc
		}
	}}
}

func withDefaultProduceTopic(topic string) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if topic == "" {
			return
		}

		c.clientOps = append(c.clientOps, kgo.DefaultProduceTopic(topic))
	}}
}

func withProducerBatchMaxBytes(v int32) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if v != 0 {
			c.clientOps = append(c.clientOps, kgo.ProducerBatchMaxBytes(v))
		}
	}}
}

func withMaxBufferedRecords(n int) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if n != 0 {
			c.clientOps = append(c.clientOps, kgo.MaxBufferedRecords(n))
		}
	}}
}

func withMaxBufferedBytes(n int) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if n != 0 {
			c.clientOps = append(c.clientOps, kgo.MaxBufferedBytes(n))
		}
	}}
}

func withProduceRequestTimeout(timeout time.Duration) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if timeout > 0 {
			c.clientOps = append(c.clientOps, kgo.ProduceRequestTimeout(timeout))
		}
	}}
}

func withRecordRetries(n int) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if n != 0 {
			c.clientOps = append(c.clientOps, kgo.RecordRetries(n))
		}
	}}
}

func withProducerLinger(linger *time.Duration) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if linger != nil {
			c.clientOps = append(c.clientOps, kgo.ProducerLinger(*linger))
		}
	}}
}

func withRecordDeliveryTimeout(timeout time.Duration) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if timeout > 0 {
			c.clientOps = append(c.clientOps, kgo.RecordDeliveryTimeout(timeout))
		}
	}}
}

func withTransactionalID(id string) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if id == "" {
			return
		}

		c.clientOps = append(c.clientOps, kgo.TransactionalID(id))
	}}
}

func withTransactionTimeout(timeout time.Duration) ProducerOpt {
	return producerOpt{fn: func(c *client) {
		if timeout > 0 {
			c.clientOps = append(c.clientOps, kgo.TransactionTimeout(timeout))
		}
	}}
}

////////////////////////////////////////////////////////
// Consumer options
////////////////////////////////////////////////////////

func WithConsumerBatchHandler(handler BatchHandlerFunc) ConsumerOpt {
	return consumerOpt{fn: func(cl *client) {
		if handler != nil {
			cl.clientHandleFetches = func(c *Client) handleFetchesFunc {
				return c.handleFetchesBatch(handler)
			}
		}
	}}
}

func WithConsumerShareBatchHandler(handler BatchHandlerFunc) ConsumerOpt {
	return consumerOpt{fn: func(cl *client) {
		if handler != nil {
			cl.clientHandleFetches = func(c *Client) handleFetchesFunc {
				return c.handleShareFetchesBatch(handler)
			}
		}
	}}
}

func WithConsumeResetOffset(offset kgo.Offset) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		c.clientOps = append(c.clientOps, kgo.ConsumeResetOffset(offset))
	}}
}

func WithFetchIsolationLevel(level kgo.IsolationLevel) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		c.clientOps = append(c.clientOps, kgo.FetchIsolationLevel(level))
	}}
}

func WithShareAckCallback(fn func(*kgo.Client, kgo.ShareAckResults)) GroupOpt {
	return groupOpt{fn: func(c *client) {
		if fn != nil {
			c.clientOps = append(c.clientOps, kgo.ShareAckCallback(fn))
		}
	}}
}

func withConsumeRegex(enabled bool) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if enabled {
			c.clientOps = append(c.clientOps, kgo.ConsumeRegex())
		}
	}}
}

func withConsumeTopics(topics ...string) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if len(topics) == 0 {
			return
		}

		c.clientOps = append(c.clientOps, kgo.ConsumeTopics(topics...))
	}}
}

func withMaxPollRecords(maxPollRecords int) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if maxPollRecords <= 0 {
			return
		}

		c.batchSize = maxPollRecords
	}}
}

func withFetchMaxWait(wait time.Duration) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if wait > 0 {
			c.clientOps = append(c.clientOps, kgo.FetchMaxWait(wait))
		}
	}}
}

func withFetchMaxBytes(bytes int32) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if bytes != 0 {
			c.clientOps = append(c.clientOps, kgo.FetchMaxBytes(bytes))
		}
	}}
}

func withFetchMinBytes(bytes int32) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if bytes != 0 {
			c.clientOps = append(c.clientOps, kgo.FetchMinBytes(bytes))
		}
	}}
}

func withFetchMaxPartitionBytes(bytes int32) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if bytes != 0 {
			c.clientOps = append(c.clientOps, kgo.FetchMaxPartitionBytes(bytes))
		}
	}}
}

func withDisableFetchSessions(disable bool) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if disable {
			c.clientOps = append(c.clientOps, kgo.DisableFetchSessions())
		}
	}}
}

func withRack(rack string) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if rack != "" {
			c.clientOps = append(c.clientOps, kgo.Rack(rack))
		}
	}}
}

func withMaxConcurrentFetches(n *int) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if n != nil {
			c.clientOps = append(c.clientOps, kgo.MaxConcurrentFetches(*n))
		}
	}}
}

////////////////////////////////////////////////////////
// Consumer group options
////////////////////////////////////////////////////////

func WithBalancers(balancers ...kgo.GroupBalancer) GroupOpt {
	return groupOpt{fn: func(c *client) {
		if len(balancers) > 0 {
			c.clientOps = append(c.clientOps, kgo.Balancers(balancers...))
		}
	}}
}

func withConsumerGroup(group string) GroupOpt {
	return groupOpt{fn: func(c *client) {
		if group == "" {
			return
		}

		c.groupSpecified = true
		c.clientOps = append(c.clientOps, kgo.ConsumerGroup(group), kgo.DisableAutoCommit())
		c.setMetricLabel("consumer_group", group)
	}}
}

func withInstanceID(id string) GroupOpt {
	return groupOpt{fn: func(c *client) {
		if id != "" {
			c.clientOps = append(c.clientOps, kgo.InstanceID(id))
		}
	}}
}

func withSessionTimeout(timeout time.Duration) GroupOpt {
	return groupOpt{fn: func(c *client) {
		if timeout > 0 {
			c.clientOps = append(c.clientOps, kgo.SessionTimeout(timeout))
		}
	}}
}

func withRebalanceTimeout(timeout time.Duration) GroupOpt {
	return groupOpt{fn: func(c *client) {
		if timeout > 0 {
			c.clientOps = append(c.clientOps, kgo.RebalanceTimeout(timeout))
		}
	}}
}

func withHeartbeatInterval(interval time.Duration) GroupOpt {
	return groupOpt{fn: func(c *client) {
		if interval > 0 {
			c.clientOps = append(c.clientOps, kgo.HeartbeatInterval(interval))
		}
	}}
}

////////////////////////////////////////////////////////
// Share group options
////////////////////////////////////////////////////////

func withShareGroup(group string) GroupOpt {
	return groupOpt{fn: func(c *client) {
		if group == "" {
			return
		}

		c.clientOps = append(c.clientOps, kgo.ShareGroup(group))
		c.setMetricLabel("consumer_group", group)
	}}
}

func withShareMaxRecords(n int32) GroupOpt {
	return groupOpt{fn: func(c *client) {
		if n > 0 {
			c.clientOps = append(c.clientOps, kgo.ShareMaxRecords(n))
		}
	}}
}

func withShareMaxRecordsStrict(strict bool) GroupOpt {
	return groupOpt{fn: func(c *client) {
		if strict {
			c.clientOps = append(c.clientOps, kgo.ShareMaxRecordsStrict())
		}
	}}
}

func withShareRejectAfterDeliveries(n int32) GroupOpt {
	return groupOpt{fn: func(c *client) {
		if n > 0 {
			c.shareRejectAfterDeliveries = n
		}
	}}
}

func withShareReleaseTimeout(timeout time.Duration) GroupOpt {
	return groupOpt{fn: func(c *client) {
		if timeout > 0 {
			c.shareReleaseTimeout = timeout
		}
	}}
}

////////////////////////////////////////////////////////
// Group transact session options
////////////////////////////////////////////////////////

func WithGroupTransactSessionBatchHandler(handler BatchTxHandlerFunc) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if handler != nil {
			c.groupHandleFetches = func(s *GroupTransactSession) handleFetchesFunc {
				return s.handleFetchesBatch(handler)
			}
		}
	}}
}

////////////////////////////////////////////////////////
// Consumer runtime options
////////////////////////////////////////////////////////

func withEnabled(enabled bool) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		c.enabled = enabled
	}}
}

func withSkipFatalErrors(skip bool) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		c.skipFatalErrors = skip
	}}
}

func withPollInterval(interval time.Duration) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if interval == 0 {
			return
		}

		c.pollInterval = interval
	}}
}

func withSuspendProcessingTimeout(timeout time.Duration) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if timeout == 0 {
			return
		}

		c.suspendProcessingTimeout = timeout
	}}
}

func withSuspendCommittingTimeout(timeout time.Duration) ConsumerOpt {
	return consumerOpt{fn: func(c *client) {
		if timeout == 0 {
			return
		}

		c.suspendCommittingTimeout = timeout
	}}
}

////////////////////////////////////////////////////////
// Helpers
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
