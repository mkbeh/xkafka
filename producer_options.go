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

type ProducerOption interface {
	apply(c *Producer)
}

type producerOptionFunc func(c *Producer)

func (f producerOptionFunc) apply(c *Producer) {
	f(c)
}

func WithProducerConfig(cfg *ProducerConfig) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if cfg == nil {
			return
		}

		opts := []ProducerOption{
			withProducerBrokers(strings.Split(cfg.Brokers, ",")...),
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
			opt.apply(p)
		}
	})
}

func WithProducerClientID(v string) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if v != "" {
			p.addClientOptions(kgo.ClientID(v))
			p.addTracerOption(kotel.ClientID(v))
			p.id = v
		}
	})
}

func WithProducerLogger(v *slog.Logger) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		p.logger = v
	})
}

func WithProducerPartitioner(partitioner kgo.Partitioner) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		p.addClientOption(kgo.RecordPartitioner(partitioner))
	})
}

// WithProducerTLS opts into dialing brokers with the given TLS config with a
// 10s dial timeout.
//
// Every dial, the input config is cloned. If the config's ServerName is not
// specified, this function uses net.SplitHostPort to extract the host from the
// broker being dialed and sets the ServerName. In short, it is not necessary
// to set the ServerName.
func WithProducerTLS(tls *tls.Config) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		p.addClientOption(kgo.DialTLSConfig(tls))
	})
}

// WithProducerCB set callback func which calls after attempt to send message in topic
func WithProducerCB(cb func(record *kgo.Record, err error)) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		p.promiseFunc = cb
	})
}

func WithProducerRequiredAcks(acks kgo.Acks) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		p.addClientOption(kgo.RequiredAcks(acks))
	})
}

func WithProducerBatchCompression(preference ...kgo.CompressionCodec) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		p.addClientOption(kgo.ProducerBatchCompression(preference...))
	})
}

// --- metrics ---

func WithProducerMeterProvider(provider metric.MeterProvider) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		p.addMeterOption(kotel.MeterProvider(provider))
	})
}

// --- tracing ---

func WithProducerTracerProvider(provider trace.TracerProvider) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		p.addTracerOption(kotel.TracerProvider(provider))
	})
}

func WithProducerTracerPropagator(propagator propagation.TextMapPropagator) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		p.addTracerOption(kotel.TracerPropagator(propagator))
	})
}

type ProducerConfig struct {
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

	// RequestTimeoutOverhead uses the given time as overhead while deadlining
	// requests.
	RequestTimeoutOverhead time.Duration `envconfig:"REQUEST_TIMEOUT_OVERHEAD"`
	// ConnIdleTimeout is a rough amount of time to allow connections to idle
	// before they are closed.
	ConnIdleTimeout time.Duration `envconfig:"CONN_IDLE_TIMEOUT"`
	// DialTimeout sets the dial timeout.
	DialTimeout time.Duration `envconfig:"DIAL_TIMEOUT"`
	// RequestRetries sets the number of tries that retryable requests are allowed.
	RequestRetries *int `envconfig:"REQUEST_RETRIES"`
	// RetryTimeout sets the upper limit on how long we allow a request to be
	// issued and then reissued on failure. That is, this control the total
	// end-to-end maximum time we allow for trying a request.
	RetryTimeout time.Duration `envconfig:"RETRY_TIMEOUT"`
	// BrokerMaxWriteBytes upper bounds the number of bytes written to a broker
	// connection in a single write.
	BrokerMaxWriteBytes *int32 `envconfig:"KAFKA_MAX_WRITE_BYTES"`
	// BrokerMaxReadBytes sets the maximum response size that can be read from
	// Kafka.
	BrokerMaxReadBytes *int32 `envconfig:"KAFKA_MAX_READ_BYTES"`
	// MetadataMaxAge sets the maximum age for the client's cached metadata.
	MetadataMaxAge time.Duration `envconfig:"METADATA_MAX_AGE"`
	// MetadataMinAge sets the minimum time between metadata queries.
	MetadataMinAge time.Duration `envconfig:"METADATA_MIN_AGE"`

	// DefaultProduceTopic sets the default topic to produce to if the topic field
	// is empty in a Record.
	DefaultProduceTopic string `envconfig:"KAFKA_DEFAULT_PRODUCE_TOPIC"`
	// ProducerBatchMaxBytes upper bounds the size of a record batch.
	ProducerBatchMaxBytes int32 `envconfig:"KAFKA_PRODUCER_BATCH_MAX_BYTES"`
	// MaxBufferedRecords sets the max amount of records the client will buffer,
	// blocking produces until records are finished if this limit is reached.
	MaxBufferedRecords int `envconfig:"KAFKA_MAX_BUFFERED_RECORDS"`
	// MaxBufferedBytes sets the max amount of bytes that the client will buffer
	// while producing, blocking produces until records are finished if this limit
	// is reached.
	MaxBufferedBytes int `envconfig:"KAFKA_MAX_BUFFERED_BYTES"`
	// ProduceRequestTimeout sets how long Kafka broker's are allowed to respond to
	// produce requests, overriding the default 10s. If a broker exceeds this
	// duration, it will reply with a request timeout error.
	ProduceRequestTimeout time.Duration `envconfig:"KAFKA_PRODUCE_REQUEST_TIMEOUT"`
	// RecordRetries sets the number of tries for producing records.
	RecordRetries int `envconfig:"KAFKA_RECORD_RETRIES"`
	// ProducerLinger sets how long individual topic partitions will linger waiting
	// for more records before triggering a request to be built.
	ProducerLinger time.Duration `envconfig:"KAFKA_PRODUCER_LINGER"`
	// RecordDeliveryTimeout sets a rough time of how long a record can sit around
	// in a batch before timing out.
	RecordDeliveryTimeout time.Duration `envconfig:"KAFKA_RECORD_DELIVERY_TIMEOUT"`
	// TransactionalID sets a transactional ID for the client, ensuring that
	// records are produced transactionally under this ID (exactly once semantics).
	TransactionalID string `envconfig:"KAFKA_TRANSACTIONAL_ID"`
	// TransactionTimeout sets the allowed for a transaction.
	TransactionTimeout time.Duration `envconfig:"KAFKA_TRANSACTION_TIMEOUT"`
}

func (cfg *ProducerConfig) getLogin() string {
	return cfg.User
}

func (cfg *ProducerConfig) getPassword() string {
	return cfg.Password
}

func (cfg *ProducerConfig) getSASLMechanism() string {
	return cfg.SASLMechanism
}

const (
	saslScramSHA512 = "SCRAM-SHA-512"
	saslScramSHA256 = "SCRAM-SHA-256"
	saslPlain       = "PLAIN"
)

func getSASLMechanism(cfg interface {
	getLogin() string
	getPassword() string
	getSASLMechanism() string
},
) (sasl.Mechanism, error) {
	switch cfg.getSASLMechanism() {
	case saslPlain:
		auth := plain.Auth{
			User: cfg.getLogin(),
			Pass: cfg.getPassword(),
		}

		return auth.AsMechanism(), nil
	case saslScramSHA256:
		auth := scram.Auth{
			User: cfg.getLogin(),
			Pass: cfg.getPassword(),
		}

		return auth.AsSha256Mechanism(), nil
	case saslScramSHA512:
		auth := scram.Auth{
			User: cfg.getLogin(),
			Pass: cfg.getPassword(),
		}

		return auth.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported mechanism: %s", cfg.getSASLMechanism())
	}
}

func withProducerBrokers(brokers ...string) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if len(brokers) > 0 {
			p.addClientOption(kgo.SeedBrokers(brokers...))
		}
	})
}

func withProducerSASL(cfg *ProducerConfig) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if cfg.SASLMechanism == "" {
			return
		}

		mechanism, err := getSASLMechanism(cfg)
		if err != nil {
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

func withDefaultProduceTopic(t string) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if t != "" {
			p.addClientOption(kgo.DefaultProduceTopic(t))
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

func withProducerLinger(linger time.Duration) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if linger > 0 {
			p.addClientOption(kgo.ProducerLinger(linger))
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
