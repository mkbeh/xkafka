package kafka

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"

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

func WithProducerConfig(v *ProducerConfig) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if v == nil {
			return
		}

		opts := []ProducerOption{
			withProducerBrokers(strings.Split(v.Brokers, ",")...),
			withProducerSASL(v),
			withDefaultProduceTopic(v.DefaultProduceTopic),
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
			p.clientID = v
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

	// DefaultProduceTopic sets the default topic to produce to if the topic field
	// is empty in a Record.
	DefaultProduceTopic string `envconfig:"KAFKA_DEFAULT_PRODUCE_TOPIC"`
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

func withDefaultProduceTopic(v string) ProducerOption {
	return producerOptionFunc(func(p *Producer) {
		if v != "" {
			p.addClientOption(kgo.DefaultProduceTopic(v))
		}
	})
}
