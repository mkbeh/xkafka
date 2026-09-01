package xkafka

// MetricsSource provides client metadata and statistics required by metrics
// integrations.
type MetricsSource interface {
	Name() string
	Labels() map[string]string
	Stats() Stats
}

// Metrics registers metrics for a Kafka client or group transaction session.
//
// Implementations must be safe to reuse across multiple clients and sessions.
type Metrics interface {
	Register(source MetricsSource) (MetricsRegistration, error)
}

// MetricsRegistration represents a metrics registration for one client or session.
//
// Close is called once before the underlying franz-go client or session is closed.
type MetricsRegistration interface {
	Close()
}

var (
	_ MetricsSource = (*Client)(nil)
	_ MetricsSource = (*GroupTransactSession)(nil)
)
