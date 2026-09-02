module otel

go 1.27

require (
	github.com/mkbeh/xkafka v0.6.0
	github.com/mkbeh/xkafka/extra/otelxkafka v0.1.0
	github.com/twmb/franz-go v1.21.6
	github.com/twmb/franz-go/plugin/kotel v1.7.0
	go.opentelemetry.io/otel v1.46.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.46.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.46.0
	go.opentelemetry.io/otel/sdk v1.46.0
	go.opentelemetry.io/otel/sdk/metric v1.46.0
)
