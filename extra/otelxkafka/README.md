# OpenTelemetry Metrics for xkafka

`otelxkafka` provides optional OpenTelemetry metrics integration for `xkafka`.

The package is exporter-agnostic: applications own the OpenTelemetry SDK lifecycle and exporter configuration, while
`otelxkafka` reads lightweight `xkafka.Stats` snapshots and exposes them through OpenTelemetry observable metrics.

## Installation

```bash
go get github.com/mkbeh/xkafka/extra/otelxkafka
```

## Usage

<!-- @formatter:off -->
```go
import (
	"context"

	"github.com/mkbeh/xkafka"
	"github.com/mkbeh/xkafka/extra/otelxkafka"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

meterProvider := sdkmetric.NewMeterProvider()
defer meterProvider.Shutdown(context.Background())

// Create one reusable OpenTelemetry metrics integration.
metrics, err := otelxkafka.New(
	otelxkafka.WithMeterProvider(meterProvider),
)
if err != nil {
	return err
}

orders, err := xkafka.NewClient(
	xkafka.WithName("orders"),
	xkafka.WithLabel("service", "orders-api"),
	xkafka.WithMetrics(metrics),
)
if err != nil {
	return err
}
defer orders.Shutdown(context.Background())

billing, err := xkafka.NewClient(
	xkafka.WithName("billing"),
	xkafka.WithLabel("service", "billing-api"),
	xkafka.WithMetrics(metrics),
)
if err != nil {
	return err
}
defer billing.Shutdown(context.Background())
```
<!-- @formatter:on -->

`Metrics` is safe to reuse across multiple clients and group transaction sessions. Each source is registered during
creation and unregistered automatically during `Shutdown`.

Names and labels are exported as metric attributes and should remain stable and low-cardinality.
