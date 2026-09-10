# OpenTelemetry for xkafka

`otelxkafka` provides optional OpenTelemetry metrics and tracing for `xkafka` runtime behavior through the xkafka hook API.

It is designed to coexist with `franz-go/plugin/kotel.Meter`: native `kotel` metrics cover franz-go client and broker operations, while `otelxkafka` covers xkafka runtime metrics and batch-oriented tracing. `otelxkafka.Tracer` handles trace propagation itself and should not be combined with `franz-go/plugin/kotel.Tracer`, which adds per-record publish and receive spans.

Applications own the OpenTelemetry SDK lifecycle and exporter configuration.

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
    "go.opentelemetry.io/otel/propagation"
    sdkmetric "go.opentelemetry.io/otel/sdk/metric"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

meterProvider := sdkmetric.NewMeterProvider()
defer meterProvider.Shutdown(context.Background())

tracerProvider := sdktrace.NewTracerProvider()
defer tracerProvider.Shutdown(context.Background())

propagator := propagation.NewCompositeTextMapPropagator(
    propagation.TraceContext{},
    propagation.Baggage{},
)

telemetry := otelxkafka.NewKotel(
    otelxkafka.WithMeter(
        otelxkafka.NewMeter(
            otelxkafka.MeterProvider(meterProvider),
        ),
    ),
    otelxkafka.WithTracer(
        otelxkafka.NewTracer(
            otelxkafka.TracerProvider(tracerProvider),
            otelxkafka.TracerPropagator(propagator),
        ),
    ),
)

client, err := xkafka.NewClient(
    xkafka.WithName("orders"),
    xkafka.WithLabel("service", "orders-api"),
    xkafka.WithHooks(telemetry.Hooks()...),
)
if err != nil {
    return err
}
defer client.Shutdown(context.Background())
```
<!-- @formatter:on -->

`Kotel` can be reused across multiple clients and group transaction sessions. Every call to `Hooks` returns runtime-scoped `Meter` and `Tracer` hook instances, so client metadata remains isolated.

`WithName` is exported as `messaging.client.id`. Custom labels are exported as OpenTelemetry attributes except for reserved attributes owned by the instrumentation. Keep labels stable and low-cardinality.

## Metrics

`Meter` exports xkafka runtime metrics such as:

```text
xkafka.produce.errors
xkafka.fetch.errors
messaging.process.duration
xkafka.handler.records
messaging.client.operation.duration
xkafka.share.ack.records
xkafka.transaction.duration
```

Native franz-go client metrics are intentionally left to `franz-go/plugin/kotel.Meter` to avoid duplicating broker, byte, and record telemetry.

## Tracing

`Tracer` creates one producer `send` span for each `ProduceSync` operation and propagates that span context through every record in the batch. The span includes standard messaging attributes such as the client ID, operation, batch message count, and destination when the whole batch targets one topic.

`Produce` and `TryProduce` remain propagation-only so asynchronous record production does not create one producer span per record.

Consumer handler spans link to unique sampled message creation contexts extracted from the batch. This keeps tracing batch-oriented and avoids creating one publish and one receive span for every record. `Tracer` also traces offset commit and Share Group acknowledgement settlement, and transaction attempts.
