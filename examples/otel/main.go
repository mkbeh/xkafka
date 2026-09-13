package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mkbeh/xkafka"
	"github.com/mkbeh/xkafka/extra/otelxkafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

const (
	clientName = "otel"
	brokers    = "localhost:29092"
	group      = "sample-otel-group"
	topicA     = "sample-otel-topic-a"
	topicB     = "sample-otel-topic-b"
	httpAddr   = "localhost:8080"

	serviceName           = "xkafka-otel-example"
	tracesEndpoint        = "http://localhost:4318/v1/traces"
	mixedBatchRecordCount = 10
)

type message struct {
	ID   int    `json:"id"`
	Text string `json:"text"`
}

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	res, err := newResource()
	if err != nil {
		return fmt.Errorf("create resource: %w", err)
	}

	registry := prometheus.NewRegistry()

	meterProvider, err := newMeterProvider(registry, res)
	if err != nil {
		return fmt.Errorf("create meter provider: %w", err)
	}
	defer shutdownMeterProvider(meterProvider)

	tracerProvider, err := newTracerProvider(ctx, res)
	if err != nil {
		return fmt.Errorf("create tracer provider: %w", err)
	}
	defer shutdownTracerProvider(tracerProvider)

	// Collect native franz-go client metrics.
	kafkaMeter := kotel.NewMeter(
		kotel.MeterProvider(meterProvider),
	)
	kafkaTelemetry := kotel.NewKotel(
		kotel.WithMeter(kafkaMeter),
	)

	// Collect xkafka runtime metrics and tracing.
	xkafkaMeter := otelxkafka.NewMeter(
		otelxkafka.MeterProvider(meterProvider),
		otelxkafka.ClientID(clientName),
		otelxkafka.ConsumerGroup(group),
	)
	xkafkaTracer := otelxkafka.NewTracer(
		otelxkafka.TracerProvider(tracerProvider),
		otelxkafka.TracerPropagator(propagation.TraceContext{}),
		otelxkafka.ClientID(clientName),
		otelxkafka.ConsumerGroup(group),
	)
	xkafkaTelemetry := otelxkafka.NewKotel(
		otelxkafka.WithMeter(xkafkaMeter),
		otelxkafka.WithTracer(xkafkaTracer),
	)

	client, err := xkafka.NewClient(
		xkafka.WithName(clientName),
		xkafka.WithLogger(
			kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil),
		),
		xkafka.WithHooks(xkafkaTelemetry.Hooks()...),
		xkafka.WithKafkaOptions(
			kgo.SeedBrokers(brokers),
			kgo.ConsumeTopics(topicA, topicB),
			kgo.ConsumerGroup(group),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.WithHooks(kafkaTelemetry.Hooks()...),
		),
		xkafka.WithMaxRetries(1),
		xkafka.WithSuspendProcessingTimeout(time.Second),
		xkafka.WithBatchHandler(newBatchHandler()),
	)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer shutdownClient(client)

	if err := pingClient(ctx, client); err != nil {
		return err
	}

	errCh := make(chan error, 2)
	go func() {
		if err := client.HandleFetches(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("handle kafka fetches: %w", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /produce", produceHandler(client))
	mux.HandleFunc("POST /produce-error", produceErrorHandler(client))
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	server := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go serveHTTP(server, errCh)

	log.Printf("HTTP server listening on http://%s", httpAddr)
	log.Printf("Prometheus metrics available at http://%s/metrics", httpAddr)
	log.Printf("Jaeger UI available at http://localhost:16686")

	select {
	case <-ctx.Done():
	case err := <-errCh:
		stop()
		return err
	}

	return shutdownHTTPServer(server)
}

func serveHTTP(server *http.Server, errCh chan<- error) {
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		errCh <- fmt.Errorf("serve HTTP: %w", err)
	}
}

func shutdownHTTPServer(server *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown HTTP server: %w", err)
	}

	return nil
}

func produceHandler(client *xkafka.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		records := make([]*kgo.Record, 0, mixedBatchRecordCount)
		for id := 1; id <= mixedBatchRecordCount; id++ {
			topic := topicA
			if id > mixedBatchRecordCount/2 {
				topic = topicB
			}

			record, err := newRecord(
				topic,
				id,
				fmt.Sprintf("otel message %d", id),
			)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			records = append(records, record)
		}

		if err := client.ProduceSync(r.Context(), records...); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(
			w,
			"published %d records: %d to %s and %d to %s\n",
			mixedBatchRecordCount,
			mixedBatchRecordCount/2,
			topicA,
			mixedBatchRecordCount/2,
			topicB,
		)
	}
}

func produceErrorHandler(client *xkafka.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const messageID = 888

		record, err := newRecord(
			topicA,
			messageID,
			"forced handler error",
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := client.ProduceSync(r.Context(), record); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintln(w, "published record that fails once in the consumer handler")
	}
}

func newRecord(topic string, id int, text string) (*kgo.Record, error) {
	payload, err := json.Marshal(message{
		ID:   id,
		Text: text,
	})
	if err != nil {
		return nil, err
	}

	return &kgo.Record{
		Topic: topic,
		Key:   []byte(strconv.Itoa(id)),
		Value: payload,
	}, nil
}

func newBatchHandler() xkafka.BatchHandlerFunc {
	failedOnce := false

	return func(_ context.Context, records []*kgo.Record) error {
		hasForcedError := false
		for _, record := range records {
			if string(record.Key) == "888" {
				hasForcedError = true
				break
			}
		}

		if hasForcedError && !failedOnce {
			failedOnce = true
			return errors.New("forced handler error")
		}

		for _, record := range records {
			var msg message
			if err := json.Unmarshal(record.Value, &msg); err != nil {
				return err
			}

			fmt.Fprintf(
				os.Stderr,
				"consume: topic=%s partition=%d offset=%d key=%q msg=%+v\n",
				record.Topic,
				record.Partition,
				record.Offset,
				record.Key,
				msg,
			)
		}

		failedOnce = false
		return nil
	}
}

func newResource() (*resource.Resource, error) {
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		),
	)
}

func newMeterProvider(registry *prometheus.Registry, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	exporter, err := otelprom.New(
		otelprom.WithRegisterer(registry),
	)
	if err != nil {
		return nil, err
	}

	return sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithResource(res),
	), nil
}

func newTracerProvider(ctx context.Context, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracehttp.New(
		ctx,
		otlptracehttp.WithEndpointURL(tracesEndpoint),
	)
	if err != nil {
		return nil, err
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	), nil
}

func pingClient(ctx context.Context, client *xkafka.Client) error {
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx); err != nil {
		return fmt.Errorf("ping kafka client: %w", err)
	}

	return nil
}

func shutdownClient(client *xkafka.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Shutdown(ctx); err != nil {
		log.Printf("shutdown kafka client: %v", err)
	}
}

func shutdownMeterProvider(provider *sdkmetric.MeterProvider) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := provider.Shutdown(ctx); err != nil {
		log.Printf("shutdown meter provider: %v", err)
	}
}

func shutdownTracerProvider(provider *sdktrace.TracerProvider) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := provider.Shutdown(ctx); err != nil {
		log.Printf("shutdown tracer provider: %v", err)
	}
}
