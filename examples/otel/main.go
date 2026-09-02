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
	"syscall"
	"time"

	"github.com/mkbeh/xkafka"
	"github.com/mkbeh/xkafka/extra/otelxkafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	clientName = "otel"
	brokers    = "localhost:29092"
	topic      = "sample-otel-topic"
	group      = "sample-otel-group"
	httpAddr   = "localhost:8080"
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
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	meterProvider, metrics, kafkaMeter, err := newMetrics()
	if err != nil {
		return fmt.Errorf("create metrics: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := meterProvider.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown meter provider: %v", err)
		}
	}()

	tracerProvider, kafkaTracer, err := newTracer()
	if err != nil {
		return fmt.Errorf("create tracer: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := tracerProvider.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown tracer provider: %v", err)
		}
	}()

	kafkaTelemetry := kotel.NewKotel(
		kotel.WithMeter(kafkaMeter),
		kotel.WithTracer(kafkaTracer),
	)

	client, err := xkafka.NewClient(
		xkafka.WithName(clientName),
		xkafka.WithLogger(
			kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil),
		),
		xkafka.WithMetrics(metrics),
		xkafka.WithKafkaOptions(
			kgo.SeedBrokers(brokers),
			kgo.DefaultProduceTopic(topic),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.WithHooks(kafkaTelemetry.Hooks()...),
		),
		xkafka.WithBatchHandler(handleRecords),
	)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := client.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown kafka client: %v", err)
		}
	}()

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = client.Ping(pingCtx)
	cancel()
	if err != nil {
		return err
	}

	go func() {
		if err := client.HandleFetches(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("handle kafka fetches: %v", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /produce", produceHandler(client))

	server := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown HTTP server: %v", err)
		}
	}()

	log.Printf("HTTP server listening on http://%s", httpAddr)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func newMetrics() (*sdkmetric.MeterProvider, *otelxkafka.Metrics, *kotel.Meter, error) {
	exporter, err := stdoutmetric.New(
		stdoutmetric.WithPrettyPrint(),
	)
	if err != nil {
		return nil, nil, nil, err
	}

	reader := sdkmetric.NewPeriodicReader(
		exporter,
		sdkmetric.WithInterval(5*time.Second),
	)

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
	)

	metrics, err := otelxkafka.New(
		otelxkafka.WithMeterProvider(meterProvider),
	)
	if err != nil {
		return nil, nil, nil, err
	}

	kafkaMeter := kotel.NewMeter(
		kotel.MeterProvider(meterProvider),
	)

	return meterProvider, metrics, kafkaMeter, nil
}

func newTracer() (*sdktrace.TracerProvider, *kotel.Tracer, error) {
	exporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
	)
	if err != nil {
		return nil, nil, err
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
	)

	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)

	kafkaTracer := kotel.NewTracer(
		kotel.ClientID(clientName),
		kotel.ConsumerGroup(group),
		kotel.TracerProvider(tracerProvider),
		kotel.TracerPropagator(propagator),
	)

	return tracerProvider, kafkaTracer, nil
}

func produceHandler(client *xkafka.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		payload, err := json.Marshal(message{
			ID:   42,
			Text: "hello from xkafka otel example",
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := client.ProduceSync(r.Context(), &kgo.Record{
			Key:   []byte("otel"),
			Value: payload,
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func handleRecords(_ context.Context, records []*kgo.Record) error {
	for _, record := range records {
		var msg message
		if err := json.Unmarshal(record.Value, &msg); err != nil {
			return err
		}

		fmt.Printf(
			"consume: topic=%s partition=%d offset=%d key=%q msg=%+v\n",
			record.Topic,
			record.Partition,
			record.Offset,
			record.Key,
			msg,
		)
	}

	return nil
}
