package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/mkbeh/xkafka"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var client *xkafka.Client

var (
	brokers        string
	topic          string
	group          string
	serviceName    string
	tracesEndpoint string
)

var (
	tracerProvider *sdktrace.TracerProvider
	tracer         trace.Tracer
	propagator     propagation.TextMapPropagator
)

func init() {
	brokers = os.Getenv("KAFKA_BROKERS")

	topic = getenv("KAFKA_TRACING_TOPIC", "sample-tracing-topic")
	group = getenv("KAFKA_TRACING_GROUP", "sample-tracing-group")

	serviceName = getenv("OTEL_SERVICE_NAME", "xkafka-tracing-example")
	tracesEndpoint = getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4318/v1/traces")
}

type Message struct {
	ID int `json:"id"`
}

func produceHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "produce tracing message")
	defer span.End()

	var msg Message

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		recordSpanError(span, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	payload, err := json.Marshal(&msg)
	if err != nil {
		recordSpanError(span, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	span.SetAttributes(
		attribute.Int("message.id", msg.ID),
		attribute.String("messaging.destination.name", topic),
		attribute.String("xkafka.example.expected_outcome", "ok"),
	)

	if err := client.ProduceSync(ctx, &kgo.Record{
		Context: ctx,
		Topic:   topic,
		Key:     []byte(strconv.Itoa(msg.ID)),
		Value:   payload,
	}); err != nil {
		recordSpanError(span, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	recordSpanOK(span)

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintln(w, "tracing message published")
}

func produceErrorHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "produce tracing error message")
	defer span.End()

	msg := Message{ID: 888}

	payload, err := json.Marshal(&msg)
	if err != nil {
		recordSpanError(span, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	span.SetAttributes(
		attribute.Int("message.id", msg.ID),
		attribute.String("messaging.destination.name", topic),
		attribute.String("xkafka.example.expected_outcome", "consumer_error"),
	)

	if err := client.ProduceSync(ctx, &kgo.Record{
		Context: ctx,
		Topic:   topic,
		Key:     []byte(strconv.Itoa(msg.ID)),
		Value:   payload,
	}); err != nil {
		recordSpanError(span, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	recordSpanOK(span)

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintln(w, "tracing error message published")
}

func main() {
	ctx := context.Background()

	var err error

	tracerProvider, err = newTracerProvider(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			log.Println("tracer provider shutdown error:", err)
		}
	}()

	propagator = propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagator)

	tracer = tracerProvider.Tracer("xkafka/examples/tracing")

	client, err = xkafka.NewClient(
		xkafka.WithConfig(&xkafka.Config{
			Enabled:             true,
			Brokers:             brokers,
			DefaultProduceTopic: topic,
			Topics:              topic,
			Group:               group,
		}),
		xkafka.WithClientID("tracing-client"),
		xkafka.WithTracerProvider(tracerProvider),
		xkafka.WithTracerPropagator(propagator),
		xkafka.WithConsumerBatchHandler(func(ctx context.Context, records []*kgo.Record) error {
			for _, record := range records {
				if err := handleRecord(ctx, record); err != nil {
					return err
				}
			}

			return nil
		}),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer client.Shutdown(ctx)

	go func() {
		if err := client.HandleFetches(ctx); err != nil {
			log.Fatalln(err)
		}
	}()

	http.HandleFunc("/produce", produceHandler)
	http.HandleFunc("/produce-error", produceErrorHandler)
	http.Handle("/metrics", promhttp.Handler())

	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
		log.Fatalln("unable to start web server:", err)
	}
}

func handleRecord(ctx context.Context, record *kgo.Record) error {
	recordCtx := ctx
	if record.Context != nil {
		recordCtx = record.Context
	}

	_, span := tracer.Start(recordCtx, "process tracing message")
	defer span.End()

	var msg Message
	if err := json.Unmarshal(record.Value, &msg); err != nil {
		recordSpanError(span, err)
		return err
	}

	span.SetAttributes(
		attribute.Int("message.id", msg.ID),
		attribute.String("messaging.destination.name", record.Topic),
		attribute.Int("messaging.kafka.partition", int(record.Partition)),
		attribute.Int64("messaging.kafka.offset", record.Offset),
	)

	if msg.ID == 888 {
		err := fmt.Errorf("forced tracing handler error")
		recordSpanError(span, err)

		return err
	}

	fmt.Printf("consume: topic=%s, partition=%d, offset=%d, msg=%+v\n",
		record.Topic,
		record.Partition,
		record.Offset,
		msg,
	)

	recordSpanOK(span)

	return nil
}

func newTracerProvider(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(tracesEndpoint),
	)
	if err != nil {
		return nil, fmt.Errorf("create otlp trace exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			"",
			attribute.String("service.name", serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create trace resource: %w", err)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	), nil
}

func recordSpanOK(span trace.Span) {
	span.SetStatus(codes.Ok, "")
	span.SetAttributes(
		attribute.String("xkafka.example.outcome", "ok"),
	)
}

func recordSpanError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	span.SetAttributes(
		attribute.Bool("error", true),
		attribute.String("xkafka.example.outcome", "error"),
		attribute.String("xkafka.example.error", err.Error()),
	)
}

func getenv(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	return value
}
