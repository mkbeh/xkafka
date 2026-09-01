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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const (
	brokers  = "localhost:29092"
	topic    = "sample-otel-topic"
	group    = "sample-otel-group"
	httpAddr = "localhost:9464"
)

var client *xkafka.Client

type message struct {
	ID   int    `json:"id"`
	Text string `json:"text"`
}

func produceHandler(w http.ResponseWriter, r *http.Request) {
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

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	registry := prometheus.NewRegistry()

	exporter, err := otelprom.New(
		otelprom.WithRegisterer(registry),
	)
	if err != nil {
		log.Fatalln(err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
	)

	metrics, err := otelxkafka.New(
		otelxkafka.WithMeterProvider(meterProvider),
	)
	if err != nil {
		log.Fatalln(err)
	}

	kafkaMetrics := kprom.NewMetrics(
		"kafka",
		kprom.Registry(registry),
		kprom.WithClientLabel(),
		kprom.FetchAndProduceDetail(
			kprom.ByNode,
			kprom.ByTopic,
			kprom.Records,
			kprom.Batches,
			kprom.CompressedBytes,
			kprom.UncompressedBytes,
			kprom.ConsistentNaming,
		),
		kprom.Histograms(
			kprom.ReadWait,
			kprom.ReadTime,
			kprom.WriteWait,
			kprom.WriteTime,
			kprom.RequestDurationE2E,
			kprom.RequestThrottled,
		),
	)

	client, err = xkafka.NewClient(
		xkafka.WithName("otel"),
		xkafka.WithMetrics(metrics),
		xkafka.WithKafkaOptions(
			kgo.SeedBrokers(brokers),
			kgo.DefaultProduceTopic(topic),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.WithHooks(kafkaMetrics),
		),
		xkafka.WithConsumerBatchHandler(handleRecords),
	)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := client.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown kafka client: %v", err)
		}
		if err := meterProvider.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown meter provider: %v", err)
		}
	}()

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if err := client.Ping(pingCtx); err != nil {
		cancel()
		log.Fatalln(err)
	}
	cancel()

	go func() {
		if err := client.HandleFetches(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("handle kafka fetches: %v", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /produce", produceHandler)
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

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
		log.Fatalln(err)
	}
}
