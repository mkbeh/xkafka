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
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	brokers  = "localhost:29092"
	topic    = "sample-topic"
	group    = "sample-group"
	httpAddr = "localhost:8080"
)

var client *xkafka.Client

type message struct {
	ID   int    `json:"id"`
	Text string `json:"text"`
}

func produceHandler(w http.ResponseWriter, r *http.Request) {
	payload, err := json.Marshal(message{
		ID:   42,
		Text: "hello from xkafka",
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := client.ProduceSync(r.Context(), &kgo.Record{
		Key:   []byte("basic"),
		Value: payload,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func statsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(client.Stats()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	var err error

	client, err = xkafka.NewClient(
		xkafka.WithName("basic"),
		xkafka.WithKafkaOptions(
			kgo.SeedBrokers(brokers),
			kgo.DefaultProduceTopic(topic),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
		),
		xkafka.WithBatchHandler(handleRecords),
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
	mux.HandleFunc("GET /stats", statsHandler)

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
