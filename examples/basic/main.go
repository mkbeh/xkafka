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

	client, err := xkafka.NewClient(
		xkafka.WithName("basic"),
		xkafka.WithKafkaOptions(
			kgo.SeedBrokers(brokers),
			kgo.DefaultProduceTopic(topic),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		),
		xkafka.WithBatchHandler(handleRecords),
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

	server := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go serveHTTP(server, errCh)

	log.Printf("HTTP server listening on http://%s", httpAddr)

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
