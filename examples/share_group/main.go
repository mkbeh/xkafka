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
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	brokers               = "localhost:29092"
	topic                 = "sample-share-topic"
	group                 = "sample-share-group"
	httpAddr              = "localhost:8080"
	consumerCount         = 3
	messageCount          = 12
	maxRecords            = 2
	rejectAfterDeliveries = 3
	releaseTimeout        = time.Second
	consumerPollInterval  = 100 * time.Millisecond
	forcedHandlerErrorID  = 888
	forcedHandlerPanicID  = 444
)

type message struct {
	ID int `json:"id"`
}

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	producer, err := xkafka.NewClient(
		xkafka.WithName("share-producer"),
		xkafka.WithKafkaOptions(
			kgo.SeedBrokers(brokers),
			kgo.DefaultProduceTopic(topic),
		),
	)
	if err != nil {
		return fmt.Errorf("create share producer: %w", err)
	}
	defer shutdownClient("share producer", producer)

	if err := pingClient(ctx, "share producer", producer); err != nil {
		return err
	}

	errCh := make(chan error, consumerCount+1)

	for i := 1; i <= consumerCount; i++ {
		consumer, err := newShareConsumer(i)
		if err != nil {
			return fmt.Errorf("create share consumer %d: %w", i, err)
		}

		name := fmt.Sprintf("share consumer %d", i)
		defer shutdownClient(name, consumer)

		if err := pingClient(ctx, name, consumer); err != nil {
			return err
		}

		go func() {
			if err := consumer.HandleFetches(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("%s: handle kafka fetches: %w", name, err)
			}
		}()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /share", shareHandler(producer))
	mux.HandleFunc("POST /share-error", shareSpecialHandler(producer, forcedHandlerErrorID))
	mux.HandleFunc("POST /share-panic", shareSpecialHandler(producer, forcedHandlerPanicID))

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

func newShareConsumer(index int) (*xkafka.Client, error) {
	name := fmt.Sprintf("share-consumer-%d", index)

	return xkafka.NewClient(
		xkafka.WithName(name),
		xkafka.WithKafkaOptions(
			kgo.SeedBrokers(brokers),
			kgo.ConsumeTopics(topic),
			kgo.ShareGroup(group),
			kgo.ShareMaxRecords(maxRecords),
			kgo.ShareMaxRecordsStrict(),
		),
		xkafka.WithPollInterval(consumerPollInterval),
		xkafka.WithShareRejectAfterDeliveries(rejectAfterDeliveries),
		xkafka.WithShareReleaseTimeout(releaseTimeout),
		xkafka.WithBatchHandler(shareBatchHandler(name)),
	)
}

func shareBatchHandler(consumerName string) xkafka.BatchHandlerFunc {
	return func(_ context.Context, records []*kgo.Record) error {
		fmt.Printf("share consume: client=%s records=%d\n", consumerName, len(records))

		for _, record := range records {
			var msg message
			if err := json.Unmarshal(record.Value, &msg); err != nil {
				return fmt.Errorf("decode record: %w", err)
			}

			fmt.Printf(
				"  record: topic=%s partition=%d offset=%d delivery_count=%d key=%q msg=%+v\n",
				record.Topic,
				record.Partition,
				record.Offset,
				record.DeliveryCount(),
				record.Key,
				msg,
			)

			switch msg.ID {
			case forcedHandlerErrorID:
				return errors.New("forced share handler error")
			case forcedHandlerPanicID:
				panic("forced share handler panic")
			}
		}

		return nil
	}
}

func shareHandler(producer *xkafka.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start, err := decodeMessage(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		records := make([]*kgo.Record, 0, messageCount)
		for i := range messageCount {
			record, err := newRecord(message{ID: start.ID + i})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			records = append(records, record)
		}

		if err := producer.ProduceSync(r.Context(), records...); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(w, "published %d share records\n", len(records))
	}
}

func shareSpecialHandler(producer *xkafka.Client, id int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		record, err := newRecord(message{ID: id})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := producer.ProduceSync(r.Context(), record); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(w, "published share record id=%d\n", id)
	}
}

func decodeMessage(r *http.Request) (message, error) {
	var msg message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		return message{}, fmt.Errorf("decode request: %w", err)
	}

	return msg, nil
}

func newRecord(msg message) (*kgo.Record, error) {
	payload, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("encode message: %w", err)
	}

	return &kgo.Record{
		Key:   []byte(strconv.Itoa(msg.ID)),
		Value: payload,
	}, nil
}

func pingClient(ctx context.Context, name string, client *xkafka.Client) error {
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx); err != nil {
		return fmt.Errorf("ping %s: %w", name, err)
	}

	return nil
}

func shutdownClient(name string, client *xkafka.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Shutdown(ctx); err != nil {
		log.Printf("shutdown %s: %v", name, err)
	}
}
