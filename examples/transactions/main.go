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
	brokers         = "localhost:29092"
	topic           = "sample-tx-topic"
	group           = "sample-tx-group"
	transactionalID = "sample-tx-producer"
	httpAddr        = "localhost:8080"
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

	// Use a dedicated transactional producer.
	producer, err := xkafka.NewClient(
		xkafka.WithName("transactions-producer"),
		xkafka.WithKafkaOptions(
			kgo.SeedBrokers(brokers),
			kgo.DefaultProduceTopic(topic),
			kgo.TransactionalID(transactionalID),
		),
	)
	if err != nil {
		return fmt.Errorf("create transactional producer: %w", err)
	}
	defer shutdownClient("transactional producer", producer)

	// Read only records from committed transactions.
	consumer, err := xkafka.NewClient(
		xkafka.WithName("transactions-consumer"),
		xkafka.WithKafkaOptions(
			kgo.SeedBrokers(brokers),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		),
		xkafka.WithBatchHandler(handleCommittedRecords),
	)
	if err != nil {
		return fmt.Errorf("create read-committed consumer: %w", err)
	}
	defer shutdownClient("read-committed consumer", consumer)

	if err := pingClient(ctx, "transactional producer", producer); err != nil {
		return err
	}

	if err := pingClient(ctx, "read-committed consumer", consumer); err != nil {
		return err
	}

	errCh := make(chan error, 2)
	go func() {
		if err := consumer.HandleFetches(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("handle kafka fetches: %w", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /tx", transactionHandler(producer))
	mux.HandleFunc("POST /tx-error", transactionErrorHandler(producer))
	mux.HandleFunc("POST /tx-panic", transactionPanicHandler(producer))

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

func transactionHandler(client *xkafka.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		msg, err := decodeMessage(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		record, err := newRecord(msg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := client.RunInTx(r.Context(), func(ctx context.Context, tx *xkafka.Tx) error {
			return tx.ProduceSync(ctx, record)
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintln(w, "transaction committed")
	}
}

func transactionErrorHandler(client *xkafka.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		msg, err := decodeMessage(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		record, err := newRecord(msg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = client.RunInTx(r.Context(), func(ctx context.Context, tx *xkafka.Tx) error {
			if err := tx.ProduceSync(ctx, record); err != nil {
				return err
			}

			return errors.New("forced transaction error")
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	}
}

func transactionPanicHandler(client *xkafka.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				http.Error(
					w,
					fmt.Sprintf("transaction panic: %v", recovered),
					http.StatusInternalServerError,
				)
			}
		}()

		msg, err := decodeMessage(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		record, err := newRecord(msg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := client.RunInTx(r.Context(), func(ctx context.Context, tx *xkafka.Tx) error {
			if err := tx.ProduceSync(ctx, record); err != nil {
				return err
			}

			panic("forced transaction panic")
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	}
}

func handleCommittedRecords(_ context.Context, records []*kgo.Record) error {
	for _, record := range records {
		var msg message
		if err := json.Unmarshal(record.Value, &msg); err != nil {
			return err
		}

		fmt.Printf(
			"consume committed transaction: topic=%s partition=%d offset=%d key=%q msg=%+v\n",
			record.Topic,
			record.Partition,
			record.Offset,
			record.Key,
			msg,
		)
	}

	return nil
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
