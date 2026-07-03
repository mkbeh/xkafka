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
)

var (
	producer *xkafka.Client
	consumer *xkafka.Client
)

var (
	brokers         string
	topic           string
	group           string
	transactionalID string
)

func init() {
	brokers = os.Getenv("KAFKA_BROKERS")
	topic = getenv("KAFKA_TX_TOPIC", "sample-tx-topic")
	group = getenv("KAFKA_TX_GROUP", "sample-tx-group")
	transactionalID = getenv("KAFKA_TRANSACTIONAL_ID", "sample-tx-producer")
}

type Message struct {
	ID int `json:"id"`
}

func produceHandler(w http.ResponseWriter, r *http.Request) {
	var msg Message

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err := producer.RunInTx(r.Context(), func(ctx context.Context, tx *xkafka.Tx) error {
		payload, err := json.Marshal(&msg)
		if err != nil {
			return err
		}

		if err := tx.ProduceSync(ctx, &kgo.Record{
			Key:   []byte(strconv.Itoa(msg.ID)),
			Value: payload,
		}); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintln(w, "transaction committed")
}

func produceErrorHandler(w http.ResponseWriter, r *http.Request) {
	var msg Message

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err := producer.RunInTx(r.Context(), func(ctx context.Context, tx *xkafka.Tx) error {
		payload, err := json.Marshal(&msg)
		if err != nil {
			return err
		}

		if err := tx.ProduceSync(ctx, &kgo.Record{
			Key:   []byte(strconv.Itoa(msg.ID)),
			Value: payload,
		}); err != nil {
			return err
		}

		return fmt.Errorf("forced error inside kafka transaction")
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func producePanicHandler(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if recovered := recover(); recovered != nil {
			http.Error(
				w,
				fmt.Sprintf("panic recovered in handler: %v", recovered),
				http.StatusInternalServerError,
			)
		}
	}()

	var msg Message

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err := producer.RunInTx(r.Context(), func(ctx context.Context, tx *xkafka.Tx) error {
		payload, err := json.Marshal(&msg)
		if err != nil {
			return err
		}

		if err := tx.ProduceSync(ctx, &kgo.Record{
			Key:   []byte(strconv.Itoa(msg.ID)),
			Value: payload,
		}); err != nil {
			return err
		}

		panic("forced panic inside kafka transaction")
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func main() {
	ctx := context.Background()

	var err error

	producer, err = xkafka.NewClient(
		xkafka.WithConfig(&xkafka.Config{
			Brokers:             brokers,
			DefaultProduceTopic: topic,
			TransactionalID:     transactionalID,
		}),
		xkafka.WithClientID("transactions-producer"),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer producer.Shutdown(ctx)

	consumer, err = xkafka.NewClient(
		xkafka.WithConfig(&xkafka.Config{
			Enabled: true,
			Brokers: brokers,
			Topics:  topic,
			Group:   group,
		}),
		xkafka.WithClientID("transactions-consumer"),
		xkafka.WithFetchIsolationLevel(kgo.ReadCommitted()),
		xkafka.WithConsumerBatchHandler(func(_ context.Context, records []*kgo.Record) error {
			for _, record := range records {
				var msg Message
				if err := json.Unmarshal(record.Value, &msg); err != nil {
					return err
				}

				fmt.Printf(
					"consume committed tx record: topic=%s, partition=%d, offset=%d, msg=%+v\n",
					record.Topic,
					record.Partition,
					record.Offset,
					msg,
				)
			}

			return nil
		}),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer consumer.Shutdown(ctx)

	go func() {
		if err := consumer.HandleFetches(ctx); err != nil {
			log.Fatalln(err)
		}
	}()

	http.HandleFunc("/tx", produceHandler)
	http.HandleFunc("/tx-error", produceErrorHandler)
	http.HandleFunc("/tx-panic", producePanicHandler)
	http.Handle("/metrics", promhttp.Handler())

	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
		log.Fatalln("unable to start web server:", err)
	}
}

func getenv(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	return value
}
