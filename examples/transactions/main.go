package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	kafka "github.com/mkbeh/xkafka"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	txProducer *kafka.Producer
	consumer   *kafka.Consumer
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

func produceTxHandler(w http.ResponseWriter, r *http.Request) {
	var msg Message

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err := txProducer.RunInTx(r.Context(), func(ctx context.Context) error {
		payload, err := json.Marshal(&msg)
		if err != nil {
			return err
		}

		if err := txProducer.ProduceSync(ctx, &kgo.Record{
			Key:   kafka.ConvertAnyToBytes(msg.ID),
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
}

func produceTxErrorHandler(w http.ResponseWriter, r *http.Request) {
	var msg Message

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err := txProducer.RunInTx(r.Context(), func(ctx context.Context) error {
		payload, err := json.Marshal(&msg)
		if err != nil {
			return err
		}

		if err := txProducer.ProduceSync(ctx, &kgo.Record{
			Key:   kafka.ConvertAnyToBytes(msg.ID),
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

func produceTxPanicHandler(w http.ResponseWriter, r *http.Request) {
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

	err := txProducer.RunInTx(r.Context(), func(ctx context.Context) error {
		payload, err := json.Marshal(&msg)
		if err != nil {
			return err
		}

		if err := txProducer.ProduceSync(ctx, &kgo.Record{
			Key:   kafka.ConvertAnyToBytes(msg.ID),
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

	kafka.WithConfig(&kafka.Config{
		Enabled:       true,
		Brokers:       brokers,
		Topics:        topic,
		Group:         group,
		ReadCommitted: true,
	}),

		txProducer, err = kafka.NewProducer(
		kafka.WithConfig(&kafka.Config{
			Brokers:             brokers,
			DefaultProduceTopic: topic,
			TransactionalID:     transactionalID,
		}),
		kafka.WithProducerClientID("transactions-client"),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer txProducer.Close(ctx)

	consumer, err = kafka.NewConsumer(
		kafka.WithConfig(&kafka.Config{
			Enabled: true,
			Brokers: brokers,
			Topics:  topic,
			Group:   group,
		}),
		kafka.WithConsumerClientID("transactions-client"),
		kafka.WithFetchIsolationLevel(kgo.ReadCommitted()),
		kafka.WithConsumerHandler(func(_ context.Context, record *kgo.Record) error {
			var msg Message
			if err := json.Unmarshal(record.Value, &msg); err != nil {
				return err
			}

			fmt.Printf("consume committed tx record: topic=%s, partition=%d, offset=%d, msg=%+v\n",
				record.Topic,
				record.Partition,
				record.Offset,
				msg,
			)

			return nil
		}),
	)
	if err != nil {
		log.Fatalln(err)
	}

	if err := consumer.PreRun(ctx); err != nil {
		log.Fatalln(err)
	}

	go func() {
		if err := consumer.Run(ctx); err != nil {
			log.Fatalln(err)
		}
	}()

	defer consumer.Shutdown(ctx)

	http.HandleFunc("/tx", produceTxHandler)
	http.HandleFunc("/tx-error", produceTxErrorHandler)
	http.HandleFunc("/tx-panic", produceTxPanicHandler)
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
