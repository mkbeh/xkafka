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
	producer   *kafka.Producer
	txProducer *kafka.Producer
	consumer   *kafka.Consumer
)

var (
	brokers string
	topics  string
	group   string
)

func init() {
	brokers = os.Getenv("KAFKA_BROKERS")
	topics = os.Getenv("KAFKA_TOPICS")
	group = os.Getenv("KAFKA_GROUP")
}

type Message struct {
	ID int `json:"id"`
}

func produceSyncHandler(w http.ResponseWriter, r *http.Request) {
	var msg Message

	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	payload, err := json.Marshal(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = producer.ProduceSync(r.Context(), &kgo.Record{
		Key:   kafka.ConvertAnyToBytes(msg.ID),
		Value: payload,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func produceAsyncHandler(w http.ResponseWriter, r *http.Request) {
	var msg Message

	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	producer.ProduceAsync(context.Background(), &kgo.Record{
		Key:   kafka.ConvertAnyToBytes(msg.ID),
		Value: payload,
	})
}

func produceTxHandler(w http.ResponseWriter, r *http.Request) {
	var msg Message

	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = txProducer.RunInTx(r.Context(), func(ctx context.Context) error {
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

	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = txProducer.RunInTx(r.Context(), func(ctx context.Context) error {
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

	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = txProducer.RunInTx(r.Context(), func(ctx context.Context) error {
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
	var err error

	ctx := context.Background()

	// init regular producer

	producerCfg := &kafka.ProducerConfig{
		Brokers:             brokers,
		DefaultProduceTopic: topics,
	}

	producer, err = kafka.NewProducer(
		kafka.WithProducerConfig(producerCfg),
		kafka.WithProducerClientID("sample-client"),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer producer.Close(ctx)

	// init transactional producer

	txProducerCfg := &kafka.ProducerConfig{
		Brokers:             brokers,
		DefaultProduceTopic: topics,
		TransactionalID:     "sample-tx-producer",
	}

	txProducer, err = kafka.NewProducer(
		kafka.WithProducerConfig(txProducerCfg),
		kafka.WithProducerClientID("sample-tx-client"),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer txProducer.Close(ctx)

	// init consumer

	consumerCfg := &kafka.ConsumerConfig{
		Enabled: true,
		Brokers: brokers,
		Topics:  topics,
		Group:   group,
	}

	consumer, err = kafka.NewConsumer(
		kafka.WithConsumerConfig(consumerCfg),
		kafka.WithConsumerClientID("sample-client"),
		kafka.WithFetchIsolationLevel(kgo.ReadCommitted()),
		kafka.WithConsumerHandler(func(_ context.Context, msg *kgo.Record) error {
			var message Message
			if err := json.Unmarshal(msg.Value, &message); err != nil {
				return err
			}
			fmt.Printf("\nconsume: topic=%s, msg=%+v", msg.Topic, message)
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

	http.HandleFunc("/sync", produceSyncHandler)
	http.HandleFunc("/async", produceAsyncHandler)
	http.HandleFunc("/tx", produceTxHandler)
	http.HandleFunc("/tx-error", produceTxErrorHandler)
	http.HandleFunc("/tx-panic", produceTxPanicHandler)
	http.Handle("/metrics", promhttp.Handler())

	err = http.ListenAndServe("localhost:8080", nil)
	if err != nil {
		log.Fatalln("Unable to start web server:", err)
	}
}
