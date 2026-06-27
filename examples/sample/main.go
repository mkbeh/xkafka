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
	producer *kafka.Producer
	consumer *kafka.Consumer
)

var (
	brokers string
	topic   string
	group   string
)

func init() {
	brokers = os.Getenv("KAFKA_BROKERS")
	topic = os.Getenv("KAFKA_TOPICS")
	group = os.Getenv("KAFKA_GROUP")
}

type Message struct {
	ID int `json:"id"`
}

func produceSyncHandler(w http.ResponseWriter, r *http.Request) {
	var msg Message

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	payload, err := json.Marshal(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := producer.ProduceSync(r.Context(), &kgo.Record{
		Key:   kafka.ConvertAnyToBytes(msg.ID),
		Value: payload,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func produceAsyncHandler(w http.ResponseWriter, r *http.Request) {
	var msg Message

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	payload, err := json.Marshal(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	producer.ProduceAsync(context.Background(), &kgo.Record{
		Key:   kafka.ConvertAnyToBytes(msg.ID),
		Value: payload,
	})

	w.WriteHeader(http.StatusAccepted)
}

func main() {
	ctx := context.Background()

	var err error

	producer, err = kafka.NewProducer(
		kafka.WithProducerConfig(&kafka.ProducerConfig{
			Brokers:             brokers,
			DefaultProduceTopic: topic,
		}),
		kafka.WithProducerClientID("sample-client"),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer producer.Close(ctx)

	consumer, err = kafka.NewConsumer(
		kafka.WithConsumerConfig(&kafka.ConsumerConfig{
			Enabled: true,
			Brokers: brokers,
			Topics:  topic,
			Group:   group,
		}),
		kafka.WithConsumerClientID("sample-client"),
		kafka.WithConsumerHandler(func(_ context.Context, record *kgo.Record) error {
			var msg Message
			if err := json.Unmarshal(record.Value, &msg); err != nil {
				return err
			}

			fmt.Printf("consume: topic=%s, partition=%d, offset=%d, msg=%+v\n",
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

	http.HandleFunc("/sync", produceSyncHandler)
	http.HandleFunc("/async", produceAsyncHandler)
	http.Handle("/metrics", promhttp.Handler())

	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
		log.Fatalln("unable to start web server:", err)
	}
}
