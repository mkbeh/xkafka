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
	client *xkafka.Client

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

	if err := client.ProduceSync(r.Context(), &kgo.Record{
		Key:   []byte(strconv.Itoa(msg.ID)),
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

	client.Produce(context.Background(), &kgo.Record{
		Key:   []byte(strconv.Itoa(msg.ID)),
		Value: payload,
	}, nil)

	w.WriteHeader(http.StatusAccepted)
}

func producePanicHandler(w http.ResponseWriter, r *http.Request) {
	msg := Message{ID: 444}

	payload, err := json.Marshal(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := client.ProduceSync(r.Context(), &kgo.Record{
		Key:   []byte(strconv.Itoa(msg.ID)),
		Value: payload,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintln(w, "published message that triggers handler panic")
}

func main() {
	ctx := context.Background()

	var err error

	client, err = xkafka.NewClient(
		xkafka.WithConfig(&xkafka.Config{
			Enabled:             true,
			Brokers:             brokers,
			DefaultProduceTopic: topic,
			Topics:              topic,
			Group:               group,
		}),
		xkafka.WithClientID("sample-client"),
		xkafka.WithConsumerBatchHandler(func(_ context.Context, records []*kgo.Record) error {
			for _, record := range records {
				var msg Message
				if err := json.Unmarshal(record.Value, &msg); err != nil {
					return err
				}

				if msg.ID == 444 {
					panic("forced consumer handler panic")
				}

				fmt.Printf("consume: topic=%s, partition=%d, offset=%d, msg=%+v\n",
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
	defer client.Shutdown(ctx)

	go func() {
		if err := client.HandleFetches(ctx); err != nil {
			log.Fatalln(err)
		}
	}()

	http.HandleFunc("/sync", produceSyncHandler)
	http.HandleFunc("/async", produceAsyncHandler)
	http.HandleFunc("/panic", producePanicHandler)
	http.Handle("/metrics", promhttp.Handler())

	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
		log.Fatalln("unable to start web server:", err)
	}
}
