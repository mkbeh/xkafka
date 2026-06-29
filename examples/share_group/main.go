package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/mkbeh/xkafka"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultMessagesCount          = 30
	defaultConsumersCount         = 4
	defaultMaxRecords             = 5
	defaultRejectAfterDeliveries  = 3
	defaultReleaseTimeout         = 5 * time.Second
	defaultHandlerProcessingDelay = 500 * time.Millisecond
)

var (
	producer  *xkafka.Client
	consumers []*xkafka.Client
)

var (
	brokers string

	topic                 string
	group                 string
	consumersCount        int
	messagesCount         int
	maxRecords            int32
	releaseTimeout        time.Duration
	rejectAfterDeliveries int32
)

func init() {
	brokers = os.Getenv("KAFKA_BROKERS")

	topic = getenv("KAFKA_SHARE_TOPIC", "sample-share-topic")
	group = getenv("KAFKA_SHARE_GROUP", "sample-share-group")
	consumersCount = int(getenvInt32("KAFKA_SHARE_CONSUMERS", defaultConsumersCount))
	messagesCount = int(getenvInt32("KAFKA_SHARE_MESSAGES", defaultMessagesCount))
	maxRecords = getenvInt32("KAFKA_SHARE_MAX_RECORDS", defaultMaxRecords)

	releaseTimeout = getenvDuration("KAFKA_SHARE_RELEASE_TIMEOUT", defaultReleaseTimeout)
	rejectAfterDeliveries = getenvInt32(
		"KAFKA_SHARE_REJECT_AFTER_DELIVERIES",
		defaultRejectAfterDeliveries,
	)
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

	records := make([]*kgo.Record, 0, messagesCount)

	for i := 0; i < messagesCount; i++ {
		message := Message{
			ID: msg.ID + i,
		}

		payload, err := json.Marshal(&message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		records = append(records, &kgo.Record{
			Topic: topic,
			Key:   []byte(strconv.Itoa(message.ID)),
			Value: payload,
		})
	}

	if err := producer.ProduceSync(r.Context(), records...); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintf(w, "published %d share messages\n", len(records))
}

func produceErrorHandler(w http.ResponseWriter, r *http.Request) {
	msg := Message{ID: 888}

	payload, err := json.Marshal(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := producer.ProduceSync(r.Context(), &kgo.Record{
		Topic: topic,
		Key:   []byte(strconv.Itoa(msg.ID)),
		Value: payload,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintln(w, "share error message published")
}

func producePanicHandler(w http.ResponseWriter, r *http.Request) {
	msg := Message{ID: 444}

	payload, err := json.Marshal(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := producer.ProduceSync(r.Context(), &kgo.Record{
		Topic: topic,
		Key:   []byte(strconv.Itoa(msg.ID)),
		Value: payload,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintln(w, "share panic message published")
}

func main() {
	ctx := context.Background()

	var err error

	producer, err = xkafka.NewClient(
		xkafka.WithConfig(&xkafka.Config{
			Brokers: brokers,
		}),
		xkafka.WithClientID("share-producer"),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer producer.Shutdown(ctx)

	for i := 1; i <= consumersCount; i++ {
		consumer, err := newConsumer(i)
		if err != nil {
			log.Fatalln(err)
		}

		go func() {
			if err := consumer.HandleFetches(ctx); err != nil {
				log.Fatalln(err)
			}
		}()

		consumers = append(consumers, consumer)
	}

	defer func() {
		for _, consumer := range consumers {
			if err := consumer.Shutdown(ctx); err != nil {
				log.Println(err)
			}
		}
	}()

	http.HandleFunc("/share", produceHandler)
	http.HandleFunc("/share-error", produceErrorHandler)
	http.HandleFunc("/share-panic", producePanicHandler)
	http.Handle("/metrics", promhttp.Handler())

	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
		log.Fatalln("unable to start web server:", err)
	}
}

func newConsumer(index int) (*xkafka.Client, error) {
	clientID := fmt.Sprintf("sample-share-client-%d", index)

	return xkafka.NewClient(
		xkafka.WithConfig(&xkafka.Config{
			Enabled: true,
			Brokers: brokers,
			Topics:  topic,

			MaxPollRecords:  int(maxRecords),
			ShareGroup:      group,
			ShareMaxRecords: maxRecords,

			ShareReleaseTimeout:        releaseTimeout,
			ShareRejectAfterDeliveries: rejectAfterDeliveries,
		}),
		xkafka.WithClientID(clientID),
		xkafka.WithConsumerShareBatchHandler(func(_ context.Context, records []*kgo.Record) error {
			fmt.Printf("share consume: client_id=%s, records=%d\n", clientID, len(records))

			time.Sleep(defaultHandlerProcessingDelay)

			for _, record := range records {
				var msg Message
				if err := json.Unmarshal(record.Value, &msg); err != nil {
					return err
				}

				fmt.Printf(
					"  record: client_id=%s, topic=%s, partition=%d, offset=%d, delivery_count=%d, msg=%+v\n",
					clientID,
					record.Topic,
					record.Partition,
					record.Offset,
					record.DeliveryCount(),
					msg,
				)

				if msg.ID == 888 {
					return fmt.Errorf("forced share handler error")
				}

				if msg.ID == 444 {
					panic("forced share handler panic")
				}
			}

			return nil
		}),
	)
}

func getenv(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	return value
}

func getenvInt32(name string, fallback int32) int32 {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		log.Fatalf("invalid %s: %v", name, err)
	}

	if parsed < 0 {
		log.Fatalf("%s must be greater than or equal to zero", name)
	}

	return int32(parsed)
}

func getenvDuration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		log.Fatalf("invalid %s: %v", name, err)
	}

	return parsed
}
