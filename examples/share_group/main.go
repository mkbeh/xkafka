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

	kafka "github.com/mkbeh/xkafka"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultShareMessagesCount          = 30
	defaultShareBatchMessagesCount     = 30
	defaultShareConsumersCount         = 4
	defaultShareBatchConsumersCount    = 2
	defaultShareBatchMaxRecords        = 5
	defaultShareRejectAfterDeliveries  = 3
	defaultShareReleaseTimeout         = 5 * time.Second
	defaultShareHandlerProcessingDelay = 500 * time.Millisecond
	defaultBatchHandlerProcessingDelay = 2 * time.Second
)

var (
	producer            *kafka.Producer
	shareConsumers      []*kafka.Consumer
	shareBatchConsumers []*kafka.Consumer
)

var (
	brokers string

	shareTopic                 string
	shareGroup                 string
	shareConsumersCount        int
	shareMessagesCount         int
	shareReleaseTimeout        time.Duration
	shareRejectAfterDeliveries int32

	shareBatchTopic          string
	shareBatchGroup          string
	shareBatchConsumersCount int
	shareBatchMessagesCount  int
	shareBatchMaxRecords     int32
)

func init() {
	brokers = os.Getenv("KAFKA_BROKERS")

	shareTopic = getenv("KAFKA_SHARE_TOPIC", "sample-share-topic")
	shareGroup = getenv("KAFKA_SHARE_GROUP", "sample-share-group")
	shareConsumersCount = int(getenvInt32("KAFKA_SHARE_CONSUMERS", defaultShareConsumersCount))
	shareMessagesCount = int(getenvInt32("KAFKA_SHARE_MESSAGES", defaultShareMessagesCount))

	shareBatchTopic = getenv("KAFKA_SHARE_BATCH_TOPIC", "sample-share-batch-topic")
	shareBatchGroup = getenv("KAFKA_SHARE_BATCH_GROUP", "sample-share-batch-group")
	shareBatchConsumersCount = int(getenvInt32("KAFKA_SHARE_BATCH_CONSUMERS", defaultShareBatchConsumersCount))
	shareBatchMessagesCount = int(getenvInt32("KAFKA_SHARE_BATCH_MESSAGES", defaultShareBatchMessagesCount))
	shareBatchMaxRecords = getenvInt32("KAFKA_SHARE_BATCH_MAX_RECORDS", defaultShareBatchMaxRecords)

	shareReleaseTimeout = getenvDuration("KAFKA_SHARE_RELEASE_TIMEOUT", defaultShareReleaseTimeout)
	shareRejectAfterDeliveries = getenvInt32(
		"KAFKA_SHARE_REJECT_AFTER_DELIVERIES",
		defaultShareRejectAfterDeliveries,
	)
}

type Message struct {
	ID int `json:"id"`
}

func produceShareHandler(w http.ResponseWriter, r *http.Request) {
	var msg Message

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for i := 0; i < shareMessagesCount; i++ {
		message := Message{
			ID: msg.ID + i,
		}

		payload, err := json.Marshal(&message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := producer.ProduceSync(r.Context(), &kgo.Record{
			Topic: shareTopic,
			Key:   kafka.ConvertAnyToBytes(message.ID),
			Value: payload,
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintf(w, "published %d share messages\n", shareMessagesCount)
}

func produceShareErrorHandler(w http.ResponseWriter, r *http.Request) {
	msg := Message{ID: 888}

	payload, err := json.Marshal(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := producer.ProduceSync(r.Context(), &kgo.Record{
		Topic: shareTopic,
		Key:   kafka.ConvertAnyToBytes(msg.ID),
		Value: payload,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintln(w, "share error message published")
}

func produceShareBatchHandler(w http.ResponseWriter, r *http.Request) {
	var msg Message

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	records := make([]*kgo.Record, 0, shareBatchMessagesCount)

	for i := 0; i < shareBatchMessagesCount; i++ {
		message := Message{
			ID: msg.ID + i,
		}

		payload, err := json.Marshal(&message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		records = append(records, &kgo.Record{
			Topic: shareBatchTopic,
			Key:   kafka.ConvertAnyToBytes(message.ID),
			Value: payload,
		})
	}

	if err := producer.ProduceSync(r.Context(), records...); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintf(w, "published %d share batch messages\n", len(records))
}

func main() {
	ctx := context.Background()

	var err error

	producer, err = kafka.NewProducer(
		kafka.WithConfig(&kafka.Config{
			Brokers: brokers,
		}),
		kafka.WithProducerClientID("share-producer"),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer producer.Close(ctx)

	for i := 1; i <= shareConsumersCount; i++ {
		shareConsumer, err := newShareConsumer(i)
		if err != nil {
			log.Fatalln(err)
		}

		if err := shareConsumer.PreRun(ctx); err != nil {
			log.Fatalln(err)
		}

		go func() {
			if err := shareConsumer.Run(ctx); err != nil {
				log.Fatalln(err)
			}
		}()

		shareConsumers = append(shareConsumers, shareConsumer)
	}

	defer func() {
		for _, shareConsumer := range shareConsumers {
			shareConsumer.Shutdown(ctx)
		}
	}()

	for i := 1; i <= shareBatchConsumersCount; i++ {
		shareBatchConsumer, err := newShareBatchConsumer(i)
		if err != nil {
			log.Fatalln(err)
		}

		if err := shareBatchConsumer.PreRun(ctx); err != nil {
			log.Fatalln(err)
		}

		go func() {
			if err := shareBatchConsumer.Run(ctx); err != nil {
				log.Fatalln(err)
			}
		}()

		shareBatchConsumers = append(shareBatchConsumers, shareBatchConsumer)
	}

	defer func() {
		for _, shareBatchConsumer := range shareBatchConsumers {
			shareBatchConsumer.Shutdown(ctx)
		}
	}()

	http.HandleFunc("/share", produceShareHandler)
	http.HandleFunc("/share-error", produceShareErrorHandler)
	http.HandleFunc("/share-batch", produceShareBatchHandler)
	http.Handle("/metrics", promhttp.Handler())

	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
		log.Fatalln("unable to start web server:", err)
	}
}

func newShareConsumer(index int) (*kafka.Consumer, error) {
	clientID := fmt.Sprintf("sample-share-client-%d", index)

	return kafka.NewConsumer(
		kafka.WithConfig(&kafka.Config{
			Enabled: true,
			Brokers: brokers,
			Topics:  shareTopic,

			ShareGroup: shareGroup,

			ShareReleaseTimeout:        shareReleaseTimeout,
			ShareRejectAfterDeliveries: shareRejectAfterDeliveries,
		}),
		kafka.WithConsumerClientID(clientID),
		kafka.WithConsumerShareHandler(func(_ context.Context, record *kgo.Record) error {
			var msg Message
			if err := json.Unmarshal(record.Value, &msg); err != nil {
				return err
			}

			time.Sleep(defaultShareHandlerProcessingDelay)

			fmt.Printf(
				"share consume: client_id=%s, topic=%s, partition=%d, offset=%d, delivery_count=%d, msg=%+v\n",
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

			return nil
		}),
	)
}

func newShareBatchConsumer(index int) (*kafka.Consumer, error) {
	clientID := fmt.Sprintf("sample-share-batch-client-%d", index)

	return kafka.NewConsumer(
		kafka.WithConfig(&kafka.Config{
			Enabled: true,
			Brokers: brokers,
			Topics:  shareBatchTopic,

			ShareGroup: shareBatchGroup,

			ShareMaxRecords:            shareBatchMaxRecords,
			ShareRejectAfterDeliveries: shareRejectAfterDeliveries,
			ShareReleaseTimeout:        shareReleaseTimeout,
		}),
		kafka.WithConsumerClientID(clientID),
		kafka.WithConsumerShareBatchHandler(func(_ context.Context, records []*kgo.Record) error {
			fmt.Printf(
				"share batch consume: client_id=%s, records=%d\n",
				clientID,
				len(records),
			)

			time.Sleep(defaultBatchHandlerProcessingDelay)

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
