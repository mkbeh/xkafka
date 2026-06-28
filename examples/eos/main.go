package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	kafka "github.com/mkbeh/xkafka"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultMessagesCount = 10
)

var (
	inputProducer  *kafka.Producer
	txSession      *kafka.GroupTransactSession
	outputConsumer *kafka.Consumer
)

var (
	brokers         string
	inputTopic      string
	outputTopic     string
	group           string
	outputGroup     string
	transactionalID string
	messagesCount   int
)

func init() {
	brokers = os.Getenv("KAFKA_BROKERS")

	inputTopic = getenv("KAFKA_GROUP_TX_INPUT_TOPIC", "sample-group-tx-input-topic")
	outputTopic = getenv("KAFKA_GROUP_TX_OUTPUT_TOPIC", "sample-group-tx-output-topic")
	group = getenv("KAFKA_GROUP_TX_GROUP", "sample-group-tx-group")
	outputGroup = getenv("KAFKA_GROUP_TX_OUTPUT_GROUP", "sample-group-tx-output-group")
	transactionalID = getenv("KAFKA_GROUP_TX_TRANSACTIONAL_ID", "sample-group-tx-session")
	messagesCount = getenvInt("KAFKA_GROUP_TX_MESSAGES", defaultMessagesCount)
}

type InputMessage struct {
	ID int `json:"id"`
}

type OutputMessage struct {
	ID        int    `json:"id"`
	Source    string `json:"source"`
	Processed bool   `json:"processed"`
}

func produceInputHandler(w http.ResponseWriter, r *http.Request) {
	var msg InputMessage

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	records := make([]*kgo.Record, 0, messagesCount)

	for i := 0; i < messagesCount; i++ {
		message := InputMessage{
			ID: msg.ID + i,
		}

		payload, err := json.Marshal(&message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		records = append(records, &kgo.Record{
			Topic: inputTopic,
			Key:   kafka.ConvertAnyToBytes(message.ID),
			Value: payload,
		})
	}

	if err := inputProducer.ProduceSync(r.Context(), records...); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintf(w, "published %d input messages\n", len(records))
}

func produceInputErrorHandler(w http.ResponseWriter, r *http.Request) {
	msg := InputMessage{ID: 888}

	payload, err := json.Marshal(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := inputProducer.ProduceSync(r.Context(), &kgo.Record{
		Topic: inputTopic,
		Key:   kafka.ConvertAnyToBytes(msg.ID),
		Value: payload,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintln(w, "group transaction error message published")
}

func main() {
	ctx := context.Background()

	var err error

	inputProducer, err = kafka.NewProducer(
		kafka.WithConfig(&kafka.Config{
			Brokers: brokers,
		}),
		kafka.WithProducerClientID("group-tx-input-producer"),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer inputProducer.Close(ctx)

	txSession, err = kafka.NewGroupTransactSession(
		kafka.WithConfig(&kafka.Config{
			Enabled: true,
			Brokers: brokers,
			Topics:  inputTopic,
			Group:   group,

			DefaultProduceTopic: outputTopic,
			TransactionalID:     transactionalID,

			MaxPollRecords: messagesCount,
		}),
		kafka.WithGroupTransactSessionClientID("group-tx-session"),
		kafka.WithGroupTransactSessionFetchIsolationLevel(kgo.ReadCommitted()),
		kafka.WithGroupTransactSessionHandler(func(
			ctx context.Context,
			records []*kgo.Record,
			session *kafka.GroupTransactSession,
		) error {
			fmt.Printf("group tx consume batch: records=%d\n", len(records))

			for _, record := range records {
				var input InputMessage
				if err := json.Unmarshal(record.Value, &input); err != nil {
					return err
				}

				if input.ID == 888 {
					return fmt.Errorf("forced group transaction handler error")
				}

				output := OutputMessage{
					ID:        input.ID,
					Source:    record.Topic,
					Processed: true,
				}

				payload, err := json.Marshal(&output)
				if err != nil {
					return err
				}

				if err := session.ProduceSync(ctx, &kgo.Record{
					Topic: outputTopic,
					Key:   record.Key,
					Value: payload,
				}); err != nil {
					return err
				}
			}

			return nil
		}),
	)
	if err != nil {
		log.Fatalln(err)
	}

	if err := txSession.PreRun(ctx); err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := txSession.Shutdown(ctx); err != nil {
			log.Println("group transact session shutdown error:", err)
		}
	}()

	outputConsumer, err = kafka.NewConsumer(
		kafka.WithConfig(&kafka.Config{
			Enabled: true,
			Brokers: brokers,
			Topics:  outputTopic,
			Group:   outputGroup,
		}),
		kafka.WithConsumerClientID("group-tx-output-consumer"),
		kafka.WithFetchIsolationLevel(kgo.ReadCommitted()),
		kafka.WithConsumerHandler(func(_ context.Context, record *kgo.Record) error {
			var msg OutputMessage
			if err := json.Unmarshal(record.Value, &msg); err != nil {
				return err
			}

			fmt.Printf("output consume: topic=%s, partition=%d, offset=%d, msg=%+v\n",
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

	if err := outputConsumer.PreRun(ctx); err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := outputConsumer.Shutdown(ctx); err != nil {
			log.Println("output consumer shutdown error:", err)
		}
	}()

	go func() {
		if err := txSession.Run(ctx); err != nil {
			log.Fatalln(err)
		}
	}()

	go func() {
		if err := outputConsumer.Run(ctx); err != nil {
			log.Fatalln(err)
		}
	}()

	http.HandleFunc("/group-tx", produceInputHandler)
	http.HandleFunc("/group-tx-error", produceInputErrorHandler)
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

func getenvInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("invalid %s: %v", name, err)
	}

	if parsed < 0 {
		log.Fatalf("%s must be greater than or equal to zero", name)
	}

	return parsed
}
