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

const defaultMessagesCount = 10

var (
	inputProducer  *xkafka.Client
	txSession      *xkafka.GroupTransactSession
	outputConsumer *xkafka.Client
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

	inputTopic = getenv("KAFKA_EOS_INPUT_TOPIC", "sample-eos-input-topic")
	outputTopic = getenv("KAFKA_EOS_OUTPUT_TOPIC", "sample-eos-output-topic")
	group = getenv("KAFKA_EOS_GROUP", "sample-eos-group")
	outputGroup = getenv("KAFKA_EOS_OUTPUT_GROUP", "sample-eos-output-group")
	transactionalID = getenv("KAFKA_EOS_TRANSACTIONAL_ID", "sample-eos-session")
	messagesCount = getenvInt("KAFKA_EOS_MESSAGES", defaultMessagesCount)
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
			Key:   []byte(strconv.Itoa(message.ID)),
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
	records := make([]*kgo.Record, 0, 2)

	messages := []InputMessage{
		{ID: 777},
		{ID: 888},
	}

	for _, msg := range messages {
		payload, err := json.Marshal(&msg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		records = append(records, &kgo.Record{
			Topic: inputTopic,
			Key:   []byte("eos-error-flow"),
			Value: payload,
		})
	}

	if err := inputProducer.ProduceSync(r.Context(), records...); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintln(w, "published one successful input message and one failing input message")
}

func produceInputPanicHandler(w http.ResponseWriter, r *http.Request) {
	msg := InputMessage{ID: 444}

	payload, err := json.Marshal(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := inputProducer.ProduceSync(r.Context(), &kgo.Record{
		Topic: inputTopic,
		Key:   []byte(strconv.Itoa(msg.ID)),
		Value: payload,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintln(w, "published input message that triggers handler panic")
}

func main() {
	ctx := context.Background()

	var err error

	inputProducer, err = xkafka.NewClient(
		xkafka.WithConfig(&xkafka.Config{
			Brokers: brokers,
		}),
		xkafka.WithClientID("eos-input-producer"),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer inputProducer.Shutdown(ctx)

	txSession, err = newGroupTransactSession()
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := txSession.Shutdown(ctx); err != nil {
			log.Println("group transact session shutdown error:", err)
		}
	}()

	outputConsumer, err = newOutputConsumer()
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := outputConsumer.Shutdown(ctx); err != nil {
			log.Println("output consumer shutdown error:", err)
		}
	}()

	go func() {
		if err := txSession.HandleFetches(ctx); err != nil {
			log.Fatalln(err)
		}
	}()

	go func() {
		if err := outputConsumer.HandleFetches(ctx); err != nil {
			log.Fatalln(err)
		}
	}()

	http.HandleFunc("/group-tx", produceInputHandler)
	http.HandleFunc("/group-tx-error", produceInputErrorHandler)
	http.HandleFunc("/group-tx-panic", produceInputPanicHandler)
	http.Handle("/metrics", promhttp.Handler())

	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
		log.Fatalln("unable to start web server:", err)
	}
}

func newGroupTransactSession() (*xkafka.GroupTransactSession, error) {
	return xkafka.NewGroupTransactSession(
		xkafka.WithConfig(&xkafka.Config{
			Enabled: true,
			Brokers: brokers,
			Topics:  inputTopic,
			Group:   group,

			DefaultProduceTopic: outputTopic,
			TransactionalID:     transactionalID,

			MaxPollRecords: messagesCount,
		}),
		xkafka.WithClientID("eos-session"),
		xkafka.WithFetchIsolationLevel(kgo.ReadCommitted()),
		xkafka.WithGroupTransactSessionBatchHandler(func(
			ctx context.Context,
			records []*kgo.Record,
			tx xkafka.Tx,
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

				if input.ID == 444 {
					panic("forced group transaction handler panic")
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

				if err := tx.ProduceSync(ctx, &kgo.Record{
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
}

func newOutputConsumer() (*xkafka.Client, error) {
	return xkafka.NewClient(
		xkafka.WithConfig(&xkafka.Config{
			Enabled: true,
			Brokers: brokers,
			Topics:  outputTopic,
			Group:   outputGroup,
		}),
		xkafka.WithClientID("eos-output-consumer"),
		xkafka.WithFetchIsolationLevel(kgo.ReadCommitted()),
		xkafka.WithConsumerBatchHandler(func(_ context.Context, records []*kgo.Record) error {
			for _, record := range records {
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
