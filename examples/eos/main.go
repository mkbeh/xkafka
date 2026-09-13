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
	inputTopic      = "sample-eos-input-topic"
	outputTopic     = "sample-eos-output-topic"
	group           = "sample-eos-group"
	outputGroup     = "sample-eos-output-group"
	transactionalID = "sample-eos-session"
	httpAddr        = "localhost:8080"
	messageCount    = 5
	pollInterval    = 100 * time.Millisecond

	forcedHandlerErrorID = 888
	forcedHandlerPanicID = 444
)

type inputMessage struct {
	ID int `json:"id"`
}

type outputMessage struct {
	ID      int    `json:"id"`
	Source  string `json:"source"`
	Attempt int    `json:"attempt"`
}

type recordID struct {
	topic     string
	partition int32
	offset    int64
}

type processor struct {
	attempts map[recordID]int
}

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	inputProducer, err := xkafka.NewClient(
		xkafka.WithName("eos-input-producer"),
		xkafka.WithKafkaOptions(
			kgo.SeedBrokers(brokers),
			kgo.DefaultProduceTopic(inputTopic),
		),
	)
	if err != nil {
		return fmt.Errorf("create input producer: %w", err)
	}
	defer shutdownClient("input producer", inputProducer)

	handler := &processor{
		attempts: make(map[recordID]int),
	}

	session, err := xkafka.NewGroupTransactSession(
		xkafka.WithName("eos-session"),
		xkafka.WithKafkaOptions(
			kgo.SeedBrokers(brokers),
			kgo.ConsumeTopics(inputTopic),
			kgo.ConsumerGroup(group),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			kgo.DefaultProduceTopic(outputTopic),
			kgo.TransactionalID(transactionalID),
		),
		xkafka.WithMaxPollRecords(messageCount),
		xkafka.WithPollInterval(pollInterval),
		xkafka.WithSuspendProcessingTimeout(time.Second),
		xkafka.WithGroupTransactSessionBatchHandler(handler.handle),
	)
	if err != nil {
		return fmt.Errorf("create group transact session: %w", err)
	}
	defer shutdownSession("group transact session", session)

	outputConsumer, err := xkafka.NewClient(
		xkafka.WithName("eos-output-consumer"),
		xkafka.WithKafkaOptions(
			kgo.SeedBrokers(brokers),
			kgo.ConsumeTopics(outputTopic),
			kgo.ConsumerGroup(outputGroup),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		),
		xkafka.WithPollInterval(pollInterval),
		xkafka.WithBatchHandler(handleOutputRecords),
	)
	if err != nil {
		return fmt.Errorf("create output consumer: %w", err)
	}
	defer shutdownClient("output consumer", outputConsumer)

	if err := pingClient(ctx, "input producer", inputProducer); err != nil {
		return err
	}
	if err := pingSession(ctx, "group transact session", session); err != nil {
		return err
	}
	if err := pingClient(ctx, "output consumer", outputConsumer); err != nil {
		return err
	}

	errCh := make(chan error, 3)
	go func() {
		if err := session.HandleFetches(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("handle group transaction fetches: %w", err)
		}
	}()
	go func() {
		if err := outputConsumer.HandleFetches(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("handle output fetches: %w", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /eos", eosHandler(inputProducer))
	mux.HandleFunc("POST /eos-error", eosSpecialHandler(inputProducer, forcedHandlerErrorID))
	mux.HandleFunc("POST /eos-panic", eosSpecialHandler(inputProducer, forcedHandlerPanicID))

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

func (p *processor) handle(ctx context.Context, records []*kgo.Record, tx *xkafka.Tx) error {
	fmt.Printf("eos process: records=%d\n", len(records))

	for _, record := range records {
		var input inputMessage
		if err := json.Unmarshal(record.Value, &input); err != nil {
			return fmt.Errorf("decode input record: %w", err)
		}

		attempt := 1
		if input.ID == forcedHandlerErrorID || input.ID == forcedHandlerPanicID {
			attempt = p.nextAttempt(record)
		}
		output := outputMessage{
			ID:      input.ID,
			Source:  record.Topic,
			Attempt: attempt,
		}

		payload, err := json.Marshal(output)
		if err != nil {
			return fmt.Errorf("encode output record: %w", err)
		}

		if err := tx.ProduceSync(ctx, &kgo.Record{
			Key:   record.Key,
			Value: payload,
		}); err != nil {
			return fmt.Errorf("produce output record: %w", err)
		}

		fmt.Printf(
			"  input: topic=%s partition=%d offset=%d key=%q id=%d attempt=%d\n",
			record.Topic,
			record.Partition,
			record.Offset,
			record.Key,
			input.ID,
			attempt,
		)

		if attempt == 1 {
			switch input.ID {
			case forcedHandlerErrorID:
				return errors.New("forced group transaction handler error")
			case forcedHandlerPanicID:
				panic("forced group transaction handler panic")
			}
		}
	}

	return nil
}

func (p *processor) nextAttempt(record *kgo.Record) int {
	id := recordID{
		topic:     record.Topic,
		partition: record.Partition,
		offset:    record.Offset,
	}
	p.attempts[id]++

	return p.attempts[id]
}

func handleOutputRecords(_ context.Context, records []*kgo.Record) error {
	for _, record := range records {
		var msg outputMessage
		if err := json.Unmarshal(record.Value, &msg); err != nil {
			return fmt.Errorf("decode output record: %w", err)
		}

		fmt.Printf(
			"eos output: topic=%s partition=%d offset=%d key=%q msg=%+v\n",
			record.Topic,
			record.Partition,
			record.Offset,
			record.Key,
			msg,
		)
	}

	return nil
}

func eosHandler(producer *xkafka.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start, err := decodeInputMessage(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		records := make([]*kgo.Record, 0, messageCount)
		for i := range messageCount {
			record, err := newInputRecord(inputMessage{ID: start.ID + i})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			records = append(records, record)
		}

		if err := producer.ProduceSync(r.Context(), records...); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(w, "published %d EOS input records\n", len(records))
	}
}

func eosSpecialHandler(producer *xkafka.Client, id int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		record, err := newInputRecord(inputMessage{ID: id})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := producer.ProduceSync(r.Context(), record); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(w, "published EOS input record id=%d\n", id)
	}
}

func decodeInputMessage(r *http.Request) (inputMessage, error) {
	var msg inputMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		return inputMessage{}, fmt.Errorf("decode request: %w", err)
	}

	return msg, nil
}

func newInputRecord(msg inputMessage) (*kgo.Record, error) {
	payload, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("encode input message: %w", err)
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

func pingSession(ctx context.Context, name string, session *xkafka.GroupTransactSession) error {
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := session.Ping(pingCtx); err != nil {
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

func shutdownSession(name string, session *xkafka.GroupTransactSession) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := session.Shutdown(ctx); err != nil {
		log.Printf("shutdown %s: %v", name, err)
	}
}
