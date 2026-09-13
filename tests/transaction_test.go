package xkafka_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type runInTxContextKey struct{}

type runInTxEndEvent struct {
	transactionType TransactionType
	outcome         TransactionOutcome
	contextValue    string
	duration        time.Duration
	err             error
}

type runInTxHookState struct {
	startCalls int
	startTypes []TransactionType
	endEvents  []runInTxEndEvent
}

type runInTxHook struct {
	mu    sync.Mutex
	state runInTxHookState
}

func (h *runInTxHook) OnTransactionStart(
	ctx context.Context,
	transactionType TransactionType,
) context.Context {
	h.mu.Lock()
	h.state.startCalls++
	h.state.startTypes = append(h.state.startTypes, transactionType)
	h.mu.Unlock()

	return context.WithValue(ctx, runInTxContextKey{}, "transaction-context")
}

func (h *runInTxHook) OnTransactionEnd(
	ctx context.Context,
	transactionType TransactionType,
	outcome TransactionOutcome,
	duration time.Duration,
	err error,
) {
	contextValue, _ := ctx.Value(runInTxContextKey{}).(string)

	h.mu.Lock()
	defer h.mu.Unlock()

	h.state.endEvents = append(h.state.endEvents, runInTxEndEvent{
		transactionType: transactionType,
		outcome:         outcome,
		contextValue:    contextValue,
		duration:        duration,
		err:             err,
	})
}

func (h *runInTxHook) snapshot() runInTxHookState {
	h.mu.Lock()
	defer h.mu.Unlock()

	state := h.state
	state.startTypes = append([]TransactionType(nil), h.state.startTypes...)
	state.endEvents = append([]runInTxEndEvent(nil), h.state.endEvents...)

	return state
}

func TestClientRunInTxCommit(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		produce func(context.Context, *Tx, *kgo.Record) error
	}{
		{
			name:  "Produce",
			topic: "run-in-tx-produce",
			produce: func(ctx context.Context, tx *Tx, record *kgo.Record) error {
				tx.Produce(ctx, record, nil)
				return nil
			},
		},
		{
			name:  "TryProduce",
			topic: "run-in-tx-try-produce",
			produce: func(ctx context.Context, tx *Tx, record *kgo.Record) error {
				tx.TryProduce(ctx, record, nil)
				return nil
			},
		},
		{
			name:  "ProduceSync",
			topic: "run-in-tx-produce-sync",
			produce: func(ctx context.Context, tx *Tx, record *kgo.Record) error {
				return tx.ProduceSync(ctx, record)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := newTestKafkaCluster(t, tt.topic)
			hook := &runInTxHook{}
			client := newTestClient(
				t,
				cluster,
				WithKafkaOptions(kgo.TransactionalID("xkafka-"+tt.topic)),
				WithHooks(hook),
			)

			record := &kgo.Record{
				Topic: tt.topic,
				Key:   []byte("order-1"),
				Value: []byte("created"),
			}

			err := client.RunInTx(context.Background(), func(ctx context.Context, tx *Tx) error {
				if got, _ := ctx.Value(runInTxContextKey{}).(string); got != "transaction-context" {
					return fmt.Errorf("transaction context = %q, want transaction-context", got)
				}
				if tx == nil {
					return errors.New("transaction is nil")
				}

				return tt.produce(ctx, tx, record)
			})
			if err != nil {
				t.Fatalf("run transaction: %v", err)
			}

			state := hook.snapshot()
			if state.startCalls != 1 {
				t.Fatalf("transaction start calls = %d, want 1", state.startCalls)
			}
			if want := []TransactionType{TransactionTypeProducer}; !reflect.DeepEqual(state.startTypes, want) {
				t.Fatalf("transaction start types = %v, want %v", state.startTypes, want)
			}
			if len(state.endEvents) != 1 {
				t.Fatalf("transaction end calls = %d, want 1", len(state.endEvents))
			}

			end := state.endEvents[0]
			if end.transactionType != TransactionTypeProducer {
				t.Fatalf("transaction end type = %q, want %q", end.transactionType, TransactionTypeProducer)
			}
			if end.outcome != TransactionOutcomeCommit {
				t.Fatalf("transaction outcome = %q, want %q", end.outcome, TransactionOutcomeCommit)
			}
			if end.contextValue != "transaction-context" {
				t.Fatalf("transaction end context = %q, want transaction-context", end.contextValue)
			}
			if end.duration < 0 {
				t.Fatalf("transaction duration = %s, want non-negative", end.duration)
			}
			if end.err != nil {
				t.Fatalf("transaction end error = %v, want nil", end.err)
			}

			consumed := consumeCommittedTestRecords(t, cluster, tt.topic, 1)[0]
			if string(consumed.Key) != "order-1" {
				t.Fatalf("committed record key = %q, want order-1", consumed.Key)
			}
			if string(consumed.Value) != "created" {
				t.Fatalf("committed record value = %q, want created", consumed.Value)
			}
		})
	}
}

func TestClientRunInTxAbortOnError(t *testing.T) {
	const topic = "run-in-tx-abort-error"

	cluster := newTestKafkaCluster(t, topic)
	hook := &runInTxHook{}
	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(kgo.TransactionalID("xkafka-run-in-tx-abort-error")),
		WithHooks(hook),
	)

	handleErr := errors.New("transaction failed")
	err := client.RunInTx(context.Background(), func(ctx context.Context, tx *Tx) error {
		if err := tx.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Value: []byte("aborted"),
		}); err != nil {
			return err
		}

		return handleErr
	})
	if !errors.Is(err, handleErr) {
		t.Fatalf("run transaction error = %v, want %v", err, handleErr)
	}

	state := hook.snapshot()
	if state.startCalls != 1 {
		t.Fatalf("transaction start calls = %d, want 1", state.startCalls)
	}
	if len(state.endEvents) != 1 {
		t.Fatalf("transaction end calls = %d, want 1", len(state.endEvents))
	}
	end := state.endEvents[0]
	if end.outcome != TransactionOutcomeAbort {
		t.Fatalf("transaction outcome = %q, want %q", end.outcome, TransactionOutcomeAbort)
	}
	if !errors.Is(end.err, handleErr) {
		t.Fatalf("transaction end error = %v, want %v", end.err, handleErr)
	}

	assertNoCommittedTestRecords(t, cluster, topic)

	if err := client.RunInTx(context.Background(), func(ctx context.Context, tx *Tx) error {
		return tx.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Value: []byte("committed-after-abort"),
		})
	}); err != nil {
		t.Fatalf("run transaction after abort: %v", err)
	}

	consumed := consumeCommittedTestRecords(t, cluster, topic, 1)[0]
	if string(consumed.Value) != "committed-after-abort" {
		t.Fatalf("committed record value = %q, want committed-after-abort", consumed.Value)
	}
}

func TestClientRunInTxPanic(t *testing.T) {
	const topic = "run-in-tx-panic"

	cluster := newTestKafkaCluster(t, topic)
	hook := &runInTxHook{}
	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(kgo.TransactionalID("xkafka-run-in-tx-panic")),
		WithHooks(hook),
	)

	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()

		_ = client.RunInTx(context.Background(), func(ctx context.Context, tx *Tx) error {
			if err := tx.ProduceSync(ctx, &kgo.Record{
				Topic: topic,
				Value: []byte("aborted-panic"),
			}); err != nil {
				return err
			}

			panic("boom")
		})
	}()

	if recovered != "boom" {
		t.Fatalf("recovered panic = %#v, want %q", recovered, "boom")
	}

	state := hook.snapshot()
	if state.startCalls != 1 {
		t.Fatalf("transaction start calls = %d, want 1", state.startCalls)
	}
	if len(state.endEvents) != 1 {
		t.Fatalf("transaction end calls = %d, want 1", len(state.endEvents))
	}
	end := state.endEvents[0]
	if end.outcome != TransactionOutcomeAbort {
		t.Fatalf("transaction outcome = %q, want %q", end.outcome, TransactionOutcomeAbort)
	}
	if end.err == nil || !strings.Contains(end.err.Error(), "kafka: transaction panic: boom") {
		t.Fatalf("transaction end error = %v, want recovered panic error", end.err)
	}

	assertNoCommittedTestRecords(t, cluster, topic)
}

func TestClientRunInTxBeginError(t *testing.T) {
	const topic = "run-in-tx-begin-error"

	cluster := newTestKafkaCluster(t, topic)
	hook := &runInTxHook{}
	client := newTestClient(t, cluster, WithHooks(hook))

	called := false
	err := client.RunInTx(context.Background(), func(context.Context, *Tx) error {
		called = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "kafka: begin transaction") {
		t.Fatalf("run transaction error = %v, want begin transaction error", err)
	}
	if called {
		t.Fatal("transaction function called after begin error")
	}

	state := hook.snapshot()
	if state.startCalls != 1 {
		t.Fatalf("transaction start calls = %d, want 1", state.startCalls)
	}
	if len(state.endEvents) != 1 {
		t.Fatalf("transaction end calls = %d, want 1", len(state.endEvents))
	}
	end := state.endEvents[0]
	if end.outcome != TransactionOutcomeError {
		t.Fatalf("transaction outcome = %q, want %q", end.outcome, TransactionOutcomeError)
	}
	if end.err == nil || !strings.Contains(end.err.Error(), "kafka: begin transaction") {
		t.Fatalf("transaction end error = %v, want begin transaction error", end.err)
	}
}

func TestClientRunInTxCommitErrorRecovers(t *testing.T) {
	const topic = "run-in-tx-commit-error"

	cluster := newTestKafkaCluster(t, topic)
	hook := &runInTxHook{}
	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(
			kgo.TransactionalID("xkafka-run-in-tx-commit-error"),
			kgo.RequestRetries(0),
		),
		WithHooks(hook),
	)

	failNextEndTxn(cluster)

	err := client.RunInTx(context.Background(), func(ctx context.Context, tx *Tx) error {
		return tx.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Value: []byte("must-abort"),
		})
	})
	if err == nil || !strings.Contains(err.Error(), "kafka: commit transaction") {
		t.Fatalf("run transaction error = %v, want commit transaction error", err)
	}

	state := hook.snapshot()
	if state.startCalls != 1 {
		t.Fatalf("transaction start calls = %d, want 1", state.startCalls)
	}
	if len(state.endEvents) != 1 {
		t.Fatalf("transaction end calls = %d, want 1", len(state.endEvents))
	}
	end := state.endEvents[0]
	if end.outcome != TransactionOutcomeError {
		t.Fatalf("transaction outcome = %q, want %q", end.outcome, TransactionOutcomeError)
	}
	if end.err == nil || !strings.Contains(end.err.Error(), "kafka: commit transaction") {
		t.Fatalf("transaction end error = %v, want commit transaction error", end.err)
	}

	assertNoCommittedTestRecords(t, cluster, topic)

	if err := client.RunInTx(context.Background(), func(ctx context.Context, tx *Tx) error {
		return tx.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Value: []byte("committed-after-recovery"),
		})
	}); err != nil {
		t.Fatalf("run transaction after commit recovery: %v", err)
	}

	consumed := consumeCommittedTestRecords(t, cluster, topic, 1)[0]
	if got := string(consumed.Value); got != "committed-after-recovery" {
		t.Fatalf("committed record value = %q, want committed-after-recovery", got)
	}
}

func TestClientRunInTxNilFunc(t *testing.T) {
	cluster := newTestKafkaCluster(t, "run-in-tx-nil-func")
	hook := &runInTxHook{}
	client := newTestClient(t, cluster, WithHooks(hook))

	err := client.RunInTx(context.Background(), nil)
	if err == nil || err.Error() != "kafka: transaction function is nil" {
		t.Fatalf("run transaction error = %v, want nil function error", err)
	}

	state := hook.snapshot()
	if state.startCalls != 0 {
		t.Fatalf("transaction start calls = %d, want 0", state.startCalls)
	}
	if len(state.endEvents) != 0 {
		t.Fatalf("transaction end calls = %d, want 0", len(state.endEvents))
	}
}
