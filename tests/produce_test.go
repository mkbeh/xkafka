package xkafka_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const testProduceHeader = "x-xkafka-test-context"

type testProduceContextKey struct{}

type testProduceError struct {
	record *kgo.Record
	err    error
}

type testProduceHookState struct {
	startCalls     int
	startRecords   []*kgo.Record
	recordContexts []string
	endCalls       int
	endRecords     []*kgo.Record
	endContext     string
	endErr         error
	errors         []testProduceError
}

type testProduceHook struct {
	mu    sync.Mutex
	state testProduceHookState
}

func (h *testProduceHook) OnProduceStart(
	ctx context.Context,
	records []*kgo.Record,
) context.Context {
	h.mu.Lock()
	h.state.startCalls++
	h.state.startRecords = append([]*kgo.Record(nil), records...)
	h.mu.Unlock()

	return context.WithValue(ctx, testProduceContextKey{}, "sync-context")
}

func (h *testProduceHook) OnProduceRecord(ctx context.Context, record *kgo.Record) {
	contextValue, _ := ctx.Value(testProduceContextKey{}).(string)

	h.mu.Lock()
	h.state.recordContexts = append(h.state.recordContexts, contextValue)
	h.mu.Unlock()

	record.Headers = append(record.Headers, kgo.RecordHeader{
		Key:   testProduceHeader,
		Value: []byte(contextValue),
	})
}

func (h *testProduceHook) OnProduceEnd(
	ctx context.Context,
	records []*kgo.Record,
	_ time.Duration,
	err error,
) {
	contextValue, _ := ctx.Value(testProduceContextKey{}).(string)

	h.mu.Lock()
	defer h.mu.Unlock()

	h.state.endCalls++
	h.state.endRecords = append([]*kgo.Record(nil), records...)
	h.state.endContext = contextValue
	h.state.endErr = err
}

func (h *testProduceHook) OnProduceError(record *kgo.Record, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.state.errors = append(h.state.errors, testProduceError{
		record: record,
		err:    err,
	})
}

func (h *testProduceHook) snapshot() testProduceHookState {
	h.mu.Lock()
	defer h.mu.Unlock()

	state := h.state
	state.startRecords = append([]*kgo.Record(nil), h.state.startRecords...)
	state.recordContexts = append([]string(nil), h.state.recordContexts...)
	state.endRecords = append([]*kgo.Record(nil), h.state.endRecords...)
	state.errors = append([]testProduceError(nil), h.state.errors...)

	return state
}

func TestClientAsyncProduce(t *testing.T) {
	tests := []struct {
		name    string
		produce func(*Client, context.Context, *kgo.Record, PromiseFunc)
	}{
		{
			name:    "Produce",
			produce: (*Client).Produce,
		},
		{
			name:    "TryProduce",
			produce: (*Client).TryProduce,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const topic = "async-produce"

			cluster := newTestKafkaCluster(t, topic)
			hook := &testProduceHook{}
			promiseCh := make(chan testProduceResult, 1)

			client := newTestClient(
				t,
				cluster,
				WithHooks(hook),
				WithProducePromise(func(record *kgo.Record, err error) {
					promiseCh <- testProduceResult{record: record, err: err}
				}),
			)

			callCtx := context.WithValue(
				context.Background(),
				testProduceContextKey{},
				"call-context",
			)
			recordCtx := context.WithValue(
				context.Background(),
				testProduceContextKey{},
				"record-context",
			)
			record := &kgo.Record{
				Topic:   topic,
				Key:     []byte("order-1"),
				Value:   []byte("created"),
				Context: recordCtx,
			}

			tt.produce(client, callCtx, record, nil)

			result := waitTestProduceResult(t, promiseCh)
			if result.err != nil {
				t.Fatalf("produce error = %v, want nil", result.err)
			}
			if result.record != record {
				t.Fatalf("promise record = %p, want %p", result.record, record)
			}

			state := hook.snapshot()
			if state.startCalls != 0 {
				t.Fatalf("produce start calls = %d, want 0", state.startCalls)
			}
			if want := []string{"record-context"}; !reflect.DeepEqual(state.recordContexts, want) {
				t.Fatalf("record hook contexts = %v, want %v", state.recordContexts, want)
			}
			if state.endCalls != 0 {
				t.Fatalf("produce end calls = %d, want 0", state.endCalls)
			}
			if len(state.errors) != 0 {
				t.Fatalf("produce error calls = %d, want 0", len(state.errors))
			}

			consumed := consumeTestRecords(t, cluster, topic, 1)[0]
			if string(consumed.Key) != "order-1" {
				t.Fatalf("record key = %q, want order-1", consumed.Key)
			}
			if string(consumed.Value) != "created" {
				t.Fatalf("record value = %q, want created", consumed.Value)
			}
			if got := recordHeaderValue(consumed, testProduceHeader); got != "record-context" {
				t.Fatalf("record hook header = %q, want record-context", got)
			}
		})
	}
}

func TestClientTryProduceMaxBuffered(t *testing.T) {
	const topic = "try-produce-full"

	cluster := newTestKafkaCluster(t, topic)
	events := make(chan string, 2)

	hook := testProduceErrorHookFunc(func(_ *kgo.Record, _ error) {
		events <- "hook"
	})

	client := newTestClient(
		t,
		cluster,
		WithHooks(hook),
		WithKafkaOptions(
			kgo.MaxBufferedRecords(1),
			kgo.ManualFlushing(),
		),
	)

	client.Produce(context.Background(), &kgo.Record{
		Topic: topic,
		Value: []byte("first"),
	}, nil)

	promiseCh := make(chan testProduceResult, 1)
	second := &kgo.Record{
		Topic: topic,
		Value: []byte("second"),
	}
	client.TryProduce(context.Background(), second, func(record *kgo.Record, err error) {
		events <- "promise"
		promiseCh <- testProduceResult{record: record, err: err}
	})

	result := waitTestProduceResult(t, promiseCh)
	if result.record != second {
		t.Fatalf("promise record = %p, want %p", result.record, second)
	}
	if !errors.Is(result.err, kgo.ErrMaxBuffered) {
		t.Fatalf("produce error = %v, want %v", result.err, kgo.ErrMaxBuffered)
	}

	if got := <-events; got != "hook" {
		t.Fatalf("first callback event = %q, want hook", got)
	}
	if got := <-events; got != "promise" {
		t.Fatalf("second callback event = %q, want promise", got)
	}
}

func TestClientProduceSync(t *testing.T) {
	const topic = "sync-produce"

	cluster := newTestKafkaCluster(t, topic)
	hook := &testProduceHook{}
	client := newTestClient(t, cluster, WithHooks(hook))

	records := []*kgo.Record{
		{Topic: topic, Key: []byte("order-1"), Value: []byte("created")},
		{Topic: topic, Key: []byte("order-2"), Value: []byte("paid")},
	}

	ctx := context.WithValue(
		context.Background(),
		testProduceContextKey{},
		"caller-context",
	)
	if err := client.ProduceSync(ctx, records...); err != nil {
		t.Fatalf("produce sync: %v", err)
	}

	state := hook.snapshot()
	if state.startCalls != 1 {
		t.Fatalf("produce start calls = %d, want 1", state.startCalls)
	}
	if !reflect.DeepEqual(state.startRecords, records) {
		t.Fatal("produce start records differ")
	}
	if want := []string{"sync-context", "sync-context"}; !reflect.DeepEqual(state.recordContexts, want) {
		t.Fatalf("record hook contexts = %v, want %v", state.recordContexts, want)
	}
	if state.endCalls != 1 {
		t.Fatalf("produce end calls = %d, want 1", state.endCalls)
	}
	if !reflect.DeepEqual(state.endRecords, records) {
		t.Fatal("produce end records differ")
	}
	if state.endContext != "sync-context" {
		t.Fatalf("produce end context = %q, want sync-context", state.endContext)
	}
	if state.endErr != nil {
		t.Fatalf("produce end error = %v, want nil", state.endErr)
	}
	if len(state.errors) != 0 {
		t.Fatalf("produce error calls = %d, want 0", len(state.errors))
	}

	consumed := consumeTestRecords(t, cluster, topic, len(records))
	for i, record := range consumed {
		if !bytes.Equal(record.Value, records[i].Value) {
			t.Fatalf("record %d value = %q, want %q", i, record.Value, records[i].Value)
		}
		if got := recordHeaderValue(record, testProduceHeader); got != "sync-context" {
			t.Fatalf("record %d hook header = %q, want sync-context", i, got)
		}
	}
}

func TestClientProduceSyncError(t *testing.T) {
	const topic = "sync-produce-error"

	cluster := newTestKafkaCluster(t, topic)
	hook := &testProduceHook{}
	client := newTestClient(
		t,
		cluster,
		WithHooks(hook),
		WithKafkaOptions(kgo.MaxBufferedBytes(1)),
	)

	records := []*kgo.Record{
		{Topic: topic, Value: []byte("first")},
		{Topic: topic, Value: []byte("second")},
	}

	err := client.ProduceSync(context.Background(), records...)
	if !errors.Is(err, kerr.MessageTooLarge) {
		t.Fatalf("produce sync error = %v, want %v", err, kerr.MessageTooLarge)
	}

	state := hook.snapshot()
	if state.startCalls != 1 {
		t.Fatalf("produce start calls = %d, want 1", state.startCalls)
	}
	if state.endCalls != 1 {
		t.Fatalf("produce end calls = %d, want 1", state.endCalls)
	}
	if want := []string{"sync-context", "sync-context"}; !reflect.DeepEqual(state.recordContexts, want) {
		t.Fatalf("record hook contexts = %v, want %v", state.recordContexts, want)
	}
	if state.endContext != "sync-context" {
		t.Fatalf("produce end context = %q, want sync-context", state.endContext)
	}
	if !errors.Is(state.endErr, kerr.MessageTooLarge) {
		t.Fatalf("produce end error = %v, want %v", state.endErr, kerr.MessageTooLarge)
	}
	if len(state.errors) != len(records) {
		t.Fatalf("produce error calls = %d, want %d", len(state.errors), len(records))
	}
	for i, produceErr := range state.errors {
		if produceErr.record != records[i] {
			t.Fatalf("produce error record %d = %p, want %p", i, produceErr.record, records[i])
		}
		if !errors.Is(produceErr.err, kerr.MessageTooLarge) {
			t.Fatalf("produce error %d = %v, want %v", i, produceErr.err, kerr.MessageTooLarge)
		}
	}
}

type testProduceErrorHookFunc func(record *kgo.Record, err error)

func (fn testProduceErrorHookFunc) OnProduceError(record *kgo.Record, err error) {
	fn(record, err)
}
