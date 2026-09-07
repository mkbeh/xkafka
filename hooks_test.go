package xkafka

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type hookContextKey string

type testHandleHook struct {
	name   string
	starts *[]string
	ends   *[]string
}

func (h *testHandleHook) OnHandleStart(ctx context.Context, _ []*kgo.Record) context.Context {
	*h.starts = append(*h.starts, h.name)
	return context.WithValue(ctx, hookContextKey(h.name), true)
}

func (h *testHandleHook) OnHandleEnd(
	ctx context.Context,
	_ []*kgo.Record,
	_ time.Duration,
	_ error,
) {
	if marked, _ := ctx.Value(hookContextKey(h.name)).(bool); marked {
		*h.ends = append(*h.ends, h.name)
	}
}

type testTransactionHook struct {
	name   string
	starts *[]string
	ends   *[]string
}

func (h *testTransactionHook) OnTransactionStart(
	ctx context.Context,
	_ TransactionType,
) context.Context {
	*h.starts = append(*h.starts, h.name)
	return context.WithValue(ctx, hookContextKey(h.name), true)
}

func (h *testTransactionHook) OnTransactionEnd(
	ctx context.Context,
	_ TransactionType,
	_ TransactionOutcome,
	_ time.Duration,
	_ error,
) {
	if marked, _ := ctx.Value(hookContextKey(h.name)).(bool); marked {
		*h.ends = append(*h.ends, h.name)
	}
}

type testCloseHook struct {
	clientCount  int
	sessionCount int
}

func (h *testCloseHook) OnClientClosed(*Client) {
	h.clientCount++
}

func (h *testCloseHook) OnGroupTransactSessionClosed(*GroupTransactSession) {
	h.sessionCount++
}

type testShareAckHook struct {
	calls []struct {
		outcome ShareAckOutcome
		count   int
	}
}

func (h *testShareAckHook) OnShareAck(_ context.Context, outcome ShareAckOutcome, count int) {
	h.calls = append(h.calls, struct {
		outcome ShareAckOutcome
		count   int
	}{
		outcome: outcome,
		count:   count,
	})
}

type testKafkaErrorHook struct {
	produceRecord    *kgo.Record
	produceErr       error
	fetchTopic       string
	fetchPartition   int32
	fetchRecoverable bool
	fetchErr         error
}

func (h *testKafkaErrorHook) OnProduceError(record *kgo.Record, err error) {
	h.produceRecord = record
	h.produceErr = err
}

func (h *testKafkaErrorHook) OnFetchError(
	_ context.Context,
	topic string,
	partition int32,
	recoverable bool,
	err error,
) {
	h.fetchTopic = topic
	h.fetchPartition = partition
	h.fetchRecoverable = recoverable
	h.fetchErr = err
}

type testOperationHook struct {
	offsetCommitDuration time.Duration
	offsetCommitErr      error
	shareAckDuration     time.Duration
	shareAckErr          error
}

func (h *testOperationHook) OnOffsetCommit(
	_ context.Context,
	duration time.Duration,
	err error,
) {
	h.offsetCommitDuration = duration
	h.offsetCommitErr = err
}

func (h *testOperationHook) OnShareAckFlush(
	_ context.Context,
	duration time.Duration,
	err error,
) {
	h.shareAckDuration = duration
	h.shareAckErr = err
}

func TestHooksKafkaErrorContext(t *testing.T) {
	hook := &testKafkaErrorHook{}
	hookSet := hooks{hook}

	record := &kgo.Record{Topic: "orders", Partition: 3}
	produceErr := errors.New("produce failed")
	hookSet.onProduceError(record, produceErr)

	if hook.produceRecord != record {
		t.Fatalf("produce record = %p, want %p", hook.produceRecord, record)
	}
	if !errors.Is(hook.produceErr, produceErr) {
		t.Fatalf("produce error = %v, want %v", hook.produceErr, produceErr)
	}

	fetchErr := errors.New("fetch failed")
	hookSet.onFetchError(context.Background(), "payments", 7, true, fetchErr)

	if hook.fetchTopic != "payments" {
		t.Fatalf("fetch topic = %q, want payments", hook.fetchTopic)
	}
	if hook.fetchPartition != 7 {
		t.Fatalf("fetch partition = %d, want 7", hook.fetchPartition)
	}
	if !hook.fetchRecoverable {
		t.Fatal("fetch recoverable = false, want true")
	}
	if !errors.Is(hook.fetchErr, fetchErr) {
		t.Fatalf("fetch error = %v, want %v", hook.fetchErr, fetchErr)
	}
}

func TestHooksHandleContext(t *testing.T) {
	var starts, ends []string

	first := &testHandleHook{name: "first", starts: &starts, ends: &ends}
	second := &testHandleHook{name: "second", starts: &starts, ends: &ends}
	hookSet := hooks{first, second}

	records := []*kgo.Record{{Topic: "orders"}, {Topic: "payments"}}
	ctx := hookSet.onHandleStart(context.Background(), records)
	hookSet.onHandleEnd(ctx, records, time.Second, nil)

	if want := []string{"first", "second"}; !reflect.DeepEqual(starts, want) {
		t.Fatalf("start order = %v, want %v", starts, want)
	}
	if want := []string{"first", "second"}; !reflect.DeepEqual(ends, want) {
		t.Fatalf("end order = %v, want %v", ends, want)
	}
}

func TestHooksTransactionContext(t *testing.T) {
	var starts, ends []string

	first := &testTransactionHook{name: "first", starts: &starts, ends: &ends}
	second := &testTransactionHook{name: "second", starts: &starts, ends: &ends}
	hookSet := hooks{first, second}

	ctx := hookSet.onTransactionStart(context.Background(), TransactionTypeProducer)
	hookSet.onTransactionEnd(
		ctx,
		TransactionTypeProducer,
		TransactionOutcomeCommit,
		time.Second,
		nil,
	)

	if want := []string{"first", "second"}; !reflect.DeepEqual(starts, want) {
		t.Fatalf("start order = %v, want %v", starts, want)
	}
	if want := []string{"first", "second"}; !reflect.DeepEqual(ends, want) {
		t.Fatalf("end order = %v, want %v", ends, want)
	}
}

func TestShutdownHooksOnce(t *testing.T) {
	hook := &testCloseHook{}

	clientRuntime := &client{
		hooks:  hooks{hook},
		exitCh: make(chan struct{}),
	}
	kafkaClient := &Client{cl: clientRuntime}

	if err := kafkaClient.Shutdown(context.Background()); err != nil {
		t.Fatalf("first client shutdown: %v", err)
	}
	if err := kafkaClient.Shutdown(context.Background()); err != nil {
		t.Fatalf("second client shutdown: %v", err)
	}

	if hook.clientCount != 1 {
		t.Fatalf("client close calls = %d, want 1", hook.clientCount)
	}

	sessionRuntime := &client{
		hooks:  hooks{hook},
		exitCh: make(chan struct{}),
	}
	session := &GroupTransactSession{cl: sessionRuntime}

	if err := session.Shutdown(context.Background()); err != nil {
		t.Fatalf("first session shutdown: %v", err)
	}
	if err := session.Shutdown(context.Background()); err != nil {
		t.Fatalf("second session shutdown: %v", err)
	}

	if hook.sessionCount != 1 {
		t.Fatalf("session close calls = %d, want 1", hook.sessionCount)
	}
}

func TestHooksShareAckSkipsZero(t *testing.T) {
	hook := &testShareAckHook{}
	hookSet := hooks{hook}

	hookSet.onShareAck(context.Background(), ShareAckAccept, 0)
	hookSet.onShareAck(context.Background(), ShareAckRelease, 2)

	want := []struct {
		outcome ShareAckOutcome
		count   int
	}{
		{outcome: ShareAckRelease, count: 2},
	}

	if !reflect.DeepEqual(hook.calls, want) {
		t.Fatalf("share ack calls = %#v, want %#v", hook.calls, want)
	}
}

func TestHooksOperations(t *testing.T) {
	hook := &testOperationHook{}
	hookSet := hooks{hook}

	offsetErr := errors.New("offset commit failed")
	hookSet.onOffsetCommit(context.Background(), 150*time.Millisecond, offsetErr)

	if hook.offsetCommitDuration != 150*time.Millisecond {
		t.Fatalf("offset commit duration = %s, want 150ms", hook.offsetCommitDuration)
	}
	if !errors.Is(hook.offsetCommitErr, offsetErr) {
		t.Fatalf("offset commit error = %v, want %v", hook.offsetCommitErr, offsetErr)
	}

	ackErr := errors.New("share ack flush failed")
	hookSet.onShareAckFlush(context.Background(), 250*time.Millisecond, ackErr)

	if hook.shareAckDuration != 250*time.Millisecond {
		t.Fatalf("share ack duration = %s, want 250ms", hook.shareAckDuration)
	}
	if !errors.Is(hook.shareAckErr, ackErr) {
		t.Fatalf("share ack error = %v, want %v", hook.shareAckErr, ackErr)
	}
}

func TestProcessHooksFlattensNestedHooks(t *testing.T) {
	var starts, ends []string

	first := &testHandleHook{name: "first", starts: &starts, ends: &ends}
	second := &testHandleHook{name: "second", starts: &starts, ends: &ends}

	processed, err := processHooks([]Hook{
		first,
		[]Hook{
			[]Hook{second},
		},
	})
	if err != nil {
		t.Fatalf("process hooks: %v", err)
	}

	want := []Hook{first, second}
	if !reflect.DeepEqual(processed, want) {
		t.Fatalf("processed hooks = %#v, want %#v", processed, want)
	}
}

func TestProcessHooksRejectsUnknownHook(t *testing.T) {
	_, err := processHooks([]Hook{struct{}{}})
	if err == nil {
		t.Fatal("expected hook validation error")
	}

	const want = "found an argument that implements no hook interfaces"
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err, want)
	}
}

func TestNewClientValidatesHooks(t *testing.T) {
	_, err := newClient(WithHooks(struct{}{}))
	if err == nil {
		t.Fatal("expected hook validation error")
	}

	const want = "found an argument that implements no hook interfaces"
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err, want)
	}
}
