package xkafka

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type hookContextKey string

type testHandleHook struct {
	name   string
	starts *[]string
	ends   *[]string
}

func (h *testHandleHook) OnHandleStart(ctx context.Context, _ int) context.Context {
	*h.starts = append(*h.starts, h.name)
	return context.WithValue(ctx, hookContextKey(h.name), true)
}

func (h *testHandleHook) OnHandleEnd(
	ctx context.Context,
	_ int,
	_ time.Duration,
	_ error,
) {
	if ctx.Value(hookContextKey(h.name)) == true {
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
	if ctx.Value(hookContextKey(h.name)) == true {
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

type testRetryHook struct {
	retries   []int
	exhausted []int
	err       error
}

func (h *testRetryHook) OnHandleRetry(_ context.Context, retry int) {
	h.retries = append(h.retries, retry)
}

func (h *testRetryHook) OnHandleRetryExhausted(_ context.Context, retries int, err error) {
	h.exhausted = append(h.exhausted, retries)
	h.err = err
}

func TestHooksHandleContext(t *testing.T) {
	var starts, ends []string

	first := &testHandleHook{name: "first", starts: &starts, ends: &ends}
	second := &testHandleHook{name: "second", starts: &starts, ends: &ends}
	hookSet := hooks{first, second}

	ctx := hookSet.onHandleStart(context.Background(), 3)
	hookSet.onHandleEnd(ctx, 3, time.Second, nil)

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

func TestHooksRetry(t *testing.T) {
	hook := &testRetryHook{}
	hookSet := hooks{hook}

	hookSet.onHandleRetry(context.Background(), 1)
	hookSet.onHandleRetry(context.Background(), 2)

	err := errors.New("handler failed")
	hookSet.onHandleRetryExhausted(context.Background(), 2, err)

	if want := []int{1, 2}; !reflect.DeepEqual(hook.retries, want) {
		t.Fatalf("retries = %v, want %v", hook.retries, want)
	}
	if want := []int{2}; !reflect.DeepEqual(hook.exhausted, want) {
		t.Fatalf("exhausted = %v, want %v", hook.exhausted, want)
	}
	if !errors.Is(hook.err, err) {
		t.Fatalf("exhausted error = %v, want %v", hook.err, err)
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
