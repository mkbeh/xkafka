package xkafka

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fetchErrorEvent struct {
	topic       string
	partition   int32
	recoverable bool
	err         error
}

type fetchErrorCollector struct {
	events []fetchErrorEvent
}

func (h *fetchErrorCollector) OnFetchError(
	_ context.Context,
	topic string,
	partition int32,
	recoverable bool,
	err error,
) {
	h.events = append(h.events, fetchErrorEvent{
		topic:       topic,
		partition:   partition,
		recoverable: recoverable,
		err:         err,
	})
}

func TestRecordContext(t *testing.T) {
	type contextKey struct{}

	fallback := context.WithValue(context.Background(), contextKey{}, "fallback")
	recordCtx := context.WithValue(context.Background(), contextKey{}, "record")

	tests := []struct {
		name   string
		record *kgo.Record
		want   string
	}{
		{name: "nil record", want: "fallback"},
		{name: "record without context", record: &kgo.Record{}, want: "fallback"},
		{name: "record context", record: &kgo.Record{Context: recordCtx}, want: "record"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := recordContext(fallback, tt.record).Value(contextKey{}).(string)
			if got != tt.want {
				t.Fatalf("record context value = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsRecoverableFetchError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "retriable Kafka error", err: kerr.UnknownTopicOrPartition, want: true},
		{name: "wrapped retriable Kafka error", err: errors.Join(errors.New("wrapped"), kerr.RequestTimedOut), want: true},
		{name: "data loss", err: &kgo.ErrDataLoss{}, want: true},
		{name: "wrapped data loss", err: errors.Join(errors.New("wrapped"), &kgo.ErrDataLoss{}), want: true},
		{name: "group session", err: &kgo.ErrGroupSession{Err: errors.New("session lost")}, want: true},
		{name: "non-retriable Kafka error", err: kerr.TopicAuthorizationFailed, want: false},
		{name: "generic error", err: errors.New("fetch failed"), want: false},
		{name: "context canceled", err: context.Canceled, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRecoverableFetchError(tt.err); got != tt.want {
				t.Fatalf("isRecoverableFetchError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestHandleFetchErrors(t *testing.T) {
	recoverableErr := kerr.UnknownTopicOrPartition
	fatalErr := kerr.TopicAuthorizationFailed
	secondFatalErr := kerr.GroupAuthorizationFailed
	dataLossErr := &kgo.ErrDataLoss{Topic: "inventory", Partition: 3}

	fetches := kgo.Fetches{
		{
			Topics: []kgo.FetchTopic{
				{
					Topic: "orders",
					Partitions: []kgo.FetchPartition{
						{Partition: 1, Err: recoverableErr},
						{Partition: 2, Err: fatalErr},
					},
				},
				{
					Topic: "inventory",
					Partitions: []kgo.FetchPartition{
						{Partition: 3, Err: dataLossErr},
						{Partition: 4, Err: secondFatalErr},
					},
				},
			},
		},
	}

	hook := &fetchErrorCollector{}
	cl := &client{hooks: hooks{hook}}

	err := cl.handleFetchErrors(context.Background(), fetches)
	if !errors.Is(err, fatalErr) {
		t.Fatalf("handle fetch errors = %v, want fatal error %v", err, fatalErr)
	}
	if !strings.Contains(err.Error(), `kafka: fetch topic "orders" partition 2`) {
		t.Fatalf("handle fetch errors = %q, want first fatal topic and partition", err)
	}
	if errors.Is(err, secondFatalErr) {
		t.Fatalf("handle fetch errors = %v, want first fatal error only", err)
	}

	want := []fetchErrorEvent{
		{topic: "orders", partition: 1, recoverable: true, err: recoverableErr},
		{topic: "orders", partition: 2, recoverable: false, err: fatalErr},
		{topic: "inventory", partition: 3, recoverable: true, err: dataLossErr},
		{topic: "inventory", partition: 4, recoverable: false, err: secondFatalErr},
	}
	if !reflect.DeepEqual(hook.events, want) {
		t.Fatalf("fetch error hook events = %#v, want %#v", hook.events, want)
	}
}

func TestHandleFetchErrorsRecoverableOnly(t *testing.T) {
	hook := &fetchErrorCollector{}
	cl := &client{hooks: hooks{hook}}

	fetches := kgo.Fetches{
		{
			Topics: []kgo.FetchTopic{
				{
					Topic: "orders",
					Partitions: []kgo.FetchPartition{
						{Partition: 0, Err: kerr.UnknownTopicOrPartition},
						{Partition: 1, Err: &kgo.ErrGroupSession{Err: errors.New("session lost")}},
					},
				},
			},
		},
	}

	if err := cl.handleFetchErrors(context.Background(), fetches); err != nil {
		t.Fatalf("handle recoverable fetch errors: %v", err)
	}
	if len(hook.events) != 2 {
		t.Fatalf("fetch error hook calls = %d, want 2", len(hook.events))
	}
	for i, event := range hook.events {
		if !event.recoverable {
			t.Fatalf("fetch error hook event %d recoverable = false, want true", i)
		}
	}
}

func TestHandleFetchesLifecycle(t *testing.T) {
	t.Run("nil connection", func(t *testing.T) {
		cl := &client{}
		err := cl.HandleFetches(context.Background())
		if err == nil || err.Error() != "kafka: conn is nil" {
			t.Fatalf("handle fetches error = %v, want nil connection error", err)
		}
	})

	t.Run("nil handler", func(t *testing.T) {
		cl := &client{conn: &testClientConn{}}
		err := cl.HandleFetches(context.Background())
		if err == nil || err.Error() != "kafka: fetches handler is nil" {
			t.Fatalf("handle fetches error = %v, want nil handler error", err)
		}
	})

	t.Run("single polling loop", func(t *testing.T) {
		cl := &client{
			conn:          &testClientConn{},
			handleFetches: func(context.Context, kgo.Fetches) error { return nil },
			pollInterval:  time.Hour,
			exitCh:        make(chan struct{}),
		}

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- cl.HandleFetches(ctx) }()

		waitTestCondition(t, "polling loop start", cl.polling.Load)

		err := cl.HandleFetches(context.Background())
		if err == nil || err.Error() != "kafka: fetch loop already running" {
			t.Fatalf("second handle fetches error = %v, want already running", err)
		}

		cancel()
		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("first handle fetches error = %v, want %v", err, context.Canceled)
			}
		case <-time.After(testTimeout):
			t.Fatal("timed out waiting for first polling loop")
		}

		canceled, cancelAgain := context.WithCancel(context.Background())
		cancelAgain()
		if err := cl.HandleFetches(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("handle fetches after polling reset = %v, want %v", err, context.Canceled)
		}
	})
}

func TestClientProcessFetchesAllowRebalance(t *testing.T) {
	handleErr := errors.New("handle failed")

	tests := []struct {
		name                string
		blockRebalance      bool
		cancelContext       bool
		wantErr             error
		wantAllowRebalances int
	}{
		{
			name:                "disabled",
			wantErr:             handleErr,
			wantAllowRebalances: 0,
		},
		{
			name:                "enabled after handler error",
			blockRebalance:      true,
			wantErr:             handleErr,
			wantAllowRebalances: 1,
		},
		{
			name:                "enabled after context cancellation",
			blockRebalance:      true,
			cancelContext:       true,
			wantErr:             context.Canceled,
			wantAllowRebalances: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &testClientConn{}
			runtime := &client{
				conn:           conn,
				blockRebalance: tt.blockRebalance,
				handleFetches: func(context.Context, kgo.Fetches) error {
					return handleErr
				},
			}

			ctx := context.Background()
			if tt.cancelContext {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			err := runtime.processFetches(ctx, nil)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("process fetches error = %v, want %v", err, tt.wantErr)
			}
			if conn.allowRebalanceCalls != tt.wantAllowRebalances {
				t.Fatalf(
					"allow rebalance calls = %d, want %d",
					conn.allowRebalanceCalls,
					tt.wantAllowRebalances,
				)
			}
		})
	}
}
