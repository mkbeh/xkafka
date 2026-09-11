package xkafka

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

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
