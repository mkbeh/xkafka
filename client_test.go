package xkafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestClientShutdownStopsFetchLoop(t *testing.T) {
	client, err := NewClient(
		WithPollInterval(time.Hour),
		WithBatchHandler(func(context.Context, []*kgo.Record) error { return nil }),
	)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	ctx := context.Background()
	errCh := runTestHandleFetches(ctx, client)
	waitTestCondition(t, "polling loop start", client.cl.polling.Load)

	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown client: %v", err)
	}
	if err := waitTestHandleFetches(t, errCh); err != nil {
		t.Fatalf("handle fetches after shutdown = %v, want nil", err)
	}
}

func TestShouldAbortAfterCommit(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "operation not attempted",
			err:  kerr.OperationNotAttempted,
			want: true,
		},
		{
			name: "wrapped operation not attempted",
			err:  fmt.Errorf("wrapped: %w", kerr.OperationNotAttempted),
			want: true,
		},
		{
			name: "transaction abortable",
			err:  kerr.TransactionAbortable,
			want: true,
		},
		{
			name: "unknown server error",
			err:  kerr.UnknownServerError,
			want: true,
		},
		{
			name: "client closed",
			err:  kgo.ErrClientClosed,
			want: false,
		},
		{
			name: "wrapped client closed",
			err:  fmt.Errorf("wrapped: %w", kgo.ErrClientClosed),
			want: false,
		},
		{
			name: "other Kafka error",
			err:  kerr.UnknownTopicOrPartition,
			want: false,
		},
		{
			name: "transport error",
			err:  errors.New("transport failed"),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldAbortAfterCommit(tt.err); got != tt.want {
				t.Fatalf("shouldAbortAfterCommit(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
