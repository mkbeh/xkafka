package xkafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const testPollInterval = 5 * time.Millisecond

type testBatchContextKey struct{}

type testBatchLifecycleHook struct {
	mu sync.Mutex

	startCalls int
	endCalls   int
	endContext string
	endErr     error
}

func (h *testBatchLifecycleHook) OnHandleStart(
	ctx context.Context,
	_ []*kgo.Record,
) context.Context {
	h.mu.Lock()
	h.startCalls++
	h.mu.Unlock()

	return context.WithValue(ctx, testBatchContextKey{}, "handle-context")
}

func (h *testBatchLifecycleHook) OnHandleEnd(
	ctx context.Context,
	_ []*kgo.Record,
	_ time.Duration,
	err error,
) {
	contextValue, _ := ctx.Value(testBatchContextKey{}).(string)

	h.mu.Lock()
	defer h.mu.Unlock()

	h.endCalls++
	h.endContext = contextValue
	h.endErr = err
}

func (h *testBatchLifecycleHook) snapshot() (startCalls, endCalls int, endContext string, endErr error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.startCalls, h.endCalls, h.endContext, h.endErr
}

func TestClientBatchConsumerMaxPollRecords(t *testing.T) {
	const (
		topic       = "batch-max-poll-records"
		recordCount = 7
	)

	cluster := newTestKafkaCluster(t, topic)
	produceTestRecords(t, cluster, topic, recordCount)

	var (
		batches    [][]int64
		totalCount int
	)
	done := make(chan struct{})

	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(kgo.ConsumeTopics(topic)),
		WithMaxPollRecords(3),
		WithPollInterval(testPollInterval),
		WithBatchHandler(func(_ context.Context, records []*kgo.Record) error {
			offsets := make([]int64, len(records))
			for i, record := range records {
				offsets[i] = record.Offset
			}

			batches = append(batches, offsets)
			totalCount += len(records)
			if totalCount == recordCount {
				close(done)
			}

			return nil
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runTestHandleFetches(ctx, client)

	waitTestSignal(t, done, "batch consumer")
	cancel()

	if err := waitTestHandleFetches(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
	}

	if totalCount != recordCount {
		t.Fatalf("handled records = %d, want %d", totalCount, recordCount)
	}

	var gotOffsets []int64
	for i, batch := range batches {
		if len(batch) == 0 || len(batch) > 3 {
			t.Fatalf("batch %d size = %d, want 1..3", i, len(batch))
		}
		gotOffsets = append(gotOffsets, batch...)
	}

	wantOffsets := []int64{0, 1, 2, 3, 4, 5, 6}
	if !reflect.DeepEqual(gotOffsets, wantOffsets) {
		t.Fatalf("handled offsets = %v, want %v", gotOffsets, wantOffsets)
	}
}

func TestClientBatchConsumerRetries(t *testing.T) {
	t.Run("until success", func(t *testing.T) {
		const topic = "batch-retries-success"

		cluster := newTestKafkaCluster(t, topic)
		produceTestRecords(t, cluster, topic, 1)

		handleErr := errors.New("handle failed")
		var offsets []int64
		done := make(chan struct{})

		client := newTestClient(
			t,
			cluster,
			WithKafkaOptions(kgo.ConsumeTopics(topic)),
			WithMaxRetries(2),
			WithSuspendProcessingTimeout(0),
			WithPollInterval(testPollInterval),
			WithBatchHandler(func(_ context.Context, records []*kgo.Record) error {
				offsets = append(offsets, records[0].Offset)
				if len(offsets) < 3 {
					return handleErr
				}

				close(done)
				return nil
			}),
		)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := runTestHandleFetches(ctx, client)

		waitTestSignal(t, done, "successful retry")
		cancel()

		if err := waitTestHandleFetches(t, errCh); !errors.Is(err, context.Canceled) {
			t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
		}

		wantOffsets := []int64{0, 0, 0}
		if !reflect.DeepEqual(offsets, wantOffsets) {
			t.Fatalf("retry offsets = %v, want %v", offsets, wantOffsets)
		}
	})

	t.Run("cancel during retry wait", func(t *testing.T) {
		const topic = "batch-retries-cancel"

		cluster := newTestKafkaCluster(t, topic)
		produceTestRecords(t, cluster, topic, 1)

		handleErr := errors.New("handle failed")
		firstAttempt := make(chan struct{})
		calls := 0

		client := newTestClient(
			t,
			cluster,
			WithKafkaOptions(kgo.ConsumeTopics(topic)),
			WithSuspendProcessingTimeout(time.Hour),
			WithPollInterval(testPollInterval),
			WithBatchHandler(func(_ context.Context, _ []*kgo.Record) error {
				calls++
				if calls == 1 {
					close(firstAttempt)
				}

				return handleErr
			}),
		)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := runTestHandleFetches(ctx, client)

		waitTestSignal(t, firstAttempt, "first retry attempt")
		cancel()

		if err := waitTestHandleFetches(t, errCh); !errors.Is(err, context.Canceled) {
			t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
		}
		if calls != 1 {
			t.Fatalf("handler calls = %d, want 1", calls)
		}
	})

	tests := []struct {
		name       string
		maxRetries int
		wantCalls  int
	}{
		{name: "disabled", maxRetries: 0, wantCalls: 1},
		{name: "exhausted", maxRetries: 2, wantCalls: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := "batch-retries-" + tt.name
			cluster := newTestKafkaCluster(t, topic)
			produceTestRecords(t, cluster, topic, 1)

			handleErr := errors.New("handle failed")
			calls := 0

			client := newTestClient(
				t,
				cluster,
				WithKafkaOptions(kgo.ConsumeTopics(topic)),
				WithMaxRetries(tt.maxRetries),
				WithSuspendProcessingTimeout(0),
				WithPollInterval(testPollInterval),
				WithBatchHandler(func(_ context.Context, records []*kgo.Record) error {
					calls++
					if len(records) != 1 || records[0].Offset != 0 {
						return errors.New("unexpected retry records")
					}

					return handleErr
				}),
			)

			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			err := client.HandleFetches(ctx)
			if !errors.Is(err, handleErr) {
				t.Fatalf("handle fetches error = %v, want %v", err, handleErr)
			}
			if !strings.Contains(err.Error(), "kafka: batch handler retries exhausted") {
				t.Fatalf("handle fetches error = %q, want retries exhausted", err)
			}
			if calls != tt.wantCalls {
				t.Fatalf("handler calls = %d, want %d", calls, tt.wantCalls)
			}
		})
	}
}

func TestClientBatchConsumerPanic(t *testing.T) {
	const topic = "batch-handler-panic"

	cluster := newTestKafkaCluster(t, topic)
	produceTestRecords(t, cluster, topic, 1)

	hook := &testBatchLifecycleHook{}
	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(kgo.ConsumeTopics(topic)),
		WithHooks(hook),
		WithMaxRetries(0),
		WithPollInterval(testPollInterval),
		WithBatchHandler(func(ctx context.Context, records []*kgo.Record) error {
			if got, _ := ctx.Value(testBatchContextKey{}).(string); got != "handle-context" {
				return fmt.Errorf("handler context = %q, want handle-context", got)
			}
			if len(records) != 1 || records[0].Offset != 0 {
				return errors.New("unexpected handler records")
			}

			panic("boom")
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	err := client.HandleFetches(ctx)
	if err == nil || !strings.Contains(err.Error(), "kafka: batch handler panic: boom") {
		t.Fatalf("handle fetches error = %v, want recovered panic", err)
	}

	startCalls, endCalls, endContext, endErr := hook.snapshot()
	if startCalls != 1 {
		t.Fatalf("handle start calls = %d, want 1", startCalls)
	}
	if endCalls != 1 {
		t.Fatalf("handle end calls = %d, want 1", endCalls)
	}
	if endContext != "handle-context" {
		t.Fatalf("handle end context = %q, want handle-context", endContext)
	}
	if endErr == nil || !strings.Contains(endErr.Error(), "kafka: batch handler panic: boom") {
		t.Fatalf("handle end error = %v, want recovered panic", endErr)
	}
}

func TestClientBatchConsumerCommitModes(t *testing.T) {
	tests := []struct {
		name      string
		groupOpts []kgo.Opt
	}{
		{
			name: "manual commit",
			groupOpts: []kgo.Opt{
				kgo.DisableAutoCommit(),
			},
		},
		{
			name: "auto commit marks",
			groupOpts: []kgo.Opt{
				kgo.AutoCommitMarks(),
				kgo.AutoCommitInterval(100 * time.Millisecond),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const (
				topic       = "batch-commit-mode"
				recordCount = 3
			)

			cluster := newTestKafkaCluster(t, topic)
			produceTestRecords(t, cluster, topic, recordCount)

			processed := make(chan struct{})
			var (
				processedOnce  sync.Once
				processedCount int
			)

			kafkaOpts := []kgo.Opt{
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup("xkafka-test-" + strings.ReplaceAll(tt.name, " ", "-")),
			}
			kafkaOpts = append(kafkaOpts, tt.groupOpts...)

			client := newTestClient(
				t,
				cluster,
				WithKafkaOptions(kafkaOpts...),
				WithMaxPollRecords(recordCount),
				WithPollInterval(testPollInterval),
				WithBatchHandler(func(_ context.Context, records []*kgo.Record) error {
					processedCount += len(records)
					if processedCount == recordCount {
						processedOnce.Do(func() { close(processed) })
					}
					return nil
				}),
			)

			ctx, cancel := context.WithCancel(context.Background())
			errCh := runTestHandleFetches(ctx, client)

			waitTestSignal(t, processed, "consumer batch")
			waitTestCondition(t, "committed offset", func() bool {
				offsets := client.cl.Client().CommittedOffsets()
				partitions, ok := offsets[topic]
				if !ok {
					return false
				}

				offset, ok := partitions[0]
				return ok && offset.Offset == recordCount
			})

			cancel()
			if err := waitTestHandleFetches(t, errCh); !errors.Is(err, context.Canceled) {
				t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
			}
		})
	}
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

type offsetCommitEvent struct {
	duration time.Duration
	err      error
}

type offsetCommitHook struct {
	events chan offsetCommitEvent
}

func newOffsetCommitHook() *offsetCommitHook {
	return &offsetCommitHook{events: make(chan offsetCommitEvent, 4)}
}

func (h *offsetCommitHook) OnOffsetCommit(_ context.Context, duration time.Duration, err error) {
	h.events <- offsetCommitEvent{duration: duration, err: err}
}

func waitOffsetCommitEvent(t *testing.T, hook *offsetCommitHook) offsetCommitEvent {
	t.Helper()

	select {
	case event := <-hook.events:
		return event
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for offset commit hook")
		return offsetCommitEvent{}
	}
}

func failNextOffsetCommit(cluster *kfake.Cluster) {
	cluster.ControlKey(int16(kmsg.OffsetCommit), func(kmsg.Request) (kmsg.Response, error, bool) {
		return nil, errors.New("forced offset commit failure"), true
	})
}

func TestClientCommitOffsetsRetries(t *testing.T) {
	const (
		topic = "commit-retry"
		group = "commit-retry-group"
	)

	cluster := newTestKafkaCluster(t, topic)
	produceTestRecords(t, cluster, topic, 1)
	failNextOffsetCommit(cluster)

	hook := newOffsetCommitHook()
	processed := make(chan struct{})
	var processedOnce sync.Once

	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.DisableAutoCommit(),
			kgo.RequestRetries(0),
		),
		WithHooks(hook),
		WithSuspendCommittingTimeout(0),
		WithPollInterval(testPollInterval),
		WithBatchHandler(func(context.Context, []*kgo.Record) error {
			processedOnce.Do(func() { close(processed) })
			return nil
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runTestHandleFetches(ctx, client)
	waitTestSignal(t, processed, "consumer batch")

	first := waitOffsetCommitEvent(t, hook)
	if first.err == nil {
		t.Fatal("first offset commit error = nil, want failure")
	}
	if first.duration < 0 {
		t.Fatalf("first offset commit duration = %s, want non-negative", first.duration)
	}

	second := waitOffsetCommitEvent(t, hook)
	if second.err != nil {
		t.Fatalf("second offset commit error = %v, want nil", second.err)
	}

	waitTestCondition(t, "committed offset after retry", func() bool {
		offsets := client.cl.Client().CommittedOffsets()
		partitions, ok := offsets[topic]
		if !ok {
			return false
		}

		offset, ok := partitions[0]
		return ok && offset.Offset == 1
	})

	cancel()
	if err := waitTestHandleFetches(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
	}
}

func TestClientCommitOffsetsCancellationStopsRetry(t *testing.T) {
	const (
		topic = "commit-retry-cancel"
		group = "commit-retry-cancel-group"
	)

	cluster := newTestKafkaCluster(t, topic)
	produceTestRecords(t, cluster, topic, 1)
	failNextOffsetCommit(cluster)

	hook := newOffsetCommitHook()
	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.DisableAutoCommit(),
			kgo.RequestRetries(0),
		),
		WithHooks(hook),
		WithSuspendCommittingTimeout(time.Hour),
		WithPollInterval(testPollInterval),
		WithBatchHandler(func(context.Context, []*kgo.Record) error { return nil }),
	)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runTestHandleFetches(ctx, client)

	first := waitOffsetCommitEvent(t, hook)
	if first.err == nil {
		t.Fatal("first offset commit error = nil, want failure")
	}
	cancel()

	if err := waitTestHandleFetches(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
	}

	select {
	case event := <-hook.events:
		t.Fatalf("unexpected offset commit retry after cancellation: %v", event.err)
	default:
	}
}
