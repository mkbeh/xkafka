package xkafka_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type groupSessionTxContextKey struct{}

type groupSessionHandleContextKey struct{}

type groupSessionTransactionEvent struct {
	transactionType TransactionType
	outcome         TransactionOutcome
	contextValue    string
	duration        time.Duration
	err             error
}

type groupSessionHandleEvent struct {
	transactionContext string
	handleContext      string
	duration           time.Duration
	err                error
}

type groupSessionHookState struct {
	sequence          []string
	transactionStarts int
	transactionEnds   []groupSessionTransactionEvent
	handleStarts      int
	handleEnds        []groupSessionHandleEvent
}

type groupSessionHook struct {
	mu sync.Mutex

	state groupSessionHookState
	ends  chan groupSessionTransactionEvent
}

func newGroupSessionHook() *groupSessionHook {
	return &groupSessionHook{
		ends: make(chan groupSessionTransactionEvent, 16),
	}
}

func (h *groupSessionHook) OnTransactionStart(
	ctx context.Context,
	_ TransactionType,
) context.Context {
	h.mu.Lock()
	h.state.sequence = append(h.state.sequence, "transaction.start")
	h.state.transactionStarts++
	h.mu.Unlock()

	return context.WithValue(ctx, groupSessionTxContextKey{}, "transaction-context")
}

func (h *groupSessionHook) OnTransactionEnd(
	ctx context.Context,
	transactionType TransactionType,
	outcome TransactionOutcome,
	duration time.Duration,
	err error,
) {
	contextValue, _ := ctx.Value(groupSessionTxContextKey{}).(string)
	event := groupSessionTransactionEvent{
		transactionType: transactionType,
		outcome:         outcome,
		contextValue:    contextValue,
		duration:        duration,
		err:             err,
	}

	h.mu.Lock()
	h.state.sequence = append(h.state.sequence, "transaction.end")
	h.state.transactionEnds = append(h.state.transactionEnds, event)
	h.mu.Unlock()

	h.ends <- event
}

func (h *groupSessionHook) OnHandleStart(
	ctx context.Context,
	_ []*kgo.Record,
) context.Context {
	h.mu.Lock()
	h.state.sequence = append(h.state.sequence, "handle.start")
	h.state.handleStarts++
	h.mu.Unlock()

	return context.WithValue(ctx, groupSessionHandleContextKey{}, "handle-context")
}

func (h *groupSessionHook) OnHandleEnd(
	ctx context.Context,
	_ []*kgo.Record,
	duration time.Duration,
	err error,
) {
	transactionContext, _ := ctx.Value(groupSessionTxContextKey{}).(string)
	handleContext, _ := ctx.Value(groupSessionHandleContextKey{}).(string)

	h.mu.Lock()
	defer h.mu.Unlock()

	h.state.sequence = append(h.state.sequence, "handle.end")
	h.state.handleEnds = append(h.state.handleEnds, groupSessionHandleEvent{
		transactionContext: transactionContext,
		handleContext:      handleContext,
		duration:           duration,
		err:                err,
	})
}

func (h *groupSessionHook) snapshot() groupSessionHookState {
	h.mu.Lock()
	defer h.mu.Unlock()

	state := h.state
	state.sequence = append([]string(nil), h.state.sequence...)
	state.transactionEnds = append([]groupSessionTransactionEvent(nil), h.state.transactionEnds...)
	state.handleEnds = append([]groupSessionHandleEvent(nil), h.state.handleEnds...)

	return state
}

func waitGroupSessionTransactionEvent(
	t *testing.T,
	hook *groupSessionHook,
) groupSessionTransactionEvent {
	t.Helper()

	select {
	case event := <-hook.ends:
		return event
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for group transaction end")
		return groupSessionTransactionEvent{}
	}
}

func TestGroupTransactSessionCommit(t *testing.T) {
	const (
		inputTopic  = "group-tx-commit-input"
		outputTopic = "group-tx-commit-output"
		group       = "group-tx-commit-group"
		recordCount = 3
	)

	cluster := newTestKafkaCluster(t, inputTopic, outputTopic)
	produceTestRecords(t, cluster, inputTopic, recordCount)

	hook := newGroupSessionHook()
	session := newTestGroupTransactSession(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(inputTopic),
			kgo.ConsumerGroup(group),
			kgo.TransactionalID("group-tx-commit-transaction"),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		),
		WithHooks(hook),
		WithMaxPollRecords(recordCount),
		WithPollInterval(testPollInterval),
		WithGroupTransactSessionBatchHandler(func(
			ctx context.Context,
			records []*kgo.Record,
			tx *Tx,
		) error {
			if got, _ := ctx.Value(groupSessionTxContextKey{}).(string); got != "transaction-context" {
				return fmt.Errorf("transaction context = %q, want transaction-context", got)
			}
			if got, _ := ctx.Value(groupSessionHandleContextKey{}).(string); got != "handle-context" {
				return fmt.Errorf("handle context = %q, want handle-context", got)
			}
			if len(records) != recordCount {
				return fmt.Errorf("handler records = %d, want %d", len(records), recordCount)
			}

			outputs := make([]*kgo.Record, 0, len(records))
			for _, record := range records {
				outputs = append(outputs, &kgo.Record{
					Topic: outputTopic,
					Key:   append([]byte(nil), record.Key...),
					Value: append([]byte("processed-"), record.Value...),
				})
			}

			return tx.ProduceSync(ctx, outputs...)
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := runTestGroupTransactSession(ctx, session)

	event := waitGroupSessionTransactionEvent(t, hook)
	if event.transactionType != TransactionTypeGroup {
		t.Fatalf("transaction type = %q, want %q", event.transactionType, TransactionTypeGroup)
	}
	if event.outcome != TransactionOutcomeCommit {
		t.Fatalf("transaction outcome = %q, want %q", event.outcome, TransactionOutcomeCommit)
	}
	if event.contextValue != "transaction-context" {
		t.Fatalf("transaction end context = %q, want transaction-context", event.contextValue)
	}
	if event.duration < 0 {
		t.Fatalf("transaction duration = %s, want non-negative", event.duration)
	}
	if event.err != nil {
		t.Fatalf("transaction end error = %v, want nil", event.err)
	}

	waitTestCommittedOffset(t, cluster, group, inputTopic, recordCount)

	cancel()
	if err := waitTestGroupTransactSession(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
	}

	state := hook.snapshot()
	if state.transactionStarts != 1 {
		t.Fatalf("transaction start calls = %d, want 1", state.transactionStarts)
	}
	if state.handleStarts != 1 {
		t.Fatalf("handle start calls = %d, want 1", state.handleStarts)
	}
	if len(state.transactionEnds) != 1 {
		t.Fatalf("transaction end calls = %d, want 1", len(state.transactionEnds))
	}
	if len(state.handleEnds) != 1 {
		t.Fatalf("handle end calls = %d, want 1", len(state.handleEnds))
	}
	if !reflect.DeepEqual(state.sequence, []string{
		"transaction.start",
		"handle.start",
		"handle.end",
		"transaction.end",
	}) {
		t.Fatalf("hook sequence = %v, want transaction/handle lifecycle", state.sequence)
	}

	handleEnd := state.handleEnds[0]
	if handleEnd.transactionContext != "transaction-context" {
		t.Fatalf("handle end transaction context = %q, want transaction-context", handleEnd.transactionContext)
	}
	if handleEnd.handleContext != "handle-context" {
		t.Fatalf("handle end context = %q, want handle-context", handleEnd.handleContext)
	}
	if handleEnd.duration < 0 {
		t.Fatalf("handle duration = %s, want non-negative", handleEnd.duration)
	}
	if handleEnd.err != nil {
		t.Fatalf("handle end error = %v, want nil", handleEnd.err)
	}

	outputs := consumeCommittedTestRecords(t, cluster, outputTopic, recordCount)
	for i, record := range outputs {
		wantKey := fmt.Sprintf("key-%d", i)
		if string(record.Key) != wantKey {
			t.Fatalf("output record %d key = %q, want %q", i, record.Key, wantKey)
		}

		wantValue := fmt.Sprintf("processed-value-%d", i)
		if string(record.Value) != wantValue {
			t.Fatalf("output record %d value = %q, want %q", i, record.Value, wantValue)
		}
	}
}

func TestGroupTransactSessionAbortAndRedelivery(t *testing.T) {
	handleErr := errors.New("handle failed")

	tests := []struct {
		name            string
		failFirst       func() error
		wantFirstErr    error
		wantErrContains string
	}{
		{
			name: "handler error",
			failFirst: func() error {
				return handleErr
			},
			wantFirstErr: handleErr,
		},
		{
			name: "handler panic",
			failFirst: func() error {
				panic("boom")
			},
			wantErrContains: "kafka: batch handler panic: boom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputTopic := "group-tx-redelivery-input-" + strings.ReplaceAll(tt.name, " ", "-")
			outputTopic := "group-tx-redelivery-output-" + strings.ReplaceAll(tt.name, " ", "-")
			group := "group-tx-redelivery-group-" + strings.ReplaceAll(tt.name, " ", "-")

			cluster := newTestKafkaCluster(t, inputTopic, outputTopic)
			produceTestRecords(t, cluster, inputTopic, 1)

			hook := newGroupSessionHook()
			var (
				attempts       int
				attemptOffsets []int64
			)

			session := newTestGroupTransactSession(
				t,
				cluster,
				WithKafkaOptions(
					kgo.ConsumeTopics(inputTopic),
					kgo.ConsumerGroup(group),
					kgo.TransactionalID("group-tx-redelivery-transaction-"+strings.ReplaceAll(tt.name, " ", "-")),
					kgo.FetchIsolationLevel(kgo.ReadCommitted()),
				),
				WithHooks(hook),
				WithMaxPollRecords(1),
				WithPollInterval(testPollInterval),
				WithSuspendProcessingTimeout(0),
				WithGroupTransactSessionBatchHandler(func(
					ctx context.Context,
					records []*kgo.Record,
					tx *Tx,
				) error {
					if len(records) != 1 {
						return fmt.Errorf("handler records = %d, want 1", len(records))
					}

					attempts++
					attemptOffsets = append(attemptOffsets, records[0].Offset)

					value := "committed"
					if attempts == 1 {
						value = "aborted"
					}

					if err := tx.ProduceSync(ctx, &kgo.Record{
						Topic: outputTopic,
						Value: []byte(value),
					}); err != nil {
						return err
					}

					if attempts == 1 {
						return tt.failFirst()
					}

					return nil
				}),
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			errCh := runTestGroupTransactSession(ctx, session)

			first := waitGroupSessionTransactionEvent(t, hook)
			if first.outcome != TransactionOutcomeAbort {
				t.Fatalf("first transaction outcome = %q, want %q", first.outcome, TransactionOutcomeAbort)
			}
			if tt.wantFirstErr != nil && !errors.Is(first.err, tt.wantFirstErr) {
				t.Fatalf("first transaction error = %v, want %v", first.err, tt.wantFirstErr)
			}
			if tt.wantErrContains != "" && (first.err == nil || !strings.Contains(first.err.Error(), tt.wantErrContains)) {
				t.Fatalf("first transaction error = %v, want %q", first.err, tt.wantErrContains)
			}

			second := waitGroupSessionTransactionEvent(t, hook)
			if second.outcome != TransactionOutcomeCommit {
				t.Fatalf("second transaction outcome = %q, want %q", second.outcome, TransactionOutcomeCommit)
			}
			if second.err != nil {
				t.Fatalf("second transaction error = %v, want nil", second.err)
			}

			waitTestCommittedOffset(t, cluster, group, inputTopic, 1)

			cancel()
			if err := waitTestGroupTransactSession(t, errCh); !errors.Is(err, context.Canceled) {
				t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
			}

			if attempts != 2 {
				t.Fatalf("handler attempts = %d, want 2", attempts)
			}
			if !reflect.DeepEqual(attemptOffsets, []int64{0, 0}) {
				t.Fatalf("handler offsets = %v, want [0 0]", attemptOffsets)
			}

			output := consumeCommittedTestRecords(t, cluster, outputTopic, 1)[0]
			if string(output.Value) != "committed" {
				t.Fatalf("committed output value = %q, want committed", output.Value)
			}

			state := hook.snapshot()
			if state.transactionStarts != 2 {
				t.Fatalf("transaction start calls = %d, want 2", state.transactionStarts)
			}
			if state.handleStarts != 2 {
				t.Fatalf("handle start calls = %d, want 2", state.handleStarts)
			}
			if len(state.transactionEnds) != 2 {
				t.Fatalf("transaction end calls = %d, want 2", len(state.transactionEnds))
			}
			if len(state.handleEnds) != 2 {
				t.Fatalf("handle end calls = %d, want 2", len(state.handleEnds))
			}
		})
	}
}

func TestGroupTransactSessionBeginErrorStopsFetchLoop(t *testing.T) {
	const (
		inputTopic = "group-tx-begin-error-input"
		group      = "group-tx-begin-error-group"
	)

	cluster := newTestKafkaCluster(t, inputTopic)
	produceTestRecords(t, cluster, inputTopic, 1)

	hook := newGroupSessionHook()
	handlerCalls := 0
	session := newTestGroupTransactSession(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(inputTopic),
			kgo.ConsumerGroup(group),
		),
		WithHooks(hook),
		WithMaxPollRecords(1),
		WithPollInterval(testPollInterval),
		WithGroupTransactSessionBatchHandler(func(context.Context, []*kgo.Record, *Tx) error {
			handlerCalls++
			return nil
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	err := session.HandleFetches(ctx)
	if err == nil || !strings.Contains(err.Error(), "kafka: begin group transaction") {
		t.Fatalf("handle fetches error = %v, want begin group transaction error", err)
	}
	if handlerCalls != 0 {
		t.Fatalf("handler calls = %d, want 0", handlerCalls)
	}

	event := waitGroupSessionTransactionEvent(t, hook)
	if event.transactionType != TransactionTypeGroup {
		t.Fatalf("transaction type = %q, want %q", event.transactionType, TransactionTypeGroup)
	}
	if event.outcome != TransactionOutcomeError {
		t.Fatalf("transaction outcome = %q, want %q", event.outcome, TransactionOutcomeError)
	}
	if event.err == nil || !strings.Contains(event.err.Error(), "kafka: begin group transaction") {
		t.Fatalf("transaction end error = %v, want begin group transaction error", event.err)
	}

	state := hook.snapshot()
	if state.transactionStarts != 1 {
		t.Fatalf("transaction start calls = %d, want 1", state.transactionStarts)
	}
	if state.handleStarts != 0 {
		t.Fatalf("handle start calls = %d, want 0", state.handleStarts)
	}
	if len(state.handleEnds) != 0 {
		t.Fatalf("handle end calls = %d, want 0", len(state.handleEnds))
	}
}

func TestGroupTransactSessionEndErrorStopsFetchLoop(t *testing.T) {
	const (
		inputTopic  = "group-tx-end-error-input"
		outputTopic = "group-tx-end-error-output"
		group       = "group-tx-end-error-group"
	)

	cluster := newTestKafkaCluster(t, inputTopic, outputTopic)
	produceTestRecords(t, cluster, inputTopic, 1)
	failNextEndTxn(cluster)

	hook := newGroupSessionHook()
	handlerCalls := 0
	session := newTestGroupTransactSession(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(inputTopic),
			kgo.ConsumerGroup(group),
			kgo.TransactionalID("group-tx-end-error-transaction"),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			kgo.RequestRetries(0),
		),
		WithHooks(hook),
		WithMaxPollRecords(1),
		WithPollInterval(testPollInterval),
		WithGroupTransactSessionBatchHandler(func(
			ctx context.Context,
			records []*kgo.Record,
			tx *Tx,
		) error {
			handlerCalls++
			if len(records) != 1 {
				return fmt.Errorf("handler records = %d, want 1", len(records))
			}

			return tx.ProduceSync(ctx, &kgo.Record{
				Topic: outputTopic,
				Value: []byte("must-not-commit"),
			})
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	err := session.HandleFetches(ctx)
	if err == nil || !strings.Contains(err.Error(), "kafka: end group transaction") {
		t.Fatalf("handle fetches error = %v, want end group transaction error", err)
	}
	if handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want 1", handlerCalls)
	}

	event := waitGroupSessionTransactionEvent(t, hook)
	if event.transactionType != TransactionTypeGroup {
		t.Fatalf("transaction type = %q, want %q", event.transactionType, TransactionTypeGroup)
	}
	if event.outcome != TransactionOutcomeError {
		t.Fatalf("transaction outcome = %q, want %q", event.outcome, TransactionOutcomeError)
	}
	if event.err == nil || !strings.Contains(event.err.Error(), "kafka: end group transaction") {
		t.Fatalf("transaction end error = %v, want end group transaction error", event.err)
	}

	state := hook.snapshot()
	if state.transactionStarts != 1 {
		t.Fatalf("transaction start calls = %d, want 1", state.transactionStarts)
	}
	if state.handleStarts != 1 {
		t.Fatalf("handle start calls = %d, want 1", state.handleStarts)
	}
	if len(state.handleEnds) != 1 {
		t.Fatalf("handle end calls = %d, want 1", len(state.handleEnds))
	}
	if len(state.transactionEnds) != 1 {
		t.Fatalf("transaction end calls = %d, want 1", len(state.transactionEnds))
	}

	assertNoCommittedTestRecords(t, cluster, outputTopic)
}

func TestGroupTransactSessionRebalanceAbortsTransaction(t *testing.T) {
	const (
		inputTopic  = "group-tx-rebalance-input"
		outputTopic = "group-tx-rebalance-output"
		group       = "group-tx-rebalance-group"
	)

	cluster := newTestKafkaCluster(t, inputTopic, outputTopic)
	produceTestRecords(t, cluster, inputTopic, 1)

	hook := newGroupSessionHook()
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	revoked := make(chan struct{})
	defer func() {
		select {
		case <-releaseHandler:
		default:
			close(releaseHandler)
		}
	}()

	var (
		handlerStartedOnce sync.Once
		revokedOnce        sync.Once
	)

	session := newTestGroupTransactSession(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(inputTopic),
			kgo.ConsumerGroup(group),
			kgo.TransactionalID("group-tx-rebalance-transaction"),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			kgo.Balancers(kgo.RangeBalancer()),
			kgo.OnPartitionsRevoked(func(context.Context, *kgo.Client, map[string][]int32) {
				revokedOnce.Do(func() { close(revoked) })
			}),
		),
		WithHooks(hook),
		WithMaxPollRecords(1),
		WithPollInterval(testPollInterval),
		WithGroupTransactSessionBatchHandler(func(
			ctx context.Context,
			records []*kgo.Record,
			tx *Tx,
		) error {
			if len(records) != 1 {
				return fmt.Errorf("handler records = %d, want 1", len(records))
			}

			if err := tx.ProduceSync(ctx, &kgo.Record{
				Topic: outputTopic,
				Value: []byte("must-abort"),
			}); err != nil {
				return err
			}

			handlerStartedOnce.Do(func() { close(handlerStarted) })
			<-releaseHandler
			return nil
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := runTestGroupTransactSession(ctx, session)
	waitTestSignal(t, handlerStarted, "group transaction handler")

	peer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(inputTopic),
		kgo.ConsumerGroup(group),
		kgo.Balancers(kgo.RangeBalancer()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("create rebalance peer: %v", err)
	}
	defer peer.Close()

	peerCtx, peerCancel := context.WithCancel(context.Background())
	defer peerCancel()
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		peer.PollRecords(peerCtx, 1)
	}()

	waitTestSignal(t, revoked, "partition revocation")
	close(releaseHandler)

	event := waitGroupSessionTransactionEvent(t, hook)
	if event.outcome != TransactionOutcomeAbort {
		t.Fatalf("transaction outcome = %q, want %q", event.outcome, TransactionOutcomeAbort)
	}
	if event.err != nil {
		t.Fatalf("transaction end error = %v, want nil", event.err)
	}

	cancel()
	if err := waitTestGroupTransactSession(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
	}

	peerCancel()
	select {
	case <-peerDone:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for rebalance peer poll")
	}

	assertNoCommittedTestRecords(t, cluster, outputTopic)
}

func TestNewGroupTransactSessionRequiresHandler(t *testing.T) {
	const inputTopic = "group-tx-requires-handler"

	cluster := newTestKafkaCluster(t, inputTopic)

	session, err := NewGroupTransactSession(
		WithKafkaOptions(
			kgo.SeedBrokers(cluster.ListenAddrs()...),
			kgo.ConsumeTopics(inputTopic),
			kgo.ConsumerGroup("group-tx-requires-handler-group"),
			kgo.TransactionalID("group-tx-requires-handler-transaction"),
		),
	)
	if session != nil {
		t.Fatal("group transaction session != nil, want nil")
	}
	if err == nil || err.Error() != "kafka: group transact session requires batch handler" {
		t.Fatalf("new group transaction session error = %v, want missing handler error", err)
	}
}

type groupSessionCloseHook struct {
	calls atomic.Int32
}

func (h *groupSessionCloseHook) OnGroupTransactSessionClosed(*GroupTransactSession) {
	h.calls.Add(1)
}

func TestGroupTransactSessionPing(t *testing.T) {
	const (
		topic = "ping-session"
		group = "ping-session-group"
	)

	cluster := newTestKafkaCluster(t, topic)
	session := newTestGroupTransactSession(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.TransactionalID("ping-session-transaction"),
		),
		WithGroupTransactSessionBatchHandler(func(context.Context, []*kgo.Record, *Tx) error {
			return nil
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := session.Ping(ctx); err != nil {
		t.Fatalf("ping group transaction session: %v", err)
	}
	if err := session.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown group transaction session: %v", err)
	}

	err := session.Ping(ctx)
	if err == nil {
		t.Fatal("ping closed group transaction session error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "kafka: ping group transact session") {
		t.Fatalf("ping closed group transaction session error = %q, want xkafka prefix", err)
	}
}

func TestGroupTransactSessionShutdownStopsFetchLoop(t *testing.T) {
	const (
		topic = "shutdown-session-fetch-loop"
		group = "shutdown-session-fetch-loop-group"
	)

	cluster := newTestKafkaCluster(t, topic)
	session := newTestGroupTransactSession(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.TransactionalID("shutdown-session-fetch-loop-transaction"),
		),
		WithPollInterval(time.Hour),
		WithGroupTransactSessionBatchHandler(func(context.Context, []*kgo.Record, *Tx) error {
			return nil
		}),
	)

	started := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		close(started)
		errCh <- session.HandleFetches(context.Background())
	}()
	waitTestSignal(t, started, "group transaction fetch loop start")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := session.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown group transaction session: %v", err)
	}

	if err := waitTestGroupTransactSession(t, errCh); err != nil {
		t.Fatalf("handle fetches after group transaction session shutdown = %v, want nil", err)
	}
}

func TestGroupTransactSessionConcurrentShutdown(t *testing.T) {
	const (
		topic = "concurrent-session-shutdown"
		group = "concurrent-session-shutdown-group"
	)

	cluster := newTestKafkaCluster(t, topic)
	hook := &groupSessionCloseHook{}
	session := newTestGroupTransactSession(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.TransactionalID("concurrent-session-shutdown-transaction"),
		),
		WithHooks(hook),
		WithGroupTransactSessionBatchHandler(func(context.Context, []*kgo.Record, *Tx) error {
			return nil
		}),
	)

	runConcurrentShutdown(t, 16, func() error {
		return session.Shutdown(context.Background())
	})

	if got := hook.calls.Load(); got != 1 {
		t.Fatalf("group transaction session close hook calls = %d, want 1", got)
	}
}
