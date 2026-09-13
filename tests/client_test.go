package xkafka_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type creationHook struct {
	client  *Client
	session *GroupTransactSession
}

func (h *creationHook) OnNewClient(client *Client) {
	h.client = client
}

func (h *creationHook) OnNewGroupTransactSession(session *GroupTransactSession) {
	h.session = session
}

type clientCloseHook struct {
	calls atomic.Int32
}

func (h *clientCloseHook) OnClientClosed(*Client) {
	h.calls.Add(1)
}

func TestConstructorsCallLifecycleHooks(t *testing.T) {
	const (
		topic = "constructor-hooks"
		group = "constructor-hooks-group"
	)

	cluster := newTestKafkaCluster(t, topic)

	clientHook := &creationHook{}
	client := newTestClient(t, cluster, WithHooks(clientHook))
	if clientHook.client != client {
		t.Fatalf("new client hook client = %p, want %p", clientHook.client, client)
	}

	sessionHook := &creationHook{}
	session := newTestGroupTransactSession(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.TransactionalID("constructor-hooks-transaction"),
		),
		WithHooks(sessionHook),
		WithGroupTransactSessionBatchHandler(func(context.Context, []*kgo.Record, *Tx) error {
			return nil
		}),
	)
	if sessionHook.session != session {
		t.Fatalf("new group transaction session hook session = %p, want %p", sessionHook.session, session)
	}
}

func TestClientPing(t *testing.T) {
	cluster := newTestKafkaCluster(t, "ping-client")
	client := newTestClient(t, cluster)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("ping client: %v", err)
	}
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown client: %v", err)
	}

	err := client.Ping(ctx)
	if err == nil {
		t.Fatal("ping closed client error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "kafka: ping client") {
		t.Fatalf("ping closed client error = %q, want xkafka prefix", err)
	}
}

func TestClientShutdownStopsRetryWait(t *testing.T) {
	const (
		topic = "shutdown-retry-wait"
		group = "shutdown-retry-wait-group"
	)

	cluster := newTestKafkaCluster(t, topic)
	produceTestRecords(t, cluster, topic, 1)

	handlerCalled := make(chan struct{})
	var handlerCalls atomic.Int32
	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
		),
		WithSuspendProcessingTimeout(time.Hour),
		WithPollInterval(testPollInterval),
		WithBatchHandler(func(context.Context, []*kgo.Record) error {
			if handlerCalls.Add(1) == 1 {
				close(handlerCalled)
			}
			return errors.New("handle failed")
		}),
	)

	errCh := runTestHandleFetches(context.Background(), client)
	waitTestSignal(t, handlerCalled, "consumer handler")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown client: %v", err)
	}

	if err := waitTestHandleFetches(t, errCh); err != nil {
		t.Fatalf("handle fetches after shutdown during retry wait = %v, want nil", err)
	}
	if got := handlerCalls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestClientShareGroupShutdownFlushesAcks(t *testing.T) {
	const (
		topic = "shutdown-share-acks"
		group = "shutdown-share-acks-group"
	)

	cluster := newTestKafkaCluster(t, topic)
	hook := newTestShareRuntimeHook()
	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ShareGroup(group),
		),
		WithHooks(hook),
		WithBatchHandler(func(context.Context, []*kgo.Record) error { return nil }),
	)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown share consumer: %v", err)
	}

	_, flushCalls, flushErrs, _ := hook.snapshot()
	if flushCalls != 1 {
		t.Fatalf("share ack flush calls = %d, want 1", flushCalls)
	}
	if len(flushErrs) != 1 || flushErrs[0] != nil {
		t.Fatalf("share ack flush errors = %v, want [<nil>]", flushErrs)
	}
}

func TestClientConcurrentShutdown(t *testing.T) {
	cluster := newTestKafkaCluster(t, "concurrent-client-shutdown")
	hook := &clientCloseHook{}
	client := newTestClient(t, cluster, WithHooks(hook))

	runConcurrentShutdown(t, 16, func() error {
		return client.Shutdown(context.Background())
	})

	if got := hook.calls.Load(); got != 1 {
		t.Fatalf("client close hook calls = %d, want 1", got)
	}
}

func TestClientShutdownFlushesAsyncProduce(t *testing.T) {
	const topic = "shutdown-flush-produce"

	cluster := newTestKafkaCluster(t, topic)
	client, err := NewClient(
		WithKafkaOptions(
			kgo.SeedBrokers(cluster.ListenAddrs()...),
			kgo.ManualFlushing(),
		),
	)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	resultCh := make(chan testProduceResult, 1)
	client.Produce(context.Background(), &kgo.Record{
		Topic: topic,
		Value: []byte("flushed-on-shutdown"),
	}, func(record *kgo.Record, err error) {
		resultCh <- testProduceResult{record: record, err: err}
	})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown client: %v", err)
	}

	result := waitTestProduceResult(t, resultCh)
	if result.err != nil {
		t.Fatalf("async produce error = %v, want nil", result.err)
	}

	records := consumeTestRecords(t, cluster, topic, 1)
	if got := string(records[0].Value); got != "flushed-on-shutdown" {
		t.Fatalf("produced value = %q, want flushed-on-shutdown", got)
	}
}
