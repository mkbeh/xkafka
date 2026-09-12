package xkafka_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type testShareAckEvent struct {
	outcome ShareAckOutcome
	count   int
}

type testShareRuntimeHook struct {
	mu sync.Mutex

	acks       []testShareAckEvent
	flushCalls int
	flushErrs  []error
	flushTimes []time.Time
	flushCh    chan struct{}
}

func newTestShareRuntimeHook() *testShareRuntimeHook {
	return &testShareRuntimeHook{
		flushCh: make(chan struct{}, 16),
	}
}

func (h *testShareRuntimeHook) OnShareAck(
	_ context.Context,
	outcome ShareAckOutcome,
	count int,
) {
	h.mu.Lock()
	h.acks = append(h.acks, testShareAckEvent{
		outcome: outcome,
		count:   count,
	})
	h.mu.Unlock()
}

func (h *testShareRuntimeHook) OnShareAckFlush(
	_ context.Context,
	_ time.Duration,
	err error,
) {
	h.mu.Lock()
	h.flushCalls++
	h.flushErrs = append(h.flushErrs, err)
	h.flushTimes = append(h.flushTimes, time.Now())
	h.mu.Unlock()

	h.flushCh <- struct{}{}
}

func (h *testShareRuntimeHook) snapshot() (
	acks []testShareAckEvent,
	flushCalls int,
	flushErrs []error,
	flushTimes []time.Time,
) {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]testShareAckEvent(nil), h.acks...),
		h.flushCalls,
		append([]error(nil), h.flushErrs...),
		append([]time.Time(nil), h.flushTimes...)
}

func TestClientShareGroupAccept(t *testing.T) {
	const (
		topic = "share-accept"
		group = "share-accept-group"
	)

	cluster := newTestKafkaCluster(t, topic)
	hook := newTestShareRuntimeHook()

	var deliveryCounts []int32
	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ShareGroup(group),
			kgo.FetchMaxWait(50*time.Millisecond),
		),
		WithHooks(hook),
		WithPollInterval(testPollInterval),
		WithBatchHandler(func(_ context.Context, records []*kgo.Record) error {
			for _, record := range records {
				deliveryCounts = append(deliveryCounts, record.DeliveryCount())
			}
			return nil
		}),
	)

	establishTestShareGroup(t, client)
	produceTestRecords(t, cluster, topic, 1)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runTestHandleFetches(ctx, client)

	waitTestSignal(t, hook.flushCh, "share accept flush")
	cancel()

	if err := waitTestHandleFetches(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
	}

	if want := []int32{1}; !reflect.DeepEqual(deliveryCounts, want) {
		t.Fatalf("delivery counts = %v, want %v", deliveryCounts, want)
	}

	acks, flushCalls, flushErrs, _ := hook.snapshot()
	if want := []testShareAckEvent{{outcome: ShareAckAccept, count: 1}}; !reflect.DeepEqual(acks, want) {
		t.Fatalf("share ack events = %#v, want %#v", acks, want)
	}
	if flushCalls != 1 {
		t.Fatalf("share ack flush calls = %d, want 1", flushCalls)
	}
	if len(flushErrs) != 1 || flushErrs[0] != nil {
		t.Fatalf("share ack flush errors = %v, want [<nil>]", flushErrs)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), testTimeout)
	defer shutdownCancel()
	if err := client.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown share consumer: %v", err)
	}

	assertNoTestShareRecords(t, cluster, topic, group)
}

func TestClientShareGroupReleaseRedelivery(t *testing.T) {
	const (
		topic = "share-release"
		group = "share-release-group"
	)

	cluster := newTestKafkaCluster(t, topic)
	hook := newTestShareRuntimeHook()
	handleErr := errors.New("handle failed")

	var (
		offsets        []int64
		deliveryCounts []int32
	)
	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ShareGroup(group),
			kgo.FetchMaxWait(50*time.Millisecond),
		),
		WithHooks(hook),
		WithShareReleaseTimeout(0),
		WithPollInterval(testPollInterval),
		WithBatchHandler(func(_ context.Context, records []*kgo.Record) error {
			if len(records) != 1 {
				return errors.New("unexpected share batch size")
			}

			record := records[0]
			offsets = append(offsets, record.Offset)
			deliveryCounts = append(deliveryCounts, record.DeliveryCount())

			if len(deliveryCounts) == 1 {
				return handleErr
			}

			return nil
		}),
	)

	establishTestShareGroup(t, client)
	produceTestRecords(t, cluster, topic, 1)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runTestHandleFetches(ctx, client)

	waitTestSignal(t, hook.flushCh, "share release flush")
	waitTestSignal(t, hook.flushCh, "share accept flush")
	cancel()

	if err := waitTestHandleFetches(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
	}

	if want := []int64{0, 0}; !reflect.DeepEqual(offsets, want) {
		t.Fatalf("share offsets = %v, want %v", offsets, want)
	}
	if want := []int32{1, 2}; !reflect.DeepEqual(deliveryCounts, want) {
		t.Fatalf("delivery counts = %v, want %v", deliveryCounts, want)
	}

	acks, flushCalls, flushErrs, _ := hook.snapshot()
	wantAcks := []testShareAckEvent{
		{outcome: ShareAckRelease, count: 1},
		{outcome: ShareAckAccept, count: 1},
	}
	if !reflect.DeepEqual(acks, wantAcks) {
		t.Fatalf("share ack events = %#v, want %#v", acks, wantAcks)
	}
	if flushCalls != 2 {
		t.Fatalf("share ack flush calls = %d, want 2", flushCalls)
	}
	for i, err := range flushErrs {
		if err != nil {
			t.Fatalf("share ack flush %d error = %v, want nil", i, err)
		}
	}
}

func TestClientShareGroupRejectAfterDeliveries(t *testing.T) {
	const (
		topic = "share-reject"
		group = "share-reject-group"
	)

	cluster := newTestKafkaCluster(t, topic)
	hook := newTestShareRuntimeHook()
	handleErr := errors.New("handle failed")

	var deliveryCounts []int32
	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ShareGroup(group),
			kgo.FetchMaxWait(50*time.Millisecond),
		),
		WithHooks(hook),
		WithShareRejectAfterDeliveries(3),
		WithShareReleaseTimeout(0),
		WithPollInterval(testPollInterval),
		WithBatchHandler(func(_ context.Context, records []*kgo.Record) error {
			if len(records) != 1 {
				return errors.New("unexpected share batch size")
			}

			deliveryCounts = append(deliveryCounts, records[0].DeliveryCount())
			return handleErr
		}),
	)

	establishTestShareGroup(t, client)
	produceTestRecords(t, cluster, topic, 1)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runTestHandleFetches(ctx, client)

	waitTestSignal(t, hook.flushCh, "first share release flush")
	waitTestSignal(t, hook.flushCh, "second share release flush")
	waitTestSignal(t, hook.flushCh, "share reject flush")
	cancel()

	if err := waitTestHandleFetches(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
	}

	if want := []int32{1, 2, 3}; !reflect.DeepEqual(deliveryCounts, want) {
		t.Fatalf("delivery counts = %v, want %v", deliveryCounts, want)
	}

	acks, flushCalls, flushErrs, _ := hook.snapshot()
	wantAcks := []testShareAckEvent{
		{outcome: ShareAckRelease, count: 1},
		{outcome: ShareAckRelease, count: 1},
		{outcome: ShareAckReject, count: 1},
	}
	if !reflect.DeepEqual(acks, wantAcks) {
		t.Fatalf("share ack events = %#v, want %#v", acks, wantAcks)
	}
	if flushCalls != 3 {
		t.Fatalf("share ack flush calls = %d, want 3", flushCalls)
	}
	for i, err := range flushErrs {
		if err != nil {
			t.Fatalf("share ack flush %d error = %v, want nil", i, err)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), testTimeout)
	defer shutdownCancel()
	if err := client.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown share consumer: %v", err)
	}

	assertNoTestShareRecords(t, cluster, topic, group)
}

func TestClientShareGroupMixedReleaseReject(t *testing.T) {
	const (
		topic = "share-mixed-release-reject"
		group = "share-mixed-release-reject-group"
	)

	cluster := newTestKafkaCluster(t, topic)
	hook := newTestShareRuntimeHook()
	handleErr := errors.New("handle failed")
	secondDelivery := make(chan struct{})
	continueSecond := make(chan struct{})
	done := make(chan struct{})

	var deliveries []map[int64]int32
	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ShareGroup(group),
			kgo.ShareMaxRecords(2),
			kgo.ShareMaxRecordsStrict(),
			kgo.FetchMaxWait(50*time.Millisecond),
		),
		WithHooks(hook),
		WithShareRejectAfterDeliveries(3),
		WithShareReleaseTimeout(0),
		WithPollInterval(testPollInterval),
		WithBatchHandler(func(_ context.Context, records []*kgo.Record) error {
			batch := make(map[int64]int32, len(records))
			for _, record := range records {
				batch[record.Offset] = record.DeliveryCount()
			}
			deliveries = append(deliveries, batch)

			switch len(deliveries) {
			case 1:
				return handleErr
			case 2:
				close(secondDelivery)
				<-continueSecond
				return handleErr
			case 3:
				return handleErr
			case 4:
				close(done)
				return nil
			default:
				return errors.New("unexpected extra share delivery")
			}
		}),
	)

	establishTestShareGroup(t, client)
	produceTestRecords(t, cluster, topic, 1)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runTestHandleFetches(ctx, client)

	waitTestSignal(t, secondDelivery, "second delivery")
	produceTestRecords(t, cluster, topic, 1)
	close(continueSecond)

	waitTestSignal(t, done, "mixed release/reject redelivery")
	for range 4 {
		waitTestSignal(t, hook.flushCh, "mixed share ack flush")
	}
	cancel()

	if err := waitTestHandleFetches(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
	}

	wantDeliveries := []map[int64]int32{
		{0: 1},
		{0: 2},
		{0: 3, 1: 1},
		{1: 2},
	}
	if !reflect.DeepEqual(deliveries, wantDeliveries) {
		t.Fatalf("share deliveries = %#v, want %#v", deliveries, wantDeliveries)
	}

	acks, flushCalls, flushErrs, _ := hook.snapshot()
	wantAcks := []testShareAckEvent{
		{outcome: ShareAckRelease, count: 1},
		{outcome: ShareAckRelease, count: 1},
		{outcome: ShareAckRelease, count: 1},
		{outcome: ShareAckReject, count: 1},
		{outcome: ShareAckAccept, count: 1},
	}
	if !reflect.DeepEqual(acks, wantAcks) {
		t.Fatalf("share ack events = %#v, want %#v", acks, wantAcks)
	}
	if flushCalls != 4 {
		t.Fatalf("share ack flush calls = %d, want 4", flushCalls)
	}
	for i, err := range flushErrs {
		if err != nil {
			t.Fatalf("share ack flush %d error = %v, want nil", i, err)
		}
	}
}

func TestClientShareGroupReleaseTimeout(t *testing.T) {
	const (
		topic          = "share-release-timeout"
		group          = "share-release-timeout-group"
		releaseTimeout = 100 * time.Millisecond
	)

	cluster := newTestKafkaCluster(t, topic)
	hook := newTestShareRuntimeHook()
	var (
		handledAt   time.Time
		handledOnce sync.Once
	)

	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ShareGroup(group),
			kgo.FetchMaxWait(50*time.Millisecond),
		),
		WithHooks(hook),
		WithShareReleaseTimeout(releaseTimeout),
		WithPollInterval(testPollInterval),
		WithBatchHandler(func(_ context.Context, _ []*kgo.Record) error {
			handledOnce.Do(func() { handledAt = time.Now() })
			return errors.New("handle failed")
		}),
	)

	establishTestShareGroup(t, client)
	produceTestRecords(t, cluster, topic, 1)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runTestHandleFetches(ctx, client)

	waitTestSignal(t, hook.flushCh, "share release flush")
	cancel()
	if err := waitTestHandleFetches(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
	}

	acks, flushCalls, flushErrs, flushTimes := hook.snapshot()
	if len(acks) == 0 || acks[0] != (testShareAckEvent{outcome: ShareAckRelease, count: 1}) {
		t.Fatalf("first share ack event = %#v, want release", acks)
	}
	if flushCalls < 1 {
		t.Fatalf("share ack flush calls = %d, want at least 1", flushCalls)
	}
	if len(flushErrs) == 0 || flushErrs[0] != nil {
		t.Fatalf("first share ack flush error = %v, want nil", flushErrs)
	}
	if len(flushTimes) == 0 {
		t.Fatal("share ack flush times is empty")
	}
	if elapsed := flushTimes[0].Sub(handledAt); elapsed < releaseTimeout {
		t.Fatalf("share release delay = %s, want at least %s", elapsed, releaseTimeout)
	}
}

func TestClientShareGroupReleaseCancellationStillFlushes(t *testing.T) {
	const (
		topic = "share-release-cancel"
		group = "share-release-cancel-group"
	)

	cluster := newTestKafkaCluster(t, topic)
	hook := newTestShareRuntimeHook()
	handleErr := errors.New("handle failed")
	handled := make(chan struct{})
	var handledOnce sync.Once

	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ShareGroup(group),
			kgo.FetchMaxWait(50*time.Millisecond),
		),
		WithHooks(hook),
		WithShareReleaseTimeout(time.Hour),
		WithPollInterval(testPollInterval),
		WithBatchHandler(func(_ context.Context, _ []*kgo.Record) error {
			handledOnce.Do(func() { close(handled) })
			return handleErr
		}),
	)

	establishTestShareGroup(t, client)
	produceTestRecords(t, cluster, topic, 1)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runTestHandleFetches(ctx, client)

	waitTestSignal(t, handled, "share handler")
	cancel()
	waitTestSignal(t, hook.flushCh, "share flush after cancellation")

	if err := waitTestHandleFetches(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
	}

	acks, flushCalls, _, _ := hook.snapshot()
	if want := []testShareAckEvent{{outcome: ShareAckRelease, count: 1}}; !reflect.DeepEqual(acks, want) {
		t.Fatalf("share ack events = %#v, want %#v", acks, want)
	}
	if flushCalls != 1 {
		t.Fatalf("share ack flush calls = %d, want 1", flushCalls)
	}
}

func TestClientShareGroupMaxRecordsStrict(t *testing.T) {
	const (
		topic       = "share-max-records-strict"
		group       = "share-max-records-strict-group"
		recordCount = 5
		maxRecords  = 2
	)

	cluster := newTestKafkaCluster(t, topic)
	hook := newTestShareRuntimeHook()
	batchCh := make(chan int)

	client := newTestClient(
		t,
		cluster,
		WithKafkaOptions(
			kgo.ConsumeTopics(topic),
			kgo.ShareGroup(group),
			kgo.ShareMaxRecords(maxRecords),
			kgo.ShareMaxRecordsStrict(),
			kgo.FetchMaxWait(50*time.Millisecond),
		),
		WithHooks(hook),
		WithPollInterval(testPollInterval),
		WithBatchHandler(func(_ context.Context, records []*kgo.Record) error {
			batchCh <- len(records)
			return nil
		}),
	)

	establishTestShareGroup(t, client)
	produceTestRecords(t, cluster, topic, recordCount)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := runTestHandleFetches(ctx, client)

	var (
		batchSizes []int
		handled    int
	)
	for handled < recordCount {
		select {
		case batchSize := <-batchCh:
			batchSizes = append(batchSizes, batchSize)
			handled += batchSize
			if batchSize < 1 || batchSize > maxRecords {
				t.Fatalf("share batch size = %d, want 1..%d", batchSize, maxRecords)
			}
			waitTestSignal(t, hook.flushCh, "share batch ack flush")
		case <-time.After(testTimeout):
			t.Fatalf("timed out waiting for strict share batches: handled %d/%d", handled, recordCount)
		}
	}

	cancel()
	if err := waitTestHandleFetches(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("handle fetches error = %v, want %v", err, context.Canceled)
	}

	if handled != recordCount {
		t.Fatalf("handled records = %d, want %d", handled, recordCount)
	}
	if len(batchSizes) < 3 {
		t.Fatalf("share batches = %v, want at least 3 batches for %d records with max %d", batchSizes, recordCount, maxRecords)
	}
}
