package xkafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const testTimeout = 5 * time.Second

func newTestKafkaCluster(t *testing.T, topics ...string) *kfake.Cluster {
	t.Helper()

	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, topics...))
	if err != nil {
		t.Fatalf("create fake Kafka cluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	return cluster
}

func newTestClient(t *testing.T, cluster *kfake.Cluster, opts ...Opt) *Client {
	t.Helper()

	clientOpts := []Opt{
		WithKafkaOptions(kgo.SeedBrokers(cluster.ListenAddrs()...)),
	}
	clientOpts = append(clientOpts, opts...)

	client, err := NewClient(clientOpts...)
	if err != nil {
		t.Fatalf("create xkafka client: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		if err := client.Shutdown(ctx); err != nil {
			t.Errorf("shutdown xkafka client: %v", err)
		}
	})

	return client
}

func newTestGroupTransactSession(
	t *testing.T,
	cluster *kfake.Cluster,
	opts ...Opt,
) *GroupTransactSession {
	t.Helper()

	sessionOpts := []Opt{
		WithKafkaOptions(kgo.SeedBrokers(cluster.ListenAddrs()...)),
	}
	sessionOpts = append(sessionOpts, opts...)

	session, err := NewGroupTransactSession(sessionOpts...)
	if err != nil {
		t.Fatalf("create xkafka group transaction session: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		if err := session.Shutdown(ctx); err != nil {
			t.Errorf("shutdown xkafka group transaction session: %v", err)
		}
	})

	return session
}

func failNextEndTxn(cluster *kfake.Cluster) {
	cluster.ControlKey(int16(kmsg.EndTxn), func(kmsg.Request) (kmsg.Response, error, bool) {
		return nil, errors.New("forced end transaction failure"), true
	})
}

func consumeTestRecords(
	t *testing.T,
	cluster *kfake.Cluster,
	topic string,
	count int,
) []*kgo.Record {
	t.Helper()

	return consumeTestRecordsWithOpts(t, cluster, topic, count)
}

func consumeCommittedTestRecords(
	t *testing.T,
	cluster *kfake.Cluster,
	topic string,
	count int,
) []*kgo.Record {
	t.Helper()

	return consumeTestRecordsWithOpts(
		t,
		cluster,
		topic,
		count,
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
}

func consumeTestRecordsWithOpts(
	t *testing.T,
	cluster *kfake.Cluster,
	topic string,
	count int,
	opts ...kgo.Opt,
) []*kgo.Record {
	t.Helper()

	consumerOpts := []kgo.Opt{
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {
				0: kgo.NewOffset().AtStart(),
			},
		}),
	}
	consumerOpts = append(consumerOpts, opts...)

	consumer, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		t.Fatalf("create Kafka consumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	records := make([]*kgo.Record, 0, count)
	for len(records) < count {
		fetches := consumer.PollRecords(ctx, count-len(records))
		if err := fetches.Err(); err != nil {
			t.Fatalf("consume Kafka records: %v", err)
		}

		records = append(records, fetches.Records()...)
	}

	return records
}

func assertNoCommittedTestRecords(
	t *testing.T,
	cluster *kfake.Cluster,
	topic string,
) {
	t.Helper()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {
				0: kgo.NewOffset().AtStart(),
			},
		}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxWait(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("create read committed consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	fetches := consumer.PollRecords(ctx, 1)
	if records := fetches.Records(); len(records) != 0 {
		t.Fatalf("read committed records = %d, want 0", len(records))
	}
	for _, fetchErr := range fetches.Errors() {
		if errors.Is(fetchErr.Err, context.DeadlineExceeded) || errors.Is(fetchErr.Err, context.Canceled) {
			continue
		}

		t.Fatalf("poll read committed records: %v", fetchErr.Err)
	}
}

func establishTestShareGroup(t *testing.T, client *Client) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	fetches := client.cl.Client().PollRecords(ctx, 1)
	if len(fetches.Records()) != 0 {
		t.Fatalf("share group setup returned %d records, want 0", len(fetches.Records()))
	}
	for _, fetchErr := range fetches.Errors() {
		if errors.Is(fetchErr.Err, context.DeadlineExceeded) || errors.Is(fetchErr.Err, context.Canceled) {
			continue
		}

		t.Fatalf("establish share group: %v", fetchErr.Err)
	}
}

func assertNoTestShareRecords(
	t *testing.T,
	cluster *kfake.Cluster,
	topic string,
	group string,
) {
	t.Helper()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("create share consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	fetches := consumer.PollRecords(ctx, 1)
	if records := fetches.Records(); len(records) != 0 {
		t.Fatalf("share group redelivered %d records, want 0", len(records))
	}
	for _, fetchErr := range fetches.Errors() {
		if errors.Is(fetchErr.Err, context.DeadlineExceeded) || errors.Is(fetchErr.Err, context.Canceled) {
			continue
		}

		t.Fatalf("poll share group: %v", fetchErr.Err)
	}
}

type testProduceResult struct {
	record *kgo.Record
	err    error
}

func waitTestProduceResult(t *testing.T, ch <-chan testProduceResult) testProduceResult {
	t.Helper()

	select {
	case result := <-ch:
		return result
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for produce result")
		return testProduceResult{}
	}
}

func recordHeaderValue(record *kgo.Record, key string) string {
	for _, header := range record.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}

	return ""
}

func produceTestRecords(
	t *testing.T,
	cluster *kfake.Cluster,
	topic string,
	count int,
) {
	t.Helper()

	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("create Kafka producer: %v", err)
	}
	t.Cleanup(producer.Close)

	records := make([]*kgo.Record, count)
	for i := range count {
		records[i] = &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	if err := producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("produce Kafka records: %v", err)
	}
}

func runTestHandleFetches(ctx context.Context, client *Client) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.HandleFetches(ctx)
	}()
	return errCh
}

func waitTestHandleFetches(t *testing.T, errCh <-chan error) error {
	t.Helper()

	select {
	case err := <-errCh:
		return err
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for HandleFetches")
		return nil
	}
}

func runTestGroupTransactSession(
	ctx context.Context,
	session *GroupTransactSession,
) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- session.HandleFetches(ctx)
	}()
	return errCh
}

func waitTestGroupTransactSession(t *testing.T, errCh <-chan error) error {
	t.Helper()

	select {
	case err := <-errCh:
		return err
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for GroupTransactSession.HandleFetches")
		return nil
	}
}

func waitTestSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func waitTestCondition(t *testing.T, name string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", name)
}

type testClientConn struct {
	allowRebalanceCalls int
}

func (*testClientConn) Produce(context.Context, *kgo.Record, func(*kgo.Record, error)) {}

func (*testClientConn) TryProduce(context.Context, *kgo.Record, func(*kgo.Record, error)) {}

func (*testClientConn) ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults {
	return nil
}

func (*testClientConn) PollRecords(context.Context, int) kgo.Fetches {
	return nil
}

func (c *testClientConn) AllowRebalance() {
	c.allowRebalanceCalls++
}

func runConcurrentShutdown(t *testing.T, count int, shutdown func() error) {
	t.Helper()

	var wg sync.WaitGroup
	errCh := make(chan error, count)
	wg.Add(count)

	for range count {
		go func() {
			defer wg.Done()
			errCh <- shutdown()
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent shutdown: %v", err)
		}
	}
}
