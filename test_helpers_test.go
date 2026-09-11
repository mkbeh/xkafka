package xkafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const testTimeout = 5 * time.Second

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
