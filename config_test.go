package xkafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type configTestHook struct{ id int }

func TestNewClientDefaults(t *testing.T) {
	cl, err := newClient()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	if cl.maxPollRecords != 100 {
		t.Fatalf("max poll records = %d, want 100", cl.maxPollRecords)
	}
	if cl.maxHandlerRetries != -1 {
		t.Fatalf("max handler retries = %d, want -1", cl.maxHandlerRetries)
	}
	if cl.pollInterval != time.Second {
		t.Fatalf("poll interval = %s, want 1s", cl.pollInterval)
	}
	if cl.suspendProcessingTimeout != 30*time.Second {
		t.Fatalf("suspend processing timeout = %s, want 30s", cl.suspendProcessingTimeout)
	}
	if cl.suspendCommittingTimeout != 10*time.Second {
		t.Fatalf("suspend committing timeout = %s, want 10s", cl.suspendCommittingTimeout)
	}
	if cl.shareRejectAfterDeliveries != 0 {
		t.Fatalf("share reject after deliveries = %d, want 0", cl.shareRejectAfterDeliveries)
	}
	if cl.shareReleaseTimeout != 0 {
		t.Fatalf("share release timeout = %s, want 0", cl.shareReleaseTimeout)
	}
	if cl.exitCh == nil {
		t.Fatal("exit channel is nil")
	}
	if cl.formatter == nil {
		t.Fatal("record formatter is nil")
	}
	if cl.defaultPromise == nil {
		t.Fatal("default promise is nil")
	}
}

func TestClientOptions(t *testing.T) {
	cl, err := newClient(
		WithMaxPollRecords(-7),
		WithMaxRetries(2),
		WithPollInterval(25*time.Millisecond),
		WithSuspendProcessingTimeout(0),
		WithSuspendCommittingTimeout(50*time.Millisecond),
		WithShareRejectAfterDeliveries(3),
		WithShareReleaseTimeout(75*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	if cl.maxPollRecords != -7 {
		t.Fatalf("max poll records = %d, want -7", cl.maxPollRecords)
	}
	if cl.maxHandlerRetries != 2 {
		t.Fatalf("max handler retries = %d, want 2", cl.maxHandlerRetries)
	}
	if cl.pollInterval != 25*time.Millisecond {
		t.Fatalf("poll interval = %s, want 25ms", cl.pollInterval)
	}
	if cl.suspendProcessingTimeout != 0 {
		t.Fatalf("suspend processing timeout = %s, want 0", cl.suspendProcessingTimeout)
	}
	if cl.suspendCommittingTimeout != 50*time.Millisecond {
		t.Fatalf("suspend committing timeout = %s, want 50ms", cl.suspendCommittingTimeout)
	}
	if cl.shareRejectAfterDeliveries != 3 {
		t.Fatalf("share reject after deliveries = %d, want 3", cl.shareRejectAfterDeliveries)
	}
	if cl.shareReleaseTimeout != 75*time.Millisecond {
		t.Fatalf("share release timeout = %s, want 75ms", cl.shareReleaseTimeout)
	}
}

func TestClientOptionsIgnoreInvalidValues(t *testing.T) {
	cl, err := newClient(
		WithMaxRetries(-2),
		WithPollInterval(0),
		WithSuspendProcessingTimeout(-time.Second),
		WithSuspendCommittingTimeout(-time.Second),
		WithShareRejectAfterDeliveries(-1),
		WithShareReleaseTimeout(-time.Second),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	if cl.maxHandlerRetries != -1 {
		t.Fatalf("max handler retries = %d, want default -1", cl.maxHandlerRetries)
	}
	if cl.pollInterval != time.Second {
		t.Fatalf("poll interval = %s, want default 1s", cl.pollInterval)
	}
	if cl.suspendProcessingTimeout != 30*time.Second {
		t.Fatalf("suspend processing timeout = %s, want default 30s", cl.suspendProcessingTimeout)
	}
	if cl.suspendCommittingTimeout != 10*time.Second {
		t.Fatalf("suspend committing timeout = %s, want default 10s", cl.suspendCommittingTimeout)
	}
	if cl.shareRejectAfterDeliveries != 0 {
		t.Fatalf("share reject after deliveries = %d, want default 0", cl.shareRejectAfterDeliveries)
	}
	if cl.shareReleaseTimeout != 0 {
		t.Fatalf("share release timeout = %s, want default 0", cl.shareReleaseTimeout)
	}
}

func TestClientName(t *testing.T) {
	client, err := NewClient(
		WithKafkaOptions(kgo.ClientID("native-client-id")),
		WithName("  orders  "),
	)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown client: %v", err)
		}
	})

	if got := client.Name(); got != "orders" {
		t.Fatalf("client name = %q, want orders", got)
	}
	if got, _ := client.cl.Client().OptValue(kgo.ClientID).(string); got != "orders" {
		t.Fatalf("Kafka client ID = %q, want orders", got)
	}

	var nilClient *Client
	if got := nilClient.Name(); got != "" {
		t.Fatalf("nil client name = %q, want empty", got)
	}

	var nilSession *GroupTransactSession
	if got := nilSession.Name(); got != "" {
		t.Fatalf("nil session name = %q, want empty", got)
	}
}

func TestOptionsSnapshotInputs(t *testing.T) {
	kafkaOpts := []kgo.Opt{kgo.ClientID("first")}
	kafkaOpt := WithKafkaOptions(kafkaOpts...)
	kafkaOpts[0] = kgo.ClientID("second")

	cl, err := newClient(kafkaOpt)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	conn, err := kgo.NewClient(cl.kafkaOpts...)
	if err != nil {
		t.Fatalf("create Kafka client: %v", err)
	}
	defer conn.Close()

	if got, _ := conn.OptValue(kgo.ClientID).(string); got != "first" {
		t.Fatalf("snapshotted Kafka client ID = %q, want first", got)
	}

	first := &configTestHook{id: 1}
	second := &configTestHook{id: 2}
	hookValues := []Hook{first}
	hookOpt := WithHooks(hookValues...)
	hookValues[0] = second

	cl, err = newClient(hookOpt)
	if err != nil {
		t.Fatalf("new client with hooks: %v", err)
	}
	if len(cl.hooks) != 1 {
		t.Fatalf("snapshotted hooks = %#v, want one hook", cl.hooks)
	}
	gotHook, ok := cl.hooks[0].(*configTestHook)
	if !ok || gotHook.id != 1 {
		t.Fatalf("snapshotted hook = %#v, want first hook", cl.hooks[0])
	}
}

func TestNewClientRequiresConsumerHandler(t *testing.T) {
	tests := []struct {
		name string
		opts []kgo.Opt
	}{
		{
			name: "consumer group",
			opts: []kgo.Opt{
				kgo.ConsumeTopics("orders"),
				kgo.ConsumerGroup("orders-group"),
			},
		},
		{
			name: "share group",
			opts: []kgo.Opt{
				kgo.ConsumeTopics("orders"),
				kgo.ShareGroup("orders-share"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(WithKafkaOptions(tt.opts...))
			if client != nil {
				t.Fatal("client != nil, want nil")
			}
			if err == nil || err.Error() != "kafka: consumer requires batch handler" {
				t.Fatalf("new client error = %v, want missing handler error", err)
			}
		})
	}
}
