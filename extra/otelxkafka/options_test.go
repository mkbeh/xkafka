package otelxkafka

import (
	"maps"
	"testing"
)

func TestClientGroupOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		opts              []ClientOpt
		wantConsumerGroup string
		wantShareGroup    string
	}{
		{
			name:           "share replaces consumer",
			opts:           []ClientOpt{ConsumerGroup("workers"), ShareGroup("shared")},
			wantShareGroup: "shared",
		},
		{
			name:              "consumer replaces share",
			opts:              []ClientOpt{ShareGroup("shared"), ConsumerGroup("workers")},
			wantConsumerGroup: "workers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var meterCfg meterConfig
			var tracerCfg tracerConfig
			for _, opt := range tt.opts {
				opt.applyMeter(&meterCfg)
				opt.applyTracer(&tracerCfg)
			}

			for _, cfg := range []clientConfig{meterCfg.client, tracerCfg.client} {
				if cfg.consumerGroup != tt.wantConsumerGroup {
					t.Fatalf("consumer group = %q, want %q", cfg.consumerGroup, tt.wantConsumerGroup)
				}
				if cfg.shareGroup != tt.wantShareGroup {
					t.Fatalf("share group = %q, want %q", cfg.shareGroup, tt.wantShareGroup)
				}
			}
		})
	}
}

func TestLabelsSnapshotMergeAndReuse(t *testing.T) {
	t.Parallel()

	input := map[string]string{"env": "prod", "team": "a"}
	shared := Labels(input)

	input["env"] = "changed"
	delete(input, "team")

	var meterCfg meterConfig
	shared.applyMeter(&meterCfg)
	Labels(map[string]string{"team": "b"}).applyMeter(&meterCfg)

	wantMeter := map[string]string{"env": "prod", "team": "b"}
	if !maps.Equal(meterCfg.client.labels, wantMeter) {
		t.Fatalf("merged labels = %v, want %v", meterCfg.client.labels, wantMeter)
	}

	meterCfg.client.labels["env"] = "meter only"

	var tracerCfg tracerConfig
	shared.applyTracer(&tracerCfg)

	wantTracer := map[string]string{"env": "prod", "team": "a"}
	if !maps.Equal(tracerCfg.client.labels, wantTracer) {
		t.Fatalf("reused labels = %v, want %v", tracerCfg.client.labels, wantTracer)
	}
}
