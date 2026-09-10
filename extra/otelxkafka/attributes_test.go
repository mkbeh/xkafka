package otelxkafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCommonRecordDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		records       []*kgo.Record
		wantTopic     string
		wantPartition int32
	}{
		{
			name: "same topic and partition",
			records: []*kgo.Record{
				{Topic: "a", Partition: 2},
				{Topic: "a", Partition: 2},
			},
			wantTopic:     "a",
			wantPartition: 2,
		},
		{
			name: "same topic different partitions",
			records: []*kgo.Record{
				{Topic: "a", Partition: 0},
				{Topic: "a", Partition: 2},
			},
			wantTopic:     "a",
			wantPartition: -1,
		},
		{
			name: "mixed topics",
			records: []*kgo.Record{
				{Topic: "a", Partition: 0},
				{Topic: "b", Partition: 0},
			},
			wantPartition: -1,
		},
		{
			name: "topic changes after partition mismatch",
			records: []*kgo.Record{
				{Topic: "a", Partition: 0},
				{Topic: "a", Partition: 1},
				{Topic: "b", Partition: 1},
			},
			wantPartition: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic, partition := commonRecordDestination(tt.records)
			if topic != tt.wantTopic {
				t.Fatalf("topic = %q, want %q", topic, tt.wantTopic)
			}
			if partition != tt.wantPartition {
				t.Fatalf("partition = %d, want %d", partition, tt.wantPartition)
			}
		})
	}
}
