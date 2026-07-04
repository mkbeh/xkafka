package xkafka

import (
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	logKeyError          = "error"
	logKeyTopic          = "topic"
	logKeyRecord         = "record"
	logKeyRecords        = "records"
	logKeyConsumerLabels = "consumer_labels"
)

func newDefaultLogger() kgo.Logger {
	return kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)
}
