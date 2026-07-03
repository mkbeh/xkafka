package xkafka

import (
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	logKeyError          = "error"
	logKeyRecord         = "record"
	logKeyRecords        = "records"
	logKeyCount          = "count"
	logKeyConsumerLabels = "consumer_labels"
)

func newDefaultLogger() kgo.Logger {
	return kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)
}
