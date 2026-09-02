package xkafka

import (
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	logKeyError         = "error"
	logKeyTopic         = "topic"
	logKeyRecord        = "record"
	logKeyRecordCount   = "record_count"
	logKeyConsumerGroup = "consumer_group"
)

func newDefaultLogger() kgo.Logger {
	return kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)
}
