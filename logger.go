package xkafka

import "github.com/twmb/franz-go/pkg/kgo"

const (
	logKeyError         = "error"
	logKeyTopic         = "topic"
	logKeyPartition     = "partition"
	logKeyRecord        = "record"
	logKeyRecordCount   = "record_count"
	logKeyConsumerGroup = "consumer_group"
)

func (c *client) logEnabled(level kgo.LogLevel) bool {
	return c.logger != nil && c.logger.Level() >= level
}

func (c *client) log(level kgo.LogLevel, msg string, keyvals ...any) {
	if !c.logEnabled(level) {
		return
	}

	c.logger.Log(level, msg, keyvals...)
}
