package otelxkafka

import "github.com/twmb/franz-go/pkg/kgo"

type recordCarrier struct {
	record *kgo.Record
}

func (c recordCarrier) Get(key string) string {
	if c.record == nil {
		return ""
	}

	for i := range c.record.Headers {
		header := &c.record.Headers[i]
		if header.Key == key {
			return string(header.Value)
		}
	}

	return ""
}

func (c recordCarrier) Set(key, value string) {
	if c.record == nil {
		return
	}

	for i := range c.record.Headers {
		header := &c.record.Headers[i]
		if header.Key == key {
			header.Value = []byte(value)
			return
		}
	}

	c.record.Headers = append(c.record.Headers, kgo.RecordHeader{
		Key:   key,
		Value: []byte(value),
	})
}

func (c recordCarrier) Keys() []string {
	if c.record == nil {
		return nil
	}

	keys := make([]string, len(c.record.Headers))
	for i := range c.record.Headers {
		keys[i] = c.record.Headers[i].Key
	}

	return keys
}
