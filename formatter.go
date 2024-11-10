package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

func newFormatter() (*kgo.RecordFormatter, error) {
	return kgo.NewRecordFormatter("topic: %t, key: %k, msg: %v")
}
