package kafka

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"
)

type (
	MarshalFunc   func(m interface{}) ([]byte, error)
	UnmarshalFunc func(data []byte, v interface{}) error
)

var (
	ProtoMarshal   = MarshalProto()
	ProtoUnmarshal = UnmarshalProto()
	JSONMarshal    = MarshalJSON()
	JSONUnmarshal  = UnmarshalJSON()
)

func MarshalProto() MarshalFunc {
	return func(v interface{}) ([]byte, error) {
		m, ok := v.(proto.Message)
		if !ok {
			return nil, fmt.Errorf("conn: invalid proto message: %T", v)
		}

		payload, err := proto.Marshal(m)
		if err != nil {
			return nil, err
		}

		return payload, nil
	}
}

func UnmarshalProto() UnmarshalFunc {
	return func(data []byte, v interface{}) error {
		m, ok := v.(proto.Message)
		if !ok {
			return fmt.Errorf("conn: invalid proto message: %T", v)
		}
		return proto.Unmarshal(data, m)
	}
}

func MarshalJSON() MarshalFunc {
	return json.Marshal
}

func UnmarshalJSON() UnmarshalFunc {
	return json.Unmarshal
}
