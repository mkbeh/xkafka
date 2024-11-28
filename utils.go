package kafka

import (
	"fmt"

	"github.com/google/uuid"
)

type NumberOrString interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64 | string
}

func GenerateUUID() string {
	return uuid.New().String()
}

func ConvertAnyToBytes[T NumberOrString](v T) []byte {
	return []byte(fmt.Sprintf("%v", v))
}
