package ringq

import "encoding/json"

type Codec[T any] interface {
	Encode(v T) ([]byte, error)
	Decode(data []byte) (T, error)
}

type JSONCodec[T any] struct{}

func (c JSONCodec[T]) Encode(v T) ([]byte, error) {
	return json.Marshal(v)
}

func (c JSONCodec[T]) Decode(data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return v, err
}
