package ringq

import "context"

type DispatchFunc func(ctx context.Context, raw []byte) Result

func Wrap[T any](handler Handler[T]) DispatchFunc {
	var codec Codec[T] = JSONCodec[T]{}
	return func(ctx context.Context, raw []byte) Result {
		decoded, err := codec.Decode(raw)
		if err != nil {
			return Result{Action: DLQ, Err: err}
		}
		return handler(ctx, decoded)
	}
}

func WrapWithCodec[T any](handler Handler[T], codec Codec[T]) DispatchFunc {
	return func(ctx context.Context, raw []byte) Result {
		decoded, err := codec.Decode(raw)
		if err != nil {
			return Result{Action: DLQ, Err: err}
		}
		return handler(ctx, decoded)
	}
}
