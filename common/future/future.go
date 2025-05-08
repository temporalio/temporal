package future

import "context"

type (
	Future[T any] interface {
		Get(ctx context.Context) (T, error)
		Ready() bool
	}
)
