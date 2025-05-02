package interceptor

import (
	"context"

	"go.temporal.io/server/common/backoff"
	"google.golang.org/grpc"
)

type (
	RetryableInterceptor struct {
		policy      backoff.RetryPolicy
		isRetryable backoff.IsRetryable
	}
)

var _ grpc.UnaryServerInterceptor = (*RetryableInterceptor)(nil).Intercept

func NewRetryableInterceptor(
	policy backoff.RetryPolicy,
	isRetryable backoff.IsRetryable,
) *RetryableInterceptor {
	return &RetryableInterceptor{
		policy:      policy,
		isRetryable: isRetryable,
	}
}

func (i *RetryableInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	var response interface{}
	op := func(ctx context.Context) error {
		var err error
		response, err = handler(ctx, req)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, i.policy, i.isRetryable)
	return response, err
}
