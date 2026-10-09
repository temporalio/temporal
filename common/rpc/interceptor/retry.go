package interceptor

import (
	"context"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/rpc/interceptor/nexus"
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
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	var response any
	op := func(ctx context.Context) error {
		var err error
		response, err = handler(ctx, req)
		return err
	}

	err := backoff.ThrottleRetryContext(ctx, op, i.policy, i.isRetryable)
	return response, err
}

// InterceptNexus is a no-op as retries are on the caller side
func (i *RetryableInterceptor) InterceptNexus(
	ctx context.Context,
	in nexus.InterceptorInput,
	next nexus.HandlerFunc,
) (any, error) {
	return next(ctx, in)
}
