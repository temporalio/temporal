package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/headers"
)

func TestErrCallerRateLimitExceeded_CauseAndScope(t *testing.T) {
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_CALLER_RPS_LIMIT, ErrCallerRateLimitExceeded.Cause)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, ErrCallerRateLimitExceeded.Scope)
	require.Equal(t, "caller rate limit exceeded", ErrCallerRateLimitExceeded.Message)
}

func TestNoopCallerRateLimitInterceptor_AllowReturnsNil(t *testing.T) {
	i := NewNoopCallerRateLimitInterceptor()
	require.NoError(t, i.Allow(nil, "SomeAPI", headers.NewGRPCHeaderGetter(context.Background())))
}
