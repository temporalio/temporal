package grpcfaultstest_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/common/testing/grpcfaultstest"
	"go.temporal.io/testx/failpoint/buggify"
	"google.golang.org/grpc"
)

// TestUnaryServerInterceptor_DoesNotAllocate is not parallel, since AllocsPerRun doesn't allow it.
func TestUnaryServerInterceptor_DoesNotAllocate(t *testing.T) {
	info := &grpc.UnaryServerInfo{FullMethod: matchingservice.MatchingService_AddWorkflowTask_FullMethodName}
	handler := func(context.Context, any) (any, error) { return nil, nil }
	ctx := context.Background()
	requests := map[string]any{
		"namespace ID":   &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"},
		"namespace name": &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace-name"},
	}

	buggifyHooks := func(activation float64) *failpoints.Source {
		source := failpoints.NewEnabledSource()
		require.NoError(t, buggify.For(source.Failpoints()).Set(buggify.Config{Seed: 1, Activation: activation, Fire: 0, Error: "Unavailable"}))
		return source
	}
	for name, source := range map[string]*failpoints.Source{
		"no failpoints":               failpoints.NewEnabledSource(),
		"BUGGIFY, not activated":      buggifyHooks(0),
		"BUGGIFY, activated, no fire": buggifyHooks(1),
	} {
		interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(source, nil, primitives.HistoryService))
		for requestName, req := range requests {
			// The first call allocates the caches of the operation.
			_, _ = interceptor(ctx, req, info, handler)
			allocs := testing.AllocsPerRun(100, func() { _, _ = interceptor(ctx, req, info, handler) })
			require.Zero(t, allocs, name+", "+requestName)
		}
	}
}
