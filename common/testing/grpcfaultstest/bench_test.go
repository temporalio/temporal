//go:build test_dep

package grpcfaultstest_test

import (
	"context"
	"testing"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/grpcfaultstest"
	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/grpc"
)

// BenchmarkUnaryServerInterceptor_NoFailpoints measures the cost of the fault interceptor in a
// test_dep build while no failpoint is registered.
func BenchmarkUnaryServerInterceptor_NoFailpoints(b *testing.B) {
	interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(testhooks.NewTestHooks(), nil))
	info := &grpc.UnaryServerInfo{FullMethod: matchingservice.MatchingService_AddWorkflowTask_FullMethodName}
	handler := func(context.Context, any) (any, error) { return nil, nil }
	ctx := context.Background()
	for _, bc := range []struct {
		name string
		req  any
	}{
		{name: "namespace ID", req: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"}},
		{name: "namespace name", req: &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace-name"}},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _ = interceptor(ctx, bc.req, info, handler)
			}
		})
	}
}
