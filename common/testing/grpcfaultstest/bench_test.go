//go:build test_dep

package grpcfaultstest_test

import (
	"context"
	"testing"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/grpcfaultstest"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/testx/failpoint/buggify"
	"google.golang.org/grpc"
)

// BenchmarkUnaryServerInterceptor_NoFailpoints measures the cost of the fault interceptor in a
// test_dep build while no failpoint is registered.
func BenchmarkUnaryServerInterceptor_NoFailpoints(b *testing.B) {
	interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(testhooks.NewTestHooks(), nil, primitives.HistoryService))
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

// BenchmarkUnaryServerInterceptor_Buggify measures the cost of the fault interceptor while BUGGIFY
// is enabled and no failpoint is registered, for a method that BUGGIFY did not activate and one
// that it activated but whose execution doesn't fire.
func BenchmarkUnaryServerInterceptor_Buggify(b *testing.B) {
	info := &grpc.UnaryServerInfo{FullMethod: matchingservice.MatchingService_AddWorkflowTask_FullMethodName}
	handler := func(context.Context, any) (any, error) { return nil, nil }
	ctx := context.Background()
	req := &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"}
	for _, bc := range []struct {
		name       string
		activation float64
	}{
		{name: "not activated", activation: 0},
		{name: "activated", activation: 1},
	} {
		b.Run(bc.name, func(b *testing.B) {
			testHooks := testhooks.NewTestHooks()
			if err := buggify.For(testHooks.Failpoints()).Set(buggify.Config{Seed: 1, Activation: bc.activation, Fire: 0}); err != nil {
				b.Fatal(err)
			}
			interceptor := grpcfaults.UnaryServerInterceptor(grpcfaultstest.NewGenerator(testHooks, nil, primitives.HistoryService))
			b.ReportAllocs()
			for b.Loop() {
				_, _ = interceptor(ctx, req, info, handler)
			}
		})
	}
}
