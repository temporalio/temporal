package grpcfaults_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"google.golang.org/grpc"
)

func TestUnaryServerInterceptor_NilGenerator(t *testing.T) {
	t.Parallel()

	require.Nil(t, grpcfaults.UnaryServerInterceptor(nil))
}

func TestUnaryServerInterceptor_ConfiguredBeforeHandler(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	generator := grpcfaults.NewCallbackGenerator()
	generator.RegisterRequestCallback(grpcfaults.Scope{}, func(context.Context, string, any) *grpcfaults.Outcome {
		return &grpcfaults.Outcome{Error: injectedErr}
	})
	interceptor := grpcfaults.UnaryServerInterceptor(generator)
	handlerCalled := false

	response, err := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			handlerCalled = true
			return "response", nil
		},
	)

	require.ErrorIs(t, err, injectedErr)
	require.Nil(t, response)
	require.False(t, handlerCalled)
}

func TestUnaryServerInterceptor_ConfiguredAfterHandler(t *testing.T) {
	t.Parallel()

	generator := grpcfaults.NewCallbackGenerator()
	generator.RegisterResponseCallback(grpcfaults.Scope{}, func(context.Context, string, any, any, error) *grpcfaults.Outcome {
		return &grpcfaults.Outcome{Response: "replacement"}
	})
	interceptor := grpcfaults.UnaryServerInterceptor(generator)

	response, err := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			return "response", nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, "replacement", response)
}

func TestUnaryServerInterceptor_ConfiguredHandlerError(t *testing.T) {
	t.Parallel()

	handlerErr := errors.New("handler")
	injectedErr := errors.New("injected")
	generator := grpcfaults.NewCallbackGenerator()
	generator.RegisterResponseCallback(grpcfaults.Scope{}, func(_ context.Context, _ string, _, response any, err error) *grpcfaults.Outcome {
		require.Nil(t, response)
		require.ErrorIs(t, err, handlerErr)
		return &grpcfaults.Outcome{Error: injectedErr}
	})
	interceptor := grpcfaults.UnaryServerInterceptor(generator)

	response, err := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			return nil, handlerErr
		},
	)

	require.ErrorIs(t, err, injectedErr)
	require.Nil(t, response)
}
