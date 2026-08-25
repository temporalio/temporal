package faultinjection_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/rpc/faultinjection"
	"google.golang.org/grpc"
)

func TestGRPCUnaryServerInterceptor_NilGenerator(t *testing.T) {
	t.Parallel()

	require.Nil(t, faultinjection.GRPCUnaryServerInterceptor(nil))
}

func TestGRPCUnaryServerInterceptor_ConfiguredBeforeHandler(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	generator := faultinjection.NewRPCFaultGenerator()
	generator.RegisterRequestCallback(faultinjection.RPCFaultScope{}, func(context.Context, string, any) (bool, any, error) {
		return true, nil, injectedErr
	})
	interceptor := faultinjection.GRPCUnaryServerInterceptor(generator)
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

func TestGRPCUnaryServerInterceptor_ConfiguredAfterHandler(t *testing.T) {
	t.Parallel()

	generator := faultinjection.NewRPCFaultGenerator()
	generator.RegisterResponseCallback(faultinjection.RPCFaultScope{}, func(context.Context, string, any, any, error) (bool, any, error) {
		return true, "replacement", nil
	})
	interceptor := faultinjection.GRPCUnaryServerInterceptor(generator)

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

func TestGRPCUnaryServerInterceptor_ConfiguredHandlerError(t *testing.T) {
	t.Parallel()

	handlerErr := errors.New("handler")
	injectedErr := errors.New("injected")
	generator := faultinjection.NewRPCFaultGenerator()
	generator.RegisterResponseCallback(faultinjection.RPCFaultScope{}, func(_ context.Context, _ string, _, response any, err error) (bool, any, error) {
		require.Nil(t, response)
		require.ErrorIs(t, err, handlerErr)
		return true, nil, injectedErr
	})
	interceptor := faultinjection.GRPCUnaryServerInterceptor(generator)

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
