package grpcfaults_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"google.golang.org/grpc"
)

// generator returns the outcomes of its functions; a nil function returns no fault.
type generator struct {
	request  func(operation string, req any) (any, error)
	response func(operation string, req, resp any, err error) (any, error)
}

func (g generator) GenerateRequest(_ context.Context, operation string, req any) (any, error) {
	if g.request == nil {
		return nil, nil
	}
	return g.request(operation, req)
}

func (g generator) GenerateResponse(_ context.Context, operation string, req, resp any, err error) (any, error) {
	if g.response == nil {
		return nil, nil
	}
	return g.response(operation, req, resp, err)
}

func TestUnaryServerInterceptor_NilGenerator(t *testing.T) {
	t.Parallel()

	require.Nil(t, grpcfaults.UnaryServerInterceptor(nil))
}

func TestUnaryServerInterceptor_NoFault(t *testing.T) {
	t.Parallel()

	handlerErr := errors.New("handler")
	interceptor := grpcfaults.UnaryServerInterceptor(generator{})

	response, err := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			return "response", handlerErr
		},
	)

	require.ErrorIs(t, err, handlerErr)
	require.Equal(t, "response", response)
}

func TestUnaryServerInterceptor_ConfiguredBeforeHandler(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	interceptor := grpcfaults.UnaryServerInterceptor(generator{
		request: func(operation string, req any) (any, error) {
			require.Equal(t, "/test.Service/Method", operation)
			require.Equal(t, "request", req)
			return nil, injectedErr
		},
	})
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

	handlerErr := errors.New("handler")
	injectedErr := errors.New("injected")
	interceptor := grpcfaults.UnaryServerInterceptor(generator{
		response: func(operation string, req, response any, err error) (any, error) {
			require.Equal(t, "/test.Service/Method", operation)
			require.Equal(t, "request", req)
			require.Equal(t, "response", response)
			require.ErrorIs(t, err, handlerErr)
			return nil, injectedErr
		},
	})

	response, err := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			return "response", handlerErr
		},
	)

	require.ErrorIs(t, err, injectedErr)
	require.NotErrorIs(t, err, handlerErr)
	require.Nil(t, response)
}

func TestUnaryServerInterceptor_ReplacementBeforeHandler(t *testing.T) {
	t.Parallel()

	interceptor := grpcfaults.UnaryServerInterceptor(generator{
		request: func(string, any) (any, error) { return "replacement", nil },
	})
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

	require.NoError(t, err)
	require.Equal(t, "replacement", response)
	require.False(t, handlerCalled)
}

func TestUnaryServerInterceptor_ReplacementAfterHandler(t *testing.T) {
	t.Parallel()

	interceptor := grpcfaults.UnaryServerInterceptor(generator{
		response: func(string, any, any, error) (any, error) { return "replacement", nil },
	})

	response, err := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(context.Context, any) (any, error) {
			return nil, errors.New("handler")
		},
	)

	require.NoError(t, err)
	require.Equal(t, "replacement", response)
}
