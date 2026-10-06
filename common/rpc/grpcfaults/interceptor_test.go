package grpcfaults_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestConfiguredUnaryFaults(t *testing.T) {
	t.Parallel()
	for name, code := range map[string]codes.Code{
		"Unavailable":       codes.Unavailable,
		"Internal":          codes.Internal,
		"ResourceExhausted": codes.ResourceExhausted,
	} {
		for _, responseFault := range []bool{false, true} {
			t.Run(name+"/"+map[bool]string{false: "request", true: "response"}[responseFault], func(t *testing.T) {
				t.Parallel()
				cfg := &config.CallFaultInjection{}
				stage := config.FaultInjectionMethodConfig{Errors: map[string]float64{name: 1}}
				if responseFault {
					cfg.Response = stage
				} else {
					cfg.Request = stage
				}
				generator, err := configuredGenerator(cfg, nil)
				require.NoError(t, err)
				interceptor := grpcfaults.UnaryServerInterceptor(generator)
				called := false
				resp, err := interceptor(t.Context(), "request", &grpc.UnaryServerInfo{FullMethod: "/test/Method"}, func(context.Context, any) (any, error) {
					called = true
					return "response", nil
				})
				require.Error(t, err)
				require.Equal(t, code, serviceerror.ToStatus(err).Code())
				require.True(t, common.IsServiceClientTransientError(err))
				require.Nil(t, resp)
				require.Equal(t, responseFault, called)
			})
		}
	}
}

type configuredServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s configuredServerStream) Context() context.Context { return s.ctx }

func TestConfiguredStreamFaults(t *testing.T) {
	t.Parallel()
	require.Nil(t, grpcfaults.StreamServerInterceptor(nil))
	for _, stage := range []string{"disabled", "request", "response", "handler error"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			cfg := &config.CallFaultInjection{}
			fault := config.FaultInjectionMethodConfig{Errors: map[string]float64{"Unavailable": 1}}
			if stage == "request" {
				cfg.Request = fault
			} else {
				cfg.Response = fault
				if stage == "disabled" {
					cfg.Response.Errors["Unavailable"] = 0
				}
			}
			generator, err := configuredGenerator(cfg, nil)
			require.NoError(t, err)
			if stage == "disabled" {
				require.Nil(t, grpcfaults.StreamServerInterceptor(generator))
				return
			}
			called := false
			handlerErr := errors.New("handler error")
			err = grpcfaults.StreamServerInterceptor(generator)(nil, configuredServerStream{ctx: t.Context()}, &grpc.StreamServerInfo{FullMethod: "/test/Stream"}, func(any, grpc.ServerStream) error {
				called = true
				if stage == "handler error" {
					return handlerErr
				}
				return nil
			})
			require.Equal(t, stage != "request", called)
			switch stage {
			case "disabled":
				require.NoError(t, err)
			case "handler error":
				require.ErrorIs(t, err, handlerErr)
			default:
				require.Equal(t, codes.Unavailable, serviceerror.ToStatus(err).Code())
			}
		})
	}
}

func configuredGenerator(cfg *config.CallFaultInjection, fallback grpcfaults.Generator) (grpcfaults.Generator, error) {
	generators, err := grpcfaults.NewConfiguredGenerators(&config.TransportFaultInjection{Inbound: cfg}, grpcfaults.Generators{Inbound: fallback})
	return generators.Inbound, err
}

func TestConfiguredUnaryClientFaults(t *testing.T) {
	t.Parallel()
	require.Nil(t, grpcfaults.UnaryClientInterceptor(nil))
	require.Nil(t, grpcfaults.ClientDialOptions(nil))
	for name, code := range map[string]codes.Code{"Unavailable": codes.Unavailable, "Internal": codes.Internal, "ResourceExhausted": codes.ResourceExhausted} {
		for _, stage := range []string{"request", "response", "call error"} {
			t.Run(name+"/"+stage, func(t *testing.T) {
				t.Parallel()
				cfg := &config.CallFaultInjection{}
				fault := config.FaultInjectionMethodConfig{Errors: map[string]float64{name: 1}}
				if stage == "request" {
					cfg.Request = fault
				} else {
					cfg.Response = fault
				}
				generators, err := grpcfaults.NewConfiguredGenerators(&config.TransportFaultInjection{Outbound: cfg}, grpcfaults.Generators{})
				require.NoError(t, err)
				called := false
				err = grpcfaults.UnaryClientInterceptor(generators.Outbound)(t.Context(), "/test/Method", "request", nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
					called = true
					if stage == "call error" {
						return status.Error(codes.InvalidArgument, "original")
					}
					return nil
				})
				require.Equal(t, stage != "request", called)
				if stage == "call error" {
					require.Equal(t, codes.InvalidArgument, status.Code(err))
				} else {
					require.Equal(t, code, status.Code(err))
					require.True(t, common.IsServiceClientTransientError(serviceerror.FromStatus(status.Convert(err))))
				}
			})
		}
	}
}

type configuredClientStream struct {
	grpc.ClientStream
	ctx  context.Context
	recv func(any) error
}

func (s configuredClientStream) Context() context.Context { return s.ctx }
func (s configuredClientStream) RecvMsg(msg any) error    { return s.recv(msg) }

func TestConfiguredStreamClientFaults(t *testing.T) {
	t.Parallel()
	require.Nil(t, grpcfaults.StreamClientInterceptor(nil))
	for _, stage := range []string{"request", "response", "disabled", "creation error", "receive error"} {
		for _, serverStreams := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/serverStreams=%t", stage, serverStreams), func(t *testing.T) {
				t.Parallel()
				cfg := &config.CallFaultInjection{}
				fault := config.FaultInjectionMethodConfig{Errors: map[string]float64{"Unavailable": 1}}
				if stage == "request" {
					cfg.Request = fault
				} else {
					cfg.Response = fault
				}
				if stage == "disabled" {
					cfg.Response.Errors["Unavailable"] = 0
				}
				generators, err := grpcfaults.NewConfiguredGenerators(&config.TransportFaultInjection{Outbound: cfg}, grpcfaults.Generators{})
				require.NoError(t, err)
				if stage == "disabled" {
					generators.Outbound = grpcfaults.NewCallbackGenerator()
				}
				received := 0
				original := status.Error(codes.InvalidArgument, "original")
				stream, err := grpcfaults.StreamClientInterceptor(generators.Outbound)(t.Context(), &grpc.StreamDesc{ServerStreams: serverStreams}, nil, "/test/Stream", func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, ...grpc.CallOption) (grpc.ClientStream, error) {
					if stage == "creation error" {
						return nil, original
					}
					return configuredClientStream{ctx: t.Context(), recv: func(any) error {
						received++
						if stage == "receive error" {
							return original
						}
						if received > 1 {
							return io.EOF
						}
						return nil
					}}, nil
				})
				switch stage {
				case "request":
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.Nil(t, stream)
					return
				case "creation error":
					require.ErrorIs(t, err, original)
					return
				}
				require.NoError(t, err)
				err = stream.RecvMsg(nil)
				if serverStreams && stage != "receive error" {
					require.NoError(t, err)
					err = stream.RecvMsg(nil)
				}
				switch stage {
				case "response":
					require.Equal(t, codes.Unavailable, status.Code(err))
				case "receive error":
					require.ErrorIs(t, err, original)
				default:
					if serverStreams {
						require.ErrorIs(t, err, io.EOF)
					} else {
						require.NoError(t, err)
					}
				}
				if stage == "disabled" {
					err = io.EOF
				}
				require.Equal(t, err, stream.RecvMsg(nil))
				require.Equal(t, map[bool]int{false: 1, true: 2}[serverStreams && stage != "receive error"], received)
			})
		}
	}
}
