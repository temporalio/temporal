package grpcfaults

import (
	"context"
	"io"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/rpc/faults"
	"google.golang.org/grpc"
)

// UnaryServerInterceptor applies faults before and after an inbound call.
func UnaryServerInterceptor(generator Generator) grpc.UnaryServerInterceptor {
	if generator == nil {
		return nil
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return faults.Invoke(ctx, generator, info.FullMethod, req, func() (any, error) { return handler(ctx, req) })
	}
}

func StreamServerInterceptor(generator Generator) grpc.StreamServerInterceptor {
	if generator == nil {
		return nil
	}
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		if outcome := generator.GenerateRequest(ctx, info.FullMethod, nil); outcome != nil {
			return serviceerror.ToStatus(outcome.Error).Err()
		}
		err := handler(srv, stream)
		if outcome := generator.GenerateResponse(ctx, info.FullMethod, nil, nil, err); outcome != nil {
			return serviceerror.ToStatus(outcome.Error).Err()
		}
		return err
	}
}

// ClientDialOptions installs faults on both unary and streaming outbound calls.
func ClientDialOptions(generator Generator) []grpc.DialOption {
	if generator == nil {
		return nil
	}
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(UnaryClientInterceptor(generator)),
		grpc.WithChainStreamInterceptor(StreamClientInterceptor(generator)),
	}
}

func UnaryClientInterceptor(generator Generator) grpc.UnaryClientInterceptor {
	if generator == nil {
		return nil
	}
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		_, err := faults.Invoke(ctx, generator, method, req, func() (any, error) { return reply, invoker(ctx, method, req, reply, cc, opts...) })
		return serviceerror.ToStatus(err).Err()
	}
}

func StreamClientInterceptor(generator Generator) grpc.StreamClientInterceptor {
	if generator == nil {
		return nil
	}
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if outcome := generator.GenerateRequest(ctx, method, nil); outcome != nil {
			return nil, serviceerror.ToStatus(outcome.Error).Err()
		}
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return &clientStream{ClientStream: stream, generator: generator, method: method, serverStreams: desc.ServerStreams}, nil
	}
}

type clientStream struct {
	grpc.ClientStream
	generator     Generator
	method        string
	serverStreams bool
	done          bool
	err           error
}

// RecvMsg samples response faults once, on successful completion of the RPC.
// A client-streaming RPC completes with its single response; a server stream completes with EOF.
func (s *clientStream) RecvMsg(msg any) error {
	if s.done {
		return s.err
	}
	err := s.ClientStream.RecvMsg(msg)
	if err == nil && s.serverStreams {
		return nil
	}
	s.done = true
	s.err = err
	callErr := err
	if err == io.EOF {
		callErr = nil
	}
	if outcome := s.generator.GenerateResponse(s.Context(), s.method, nil, msg, callErr); outcome != nil {
		s.err = serviceerror.ToStatus(outcome.Error).Err()
	}
	result := s.err
	if s.err == nil {
		s.err = io.EOF
	}
	return result
}
