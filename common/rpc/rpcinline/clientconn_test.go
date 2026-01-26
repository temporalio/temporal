package rpcinline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// testServer implements a simple gRPC service for testing
type testServer struct {
	lastRequest proto.Message
}

func (s *testServer) Echo(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	s.lastRequest = req
	return &wrapperspb.StringValue{Value: "response:" + req.GetValue()}, nil
}

func (s *testServer) EchoError(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return nil, status.Error(codes.InvalidArgument, "test error")
}

// testServiceDesc is a gRPC ServiceDesc for the testServer, mimicking generated code
var testServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.Service",
	HandlerType: (*testServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Echo",
			Handler:    testServiceEchoHandler,
		},
		{
			MethodName: "EchoError",
			Handler:    testServiceEchoErrorHandler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "test.proto",
}

//nolint:revive // gRPC MethodHandler signature
func testServiceEchoHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(wrapperspb.StringValue)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*testServer).Echo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/test.Service/Echo",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*testServer).Echo(ctx, req.(*wrapperspb.StringValue))
	}
	return interceptor(ctx, in, info, handler)
}

//nolint:revive // gRPC MethodHandler signature
func testServiceEchoErrorHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(wrapperspb.StringValue)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*testServer).EchoError(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/test.Service/EchoError",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*testServer).EchoError(ctx, req.(*wrapperspb.StringValue))
	}
	return interceptor(ctx, in, info, handler)
}

func TestClientConn_BasicInvoke(t *testing.T) {
	server := &testServer{}
	conn := newClientConn()
	conn.registerServiceDesc(&testServiceDesc, server, nil, nil)

	req := &wrapperspb.StringValue{Value: "hello"}
	resp := &wrapperspb.StringValue{}

	err := conn.Invoke(context.Background(), "/test.Service/Echo", req, resp)
	require.NoError(t, err)
	assert.Equal(t, "response:hello", resp.GetValue())
}

func TestClientConn_Error(t *testing.T) {
	server := &testServer{}
	conn := newClientConn()
	conn.registerServiceDesc(&testServiceDesc, server, nil, nil)

	req := &wrapperspb.StringValue{Value: "hello"}
	resp := &wrapperspb.StringValue{}

	err := conn.Invoke(context.Background(), "/test.Service/EchoError", req, resp)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestClientConn_MethodNotFound(t *testing.T) {
	conn := newClientConn()

	req := &wrapperspb.StringValue{Value: "hello"}
	resp := &wrapperspb.StringValue{}

	err := conn.Invoke(context.Background(), "/test.Service/NotFound", req, resp)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unimplemented, st.Code())
}

func TestClientConn_MetadataHandling(t *testing.T) {
	server := &testServer{}
	conn := newClientConn()
	conn.registerServiceDesc(&testServiceDesc, server, nil, nil)

	// Set outgoing metadata
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("key", "value"))

	req := &wrapperspb.StringValue{Value: "hello"}
	resp := &wrapperspb.StringValue{}

	err := conn.Invoke(ctx, "/test.Service/Echo", req, resp)
	require.NoError(t, err)
}

func TestClientConn_Interceptors(t *testing.T) {
	server := &testServer{}
	conn := newClientConn()

	clientCalled := false
	serverCalled := false

	clientInterceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		clientCalled = true
		return invoker(ctx, method, req, reply, cc, opts...)
	}

	serverInterceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		serverCalled = true
		return handler(ctx, req)
	}

	conn.registerServiceDesc(&testServiceDesc, server, []grpc.UnaryClientInterceptor{clientInterceptor}, []grpc.UnaryServerInterceptor{serverInterceptor})

	req := &wrapperspb.StringValue{Value: "hello"}
	resp := &wrapperspb.StringValue{}

	err := conn.Invoke(context.Background(), "/test.Service/Echo", req, resp)
	require.NoError(t, err)
	assert.True(t, clientCalled, "client interceptor should be called")
	assert.True(t, serverCalled, "server interceptor should be called")
}

func TestClientConn_NewStream_NotSupported(t *testing.T) {
	conn := newClientConn()
	_, err := conn.NewStream(context.Background(), nil, "")
	assert.Error(t, err)
}
