package rpcinline

import (
	"context"
	"fmt"
	"reflect"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var _ grpc.ClientConnInterface = (*clientConn)(nil)

// clientConn is a grpc.ClientConnInterface that routes calls directly to
// in-process handlers without network or serialization overhead.
type clientConn struct {
	methods map[string]*serviceMethod
	streams map[string]*streamMethod
}

type serviceMethod struct {
	info              grpc.UnaryServerInfo
	handler           grpc.UnaryHandler
	clientInterceptor grpc.UnaryClientInterceptor
	serverInterceptor grpc.UnaryServerInterceptor
}

type streamMethod struct {
	desc    *grpc.StreamDesc
	handler grpc.StreamHandler
	impl    any
}

func newClientConn() *clientConn {
	return &clientConn{
		methods: make(map[string]*serviceMethod),
		streams: make(map[string]*streamMethod),
	}
}

func (c *clientConn) registerServiceDesc(
	desc *grpc.ServiceDesc,
	impl any,
	clientInterceptors []grpc.UnaryClientInterceptor,
	serverInterceptors []grpc.UnaryServerInterceptor,
) {
	for _, method := range desc.Methods {
		fullMethod := "/" + desc.ServiceName + "/" + method.MethodName
		handler := method.Handler
		c.methods[fullMethod] = &serviceMethod{
			info: grpc.UnaryServerInfo{Server: impl, FullMethod: fullMethod},
			handler: func(ctx context.Context, req any) (any, error) {
				return handler(impl, ctx, func(out any) error {
					// Copy request to the handler's expected type
					reflect.ValueOf(out).Elem().Set(reflect.ValueOf(req).Elem())
					return nil
				}, nil)
			},
			clientInterceptor: chainUnaryClientInterceptors(clientInterceptors),
			serverInterceptor: chainUnaryServerInterceptors(serverInterceptors),
		}
	}

	// Register streaming methods
	for i := range desc.Streams {
		stream := &desc.Streams[i]
		fullMethod := "/" + desc.ServiceName + "/" + stream.StreamName
		c.streams[fullMethod] = &streamMethod{
			desc:    stream,
			handler: stream.Handler,
			impl:    impl,
		}
	}
}

func (c *clientConn) Invoke(
	ctx context.Context,
	method string,
	args any,
	reply any,
	opts ...grpc.CallOption,
) error {
	sm := c.methods[method]
	if sm == nil {
		return status.Error(codes.Unimplemented, "method not found: "+method)
	}

	invoker := func(ctx context.Context, method string, req, reply any, _ *grpc.ClientConn, opts ...grpc.CallOption) error {
		// Set up server transport stream for header/trailer capture
		stream := &fakeServerStream{method: method}
		ctx = grpc.NewContextWithServerTransportStream(ctx, stream)

		// Move outgoing metadata to incoming
		md, _ := metadata.FromOutgoingContext(ctx)
		ctx = metadata.NewIncomingContext(ctx, md)
		ctx = metadata.NewOutgoingContext(ctx, metadata.MD{})

		// Call through server interceptor chain
		resp, err := sm.serverInterceptor(ctx, req, &sm.info, sm.handler)

		// Copy headers/trailers to call options
		for _, opt := range opts {
			if h, ok := opt.(grpc.HeaderCallOption); ok {
				*h.HeaderAddr = stream.header
			} else if t, ok := opt.(grpc.TrailerCallOption); ok {
				*t.TrailerAddr = stream.trailer
			}
		}

		// Copy response to reply
		if resp != nil {
			if copyErr := copyResponse(resp, reply); copyErr != nil {
				return copyErr
			}
		}

		return err
	}

	return sm.clientInterceptor(ctx, method, args, reply, nil, invoker, opts...)
}

func (c *clientConn) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	sm := c.streams[method]
	if sm == nil {
		return nil, status.Error(codes.Unimplemented, "streaming method not found: "+method)
	}

	// Move outgoing metadata to incoming for the server side
	md, _ := metadata.FromOutgoingContext(ctx)
	ctx = metadata.NewIncomingContext(ctx, md)

	// Create the inline stream
	stream := newInlineStream(ctx, method)

	// Run the server handler in a goroutine
	go func() {
		defer close(stream.serverToClient)
		defer stream.cancel()
		// Ensure header is sent even if handler doesn't explicitly send it
		defer func() {
			select {
			case <-stream.headerCh:
			default:
				close(stream.headerCh)
			}
		}()

		err := sm.handler(sm.impl, stream.serverStream())
		if err != nil {
			stream.setHandlerError(err)
		}
	}()

	return stream.clientStream(), nil
}

// copyResponse copies src to dst, handling type mismatches for wire-compatible protos.
func copyResponse(src, dst any) error {
	srcMsg, srcOk := src.(proto.Message)
	dstMsg, dstOk := dst.(proto.Message)

	if !srcOk || srcMsg == nil {
		return nil
	}

	if dstOk {
		// For wire-compatible proto types with different descriptors,
		// we must marshal/unmarshal since proto.Merge requires same descriptor
		if reflect.TypeOf(src).AssignableTo(reflect.TypeOf(dst)) {
			proto.Merge(dstMsg, srcMsg)
		} else {
			data, err := proto.Marshal(srcMsg)
			if err != nil {
				return fmt.Errorf("rpcinline: failed to marshal response: %w", err)
			}
			if err := proto.Unmarshal(data, dstMsg); err != nil {
				return fmt.Errorf("rpcinline: failed to unmarshal response: %w", err)
			}
		}
		return nil
	}

	// Fallback to reflection (shallow copy)
	reflect.ValueOf(dst).Elem().Set(reflect.ValueOf(src).Elem())
	return nil
}

func chainUnaryServerInterceptors(interceptors []grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	switch len(interceptors) {
	case 0:
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	case 1:
		return interceptors[0]
	default:
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return interceptors[0](ctx, req, info, chainUnaryHandler(interceptors, 0, info, handler))
		}
	}
}

func chainUnaryHandler(
	interceptors []grpc.UnaryServerInterceptor,
	curr int,
	info *grpc.UnaryServerInfo,
	finalHandler grpc.UnaryHandler,
) grpc.UnaryHandler {
	if curr == len(interceptors)-1 {
		return finalHandler
	}
	return func(ctx context.Context, req any) (any, error) {
		return interceptors[curr+1](ctx, req, info, chainUnaryHandler(interceptors, curr+1, info, finalHandler))
	}
}

func chainUnaryClientInterceptors(interceptors []grpc.UnaryClientInterceptor) grpc.UnaryClientInterceptor {
	switch len(interceptors) {
	case 0:
		return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
	case 1:
		return interceptors[0]
	default:
		return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return interceptors[0](ctx, method, req, reply, cc, chainUnaryInvoker(interceptors, 0, invoker), opts...)
		}
	}
}

func chainUnaryInvoker(interceptors []grpc.UnaryClientInterceptor, curr int, finalInvoker grpc.UnaryInvoker) grpc.UnaryInvoker {
	if curr == len(interceptors)-1 {
		return finalInvoker
	}
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return interceptors[curr+1](ctx, method, req, reply, cc, chainUnaryInvoker(interceptors, curr+1, finalInvoker), opts...)
	}
}
