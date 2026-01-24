// Package inline provides an in-process gRPC client implementation.
// Based on PR #6733 "Inline matching/history rpcs and local load balancing".
package inline

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	errStreamNotSupported = errors.New("inline: streaming not supported")
)

// ClientConn is a grpc.ClientConnInterface that routes calls directly to
// in-process handlers without network overhead.
type ClientConn struct {
	methods map[string]*serviceMethod
}

var _ grpc.ClientConnInterface = (*ClientConn)(nil)

type serviceMethod struct {
	info              grpc.UnaryServerInfo
	handler           grpc.UnaryHandler
	clientInterceptor grpc.UnaryClientInterceptor
	serverInterceptor grpc.UnaryServerInterceptor
}

var (
	contextType      = reflect.TypeOf((*context.Context)(nil)).Elem()
	protoMessageType = reflect.TypeOf((*proto.Message)(nil)).Elem()
	errorType        = reflect.TypeOf((*error)(nil)).Elem()
)

// NewClientConn creates a new inline ClientConn.
func NewClientConn() *ClientConn {
	return &ClientConn{
		methods: make(map[string]*serviceMethod),
	}
}

// RegisterServer registers a gRPC server implementation for inline calls.
// The qualifiedServerName should be the full service name (e.g., "temporal.api.workflowservice.v1.WorkflowService").
// This uses reflection to discover RPC methods on the server.
func (c *ClientConn) RegisterServer(
	qualifiedServerName string,
	server any,
	clientInterceptors []grpc.UnaryClientInterceptor,
	serverInterceptors []grpc.UnaryServerInterceptor,
) {
	serverVal := reflect.ValueOf(server)
	for i := 0; i < serverVal.Type().NumMethod(); i++ {
		reflectMethod := serverVal.Type().Method(i)
		methodVal := serverVal.MethodByName(reflectMethod.Name)

		// Check if this looks like a gRPC handler method:
		// func(ctx context.Context, req proto.Message) (proto.Message, error)
		methodType := methodVal.Type()
		validRPCMethod := methodType.Kind() == reflect.Func &&
			methodType.NumIn() == 2 &&
			methodType.NumOut() == 2 &&
			methodType.In(0) == contextType &&
			methodType.In(1).Implements(protoMessageType) &&
			methodType.Out(0).Implements(protoMessageType) &&
			methodType.Out(1) == errorType

		if !validRPCMethod {
			continue
		}

		fullMethod := "/" + qualifiedServerName + "/" + reflectMethod.Name
		c.methods[fullMethod] = &serviceMethod{
			info: grpc.UnaryServerInfo{Server: server, FullMethod: fullMethod},
			handler: func(ctx context.Context, req interface{}) (interface{}, error) {
				ret := methodVal.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)})
				err, _ := ret[1].Interface().(error)
				return ret[0].Interface(), err
			},
			clientInterceptor: chainUnaryClientInterceptors(clientInterceptors),
			serverInterceptor: chainUnaryServerInterceptors(serverInterceptors),
		}
	}
}

// Invoke implements grpc.ClientConnInterface.
func (c *ClientConn) Invoke(
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
		// Set up fake server transport stream for header/trailer capture
		stream := &fakeServerTransportStream{method: method}
		ctx = grpc.NewContextWithServerTransportStream(ctx, stream)

		// Move outgoing metadata to incoming
		md, _ := metadata.FromOutgoingContext(ctx)
		ctx = metadata.NewIncomingContext(ctx, md)
		ctx = metadata.NewOutgoingContext(ctx, metadata.MD{})

		// Call through server interceptor chain
		resp, err := sm.serverInterceptor(ctx, args, &sm.info, sm.handler)

		// Copy headers/trailers to call options
		for _, opt := range opts {
			if h, ok := opt.(grpc.HeaderCallOption); ok {
				*h.HeaderAddr = stream.header
			} else if t, ok := opt.(grpc.TrailerCallOption); ok {
				*t.TrailerAddr = stream.trailer
			}
		}

		// Merge response proto onto reply
		if respProto, _ := resp.(proto.Message); respProto != nil {
			proto.Merge(reply.(proto.Message), respProto)
		}

		return err
	}

	return sm.clientInterceptor(ctx, method, args, reply, nil, invoker, opts...)
}

// NewStream implements grpc.ClientConnInterface.
func (c *ClientConn) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return nil, errStreamNotSupported
}

// fakeServerTransportStream captures headers and trailers set by server interceptors.
type fakeServerTransportStream struct {
	method  string
	header  metadata.MD
	trailer metadata.MD
}

func (f *fakeServerTransportStream) Method() string               { return f.method }
func (f *fakeServerTransportStream) SetHeader(md metadata.MD) error {
	f.header = metadata.Join(f.header, md)
	return nil
}
func (f *fakeServerTransportStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeServerTransportStream) SetTrailer(md metadata.MD) error {
	f.trailer = metadata.Join(f.trailer, md)
	return nil
}

// Interceptor chaining helpers (from grpc-go)

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
			return interceptors[0](ctx, req, info, getChainUnaryHandler(interceptors, 0, info, handler))
		}
	}
}

func getChainUnaryHandler(
	interceptors []grpc.UnaryServerInterceptor,
	curr int,
	info *grpc.UnaryServerInfo,
	finalHandler grpc.UnaryHandler,
) grpc.UnaryHandler {
	if curr == len(interceptors)-1 {
		return finalHandler
	}
	return func(ctx context.Context, req any) (any, error) {
		return interceptors[curr+1](ctx, req, info, getChainUnaryHandler(interceptors, curr+1, info, finalHandler))
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
			return interceptors[0](ctx, method, req, reply, cc, getChainUnaryInvoker(interceptors, 0, invoker), opts...)
		}
	}
}

func getChainUnaryInvoker(interceptors []grpc.UnaryClientInterceptor, curr int, finalInvoker grpc.UnaryInvoker) grpc.UnaryInvoker {
	if curr == len(interceptors)-1 {
		return finalInvoker
	}
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return interceptors[curr+1](ctx, method, req, reply, cc, getChainUnaryInvoker(interceptors, curr+1, finalInvoker), opts...)
	}
}

// Global registry for address-based lookups

var (
	registryMu sync.RWMutex
	registry   = make(map[string]*ClientConn)
)

// Register registers a ClientConn at the given address for lookup.
func Register(addr string, conn *ClientConn) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[addr] = conn
}

// Unregister removes a ClientConn from the registry.
func Unregister(addr string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(registry, addr)
}

// GetInlineConn returns the inline ClientConn for the given address, or nil if none.
func GetInlineConn(addr string) *ClientConn {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return registry[addr]
}
