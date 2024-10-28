package inline

import (
	"context"
	"errors"
	"reflect"

	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/interceptor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	errGRPCStreamNotSupported = errors.New("stream not supported")
)

// inlineClientConn is a [grpc.ClientConnInterface] implementation that forwards
// requests directly to gRPC via interceptors. This implementation moves all
// outgoing metadata to incoming and takes resulting outgoing metadata and sets
// as header. But which headers to use and TLS peer context and such are
// expected to be handled by the caller.
//
// RegisterServer must not be called concurrently with itself or with Invoke,
// but after all RegisterServer calls are done (in server initialization),
// Invoke may be called concurrently.
type inlineClientConn struct {
	methods map[string]*serviceMethod
}

var _ grpc.ClientConnInterface = (*inlineClientConn)(nil)

type serviceMethod struct {
	info              grpc.UnaryServerInfo
	handler           grpc.UnaryHandler
	clientInterceptor grpc.UnaryClientInterceptor
	serverInterceptor grpc.UnaryServerInterceptor
	requestCounter    metrics.CounterIface
	namespaceRegistry namespace.Registry
	fakeClientConn    *grpc.ClientConn
}

var contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
var protoMessageType = reflect.TypeOf((*proto.Message)(nil)).Elem()
var errorType = reflect.TypeOf((*error)(nil)).Elem()

func init() {
	// This must be done at static init time.
	resolver.Register(inlineGrpcBuilder{})
}

func NewInlineClientConn() *inlineClientConn {
	return &inlineClientConn{
		methods: make(map[string]*serviceMethod),
	}
}

// RegisterServer adds a server to the inlineClientConn. This must not be called concurrently.
func (icc *inlineClientConn) RegisterServer(
	qualifiedServerName string,
	server any,
	clientInterceptors []grpc.UnaryClientInterceptor,
	serverInterceptors []grpc.UnaryServerInterceptor,
	requestCounter metrics.CounterIface,
	namespaceRegistry namespace.Registry,
) {
	// Create a fake "real" *grpc.ClientConn just for client interceptors, since the function
	// signature requires one. Our interceptors don't refer to the cc parameter, but the
	// otelgrpc interceptor does, it calls cc.Target(). This will cause that to return
	// "internal://...". We register an actual grpc resolver builder for the "internal" scheme
	// to prevent grpc from falling back to dns. (Note that even if it falls back to dns, it
	// won't do a lookup until the ClientConn is actually used. But it's safer to do this.)
	target := inlineScheme + "://" + qualifiedServerName
	fakeClientConn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}

	// Create the set of methods via reflection. We currently accept the overhead
	// of reflection compared to having to custom generate gateway code.
	serverVal := reflect.ValueOf(server)
	for i := 0; i < serverVal.Type().NumMethod(); i++ {
		reflectMethod := serverVal.Type().Method(i)
		// We intentionally look this up by name to not assume method indexes line
		// up from type to value
		methodVal := serverVal.MethodByName(reflectMethod.Name)
		// We assume the methods we want only accept a context + request and only
		// return a response + error. We also assume the method name matches the
		// RPC name.
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
		icc.methods[fullMethod] = &serviceMethod{
			info: grpc.UnaryServerInfo{Server: server, FullMethod: fullMethod},
			handler: func(ctx context.Context, req interface{}) (interface{}, error) {
				ret := methodVal.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)})
				err, _ := ret[1].Interface().(error)
				return ret[0].Interface(), err
			},
			clientInterceptor: chainUnaryClientInterceptors(clientInterceptors),
			serverInterceptor: chainUnaryServerInterceptors(serverInterceptors),
			requestCounter:    requestCounter,
			namespaceRegistry: namespaceRegistry,
			fakeClientConn:    fakeClientConn,
		}
	}
}

func (icc *inlineClientConn) Invoke(
	ctx context.Context,
	method string,
	args any,
	reply any,
	opts ...grpc.CallOption,
) error {
	// Get the method. Should never fail, but we check anyways
	serviceMethod := icc.methods[method]
	if serviceMethod == nil {
		return status.Error(codes.NotFound, "call not found")
	}

	// Add metric
	var namespaceTag metrics.Tag
	if namespaceName := interceptor.MustGetNamespaceName(serviceMethod.namespaceRegistry, args); namespaceName != "" {
		namespaceTag = metrics.NamespaceTag(namespaceName.String())
	} else {
		namespaceTag = metrics.NamespaceUnknownTag()
	}
	serviceMethod.requestCounter.Record(1, metrics.OperationTag(method), namespaceTag)

	// Invoke
	invoker := func(ctx context.Context, method string, req, reply any, _ *grpc.ClientConn, opts ...grpc.CallOption) error {
		// We are now after all client interceptors, and before all server interceptors.

		// Add a fake ServerTransportStream to capture any headers or trailers set by server interceptors.
		stream := fakeServerTransportStream{method: method}
		ctx = grpc.NewContextWithServerTransportStream(ctx, &stream)

		// Move outgoing metadata to incoming and set new outgoing metadata
		md, _ := metadata.FromOutgoingContext(ctx)
		ctx = metadata.NewIncomingContext(ctx, md)
		outgoingMD := metadata.MD{}
		ctx = metadata.NewOutgoingContext(ctx, outgoingMD)

		resp, err := serviceMethod.serverInterceptor(ctx, args, &serviceMethod.info, serviceMethod.handler)

		// Find the header/trailer call option and set response headers. We accept that if
		// somewhere internally the metadata was replaced instead of appended to, this
		// does not work.
		for _, opt := range opts {
			if callOpt, ok := opt.(grpc.HeaderCallOption); ok {
				*callOpt.HeaderAddr = metadata.Join(outgoingMD, stream.header)
			} else if trailerOpt, ok := opt.(grpc.TrailerCallOption); ok {
				*trailerOpt.TrailerAddr = stream.trailer
			}
		}

		// Merge the response proto onto the wanted reply if non-nil
		// TODO: is there any way to optimize this to not call Merge and do a "move" instead?
		if respProto, _ := resp.(proto.Message); respProto != nil {
			proto.Merge(reply.(proto.Message), respProto)
		}

		return err
	}

	return serviceMethod.clientInterceptor(ctx, method, args, reply, serviceMethod.fakeClientConn, invoker, opts...)
}

func (*inlineClientConn) NewStream(
	context.Context,
	*grpc.StreamDesc,
	string,
	...grpc.CallOption,
) (grpc.ClientStream, error) {
	return nil, errGRPCStreamNotSupported
}

// Mostly taken from https://github.com/grpc/grpc-go/blob/v1.56.1/server.go#L1124-L1158
// with slight modifications.
func chainUnaryServerInterceptors(interceptors []grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	switch len(interceptors) {
	case 0:
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
			return handler(ctx, req)
		}
	case 1:
		return interceptors[0]
	default:
		return chainUnaryInterceptors(interceptors)
	}
}

func chainUnaryInterceptors(interceptors []grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return interceptors[0](ctx, req, info, getChainUnaryHandler(interceptors, 0, info, handler))
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
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return interceptors[curr+1](ctx, req, info, getChainUnaryHandler(interceptors, curr+1, info, finalHandler))
	}
}

// Mostly taken from https://github.com/grpc/grpc-go/blob/v1.66.0/clientconn.go
// with modifications.
func chainUnaryClientInterceptors(interceptors []grpc.UnaryClientInterceptor) grpc.UnaryClientInterceptor {
	if len(interceptors) == 0 {
		return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
	} else if len(interceptors) == 1 {
		return interceptors[0]
	} else {
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

type fakeServerTransportStream struct {
	method  string
	header  metadata.MD
	trailer metadata.MD
}

func (f *fakeServerTransportStream) Method() string {
	return f.method
}

func (f *fakeServerTransportStream) SetHeader(md metadata.MD) error {
	f.header = metadata.Join(f.header, md)
	return nil
}

func (f *fakeServerTransportStream) SendHeader(md metadata.MD) error {
	panic("not implemented")
}

func (f *fakeServerTransportStream) SetTrailer(md metadata.MD) error {
	f.trailer = metadata.Join(f.trailer, md)
	return nil
}

type inlineGrpcBuilder struct{}

const inlineScheme = "inline"

func (i inlineGrpcBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	// This resolver is never actually used so we should not get here.
	panic("not implemented")
}

func (i inlineGrpcBuilder) Scheme() string {
	return inlineScheme
}
