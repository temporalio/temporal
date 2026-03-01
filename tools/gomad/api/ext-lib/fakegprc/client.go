package fakegrpc

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	SIMLIB "go.temporal.io/server/tools/gomad/api/lib"
)

var _ clientConn = &ClientConn{}

type (
	clientConn interface {
		io.Closer
		grpc.ClientConnInterface
	}
	ClientConn struct {
		targetAddress string
	}
	UnaryInvoker            func(ctx context.Context, method string, req, reply any, cc *ClientConn, opts ...grpc.CallOption) error
	UnaryClientInterceptor  func(ctx context.Context, method string, req, reply any, cc *ClientConn, invoker UnaryInvoker, opts ...grpc.CallOption) error
	StreamClientInterceptor func(ctx context.Context, desc *grpc.StreamDesc, cc *ClientConn, method string, streamer Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error)
	Streamer                func(ctx context.Context, desc *grpc.StreamDesc, cc *ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error)
	DialOption              func(conn ClientConn)
	methodHandler           func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error)
)

func NewClient(target string, opts ...DialOption) (*ClientConn, error) {
	if target == "membership://frontend" {
		// TODO: remove hack
		target = "127.0.0.1:7233"
	}
	return &ClientConn{targetAddress: SIMLIB.NormalizeAddr(target)}, nil
}

// TODO: remove once Server migrates to NewClient
func DialContext(ctx context.Context, target string, opts ...DialOption) (*ClientConn, error) {
	return NewClient(target, opts...)
}

// TODO: remove once Server migrates to NewClient
func Dial(target string, opts ...DialOption) (*ClientConn, error) {
	return DialContext(context.Background(), target, opts...)
}

func (c *ClientConn) Invoke(ctx context.Context, method string, arg any, reply any, opts ...grpc.CallOption) error {
	var srv *Server
	for {
		if ctx.Err() != nil {
			return fmt.Errorf("grpc connection to %v failed: no server", c.targetAddress)
		}
		var ok bool
		srv, ok = serversByAddr[c.targetAddress]
		if ok {
			break
		}
		SIMLIB.Sleep(10 * time.Millisecond)
	}

	// simulate network latency
	SIMLIB.Sleep(10 * time.Millisecond)

	// TODO: move into server
	parts := strings.Split(method, "/")
	impl := srv.impls[parts[1]]
	handler := methodHandler(srv.methods[parts[1]][parts[2]].Handler)
	out, err := handler(impl, ctx, decode(arg), srv.interceptor)
	if err != nil {
		return err
	}
	cp(out, reply)
	return nil
}

func (c *ClientConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	panic("not implemented: fakegrpc.ClientConn/NewStream")
}

func (c *ClientConn) Close() error {
	// TODO?
	return nil
}

func (c *ClientConn) ResetConnectBackoff() {
}

func (c *ClientConn) Target() string {
	return "" // TODO?
}

func decode(from any) func(in any) error {
	return func(to any) error {
		cp(from, to)
		return nil
	}
}

// TODO: must copy struct to prevent changes
func cp(from, to any) {
	source := reflect.ValueOf(from).Elem()
	target := reflect.ValueOf(to).Elem()
	target.Set(source)
}

func WithUnaryInterceptor(f UnaryClientInterceptor) DialOption {
	return func(conn ClientConn) {
		// TODO
	}
}

func WithChainUnaryInterceptor(interceptors ...UnaryClientInterceptor) DialOption {
	return func(conn ClientConn) {
		// TODO
	}
}

func WithDefaultServiceConfig(s string) DialOption {
	return func(conn ClientConn) {
		// TODO
	}
}

func WithConnectParams(p grpc.ConnectParams) DialOption {
	return func(conn ClientConn) {
		// TODO
	}
}

func WithReadBufferSize(s int) DialOption {
	return func(conn ClientConn) {
		// ignore
	}
}

func WithWriteBufferSize(s int) DialOption {
	return func(conn ClientConn) {
		// ignore
	}
}

func WithUserAgent(s string) DialOption {
	return func(conn ClientConn) {
		// ignore
	}
}

func WithDisableServiceConfig() DialOption {
	return func(conn ClientConn) {
		// ignore
	}
}

func WithChainStreamInterceptor(interceptors ...StreamClientInterceptor) DialOption {
	return func(conn ClientConn) {
		// ignore
	}
}

func WithTransportCredentials(creds credentials.TransportCredentials) DialOption {
	return func(conn ClientConn) {
		// ignore
	}
}

func WithBlock() DialOption {
	return func(conn ClientConn) {
		// ignore
	}
}

func WithAuthority(a string) DialOption {
	return func(conn ClientConn) {
		// ignore
	}
}

func WithDefaultCallOptions(cos ...grpc.CallOption) DialOption {
	return func(conn ClientConn) {
		// ignore
	}
}

func WithKeepaliveParams(kp keepalive.ClientParameters) DialOption {
	return func(conn ClientConn) {
		// ignore
	}
}
