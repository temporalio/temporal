package fakegrpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"reflect"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/proto"

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

// resolveTarget resolves a gRPC target URL to an actual host:port address by
// delegating to the registered gRPC resolver (e.g. the membership:// scheme
// registered by common/membership). Returns the original target unchanged if
// the scheme has no registered resolver or resolution fails.
func resolveTarget(target string) string {
	u, err := url.Parse(target)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return target
	}
	b := resolver.Get(u.Scheme)
	if b == nil {
		return target
	}
	addrCh := make(chan string, 1)
	cc := &resolverCapture{ch: addrCh}
	r, err := b.Build(resolver.Target{URL: *u}, cc, resolver.BuildOptions{})
	if err != nil || r == nil {
		return target
	}
	// Clean up the resolver in the background so we don't block the caller
	// on wg.Wait() from within a cooperative goroutine.
	go r.Close()
	select {
	case addr := <-addrCh:
		return addr
	default:
		return target
	}
}

// resolverCapture is a minimal resolver.ClientConn that captures the first
// resolved address. It embeds resolver.ClientConn to satisfy any future
// interface extensions without code changes.
type resolverCapture struct {
	resolver.ClientConn
	ch chan string
}

func (c *resolverCapture) UpdateState(state resolver.State) error {
	if len(state.Addresses) > 0 {
		select {
		case c.ch <- state.Addresses[0].Addr:
		default:
		}
	}
	return nil
}

func (c *resolverCapture) ReportError(err error) {}

func NewClient(target string, opts ...DialOption) (*ClientConn, error) {
	resolved := resolveTarget(target)
	// resolveTarget may return a non-host:port target if resolution failed;
	// NormalizeAddr panics on bad input, so fall back to the raw target.
	addr := resolved
	if host, _, err := net.SplitHostPort(resolved); err == nil && host != "" {
		addr = SIMLIB.NormalizeAddr(resolved)
	}
	return &ClientConn{targetAddress: addr}, nil
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
		var ok bool
		srv, ok = serversByAddr[c.targetAddress]
		if ok {
			break
		}
		if ctx.Err() != nil {
			return fmt.Errorf("grpc connection to %v failed: %w", c.targetAddress, ctx.Err())
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
	if source.Type() == target.Type() {
		target.Set(source)
		return
	}
	// When the server returns a different (but proto-compatible) type than the client
	// expects (e.g. ResponseWithRawHistory vs Response in the matching service), use
	// proto marshal/unmarshal to bridge the gap, mirroring real gRPC wire serialization.
	fromMsg, fromOk := from.(proto.Message)
	toMsg, toOk := to.(proto.Message)
	if !fromOk || !toOk {
		panic(fmt.Sprintf("fakegrpc: cp: incompatible types %T -> %T", from, to))
	}
	b, err := proto.Marshal(fromMsg)
	if err != nil {
		panic(fmt.Sprintf("fakegrpc: cp: marshal %T: %v", from, err))
	}
	if err := proto.Unmarshal(b, toMsg); err != nil {
		panic(fmt.Sprintf("fakegrpc: cp: unmarshal %T: %v", to, err))
	}
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

func WithStreamInterceptor(f StreamClientInterceptor) DialOption {
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

func WithContextDialer(f func(context.Context, string) (net.Conn, error)) DialOption {
	return func(conn ClientConn) {
		// ignore: fakegrpc uses simulated in-process RPC, not real connections
	}
}

func WithStatsHandler(h stats.Handler) DialOption {
	return func(conn ClientConn) {
		// ignore
	}
}
