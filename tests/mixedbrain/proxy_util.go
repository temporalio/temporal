package mixedbrain

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/siderolabs/grpc-proxy/proxy"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
)

// frontendProxy is a gRPC proxy that distributes RPCs round-robin across
// multiple frontend backends, ensuring Omes exercises both servers.
type frontendProxy struct {
	listener  net.Listener
	server    *grpc.Server
	conns     []*grpc.ClientConn
	backends  []proxy.Backend
	callCount []atomic.Int64
	next      atomic.Int64
	wg        sync.WaitGroup
}

func startFrontendProxy(t *testing.T, addresses ...string) *frontendProxy {
	t.Helper()
	require.NotEmpty(t, addresses)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &frontendProxy{
		listener:  listener,
		conns:     make([]*grpc.ClientConn, 0, len(addresses)),
		backends:  make([]proxy.Backend, 0, len(addresses)),
		callCount: make([]atomic.Int64, len(addresses)),
	}
	codec := proxy.Codec()
	for _, address := range addresses {
		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.ForceCodecV2(codec)),
		)
		require.NoError(t, err)
		p.conns = append(p.conns, conn)
		p.backends = append(p.backends, &proxy.SingleBackend{
			GetConn: func(ctx context.Context) (context.Context, *grpc.ClientConn, error) {
				md, _ := metadata.FromIncomingContext(ctx)
				outCtx := metadata.NewOutgoingContext(ctx, md.Copy())
				return outCtx, conn, nil
			},
		})
	}

	p.server = grpc.NewServer(
		grpc.ForceServerCodecV2(codec),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(p.director)),
	)
	p.wg.Go(func() {
		_ = p.server.Serve(listener)
	})
	return p
}

func (p *frontendProxy) addr() string {
	return p.listener.Addr().String()
}

func (p *frontendProxy) stop() {
	p.server.Stop()
	for _, conn := range p.conns {
		_ = conn.Close()
	}
	_ = p.listener.Close()
	p.wg.Wait()
}

func (p *frontendProxy) director(ctx context.Context, _ string) (proxy.Mode, []proxy.Backend, error) {
	idx := int(p.next.Add(1)-1) % len(p.backends)
	p.callCount[idx].Add(1)

	return proxy.One2One, p.backends[idx : idx+1], nil
}
