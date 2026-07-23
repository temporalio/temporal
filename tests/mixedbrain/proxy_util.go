package mixedbrain

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/siderolabs/grpc-proxy/proxy"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	errBackendAlreadyActive  = errors.New("backend is already active")
	errBackendNotFound       = errors.New("backend is not active")
	errBackendVersionChanged = errors.New("backend version changed")
)

type frontendBackend struct {
	name    string
	version string
	backend proxy.Backend
	count   atomic.Int64
}

// frontendProxy is a gRPC proxy that distributes RPCs round-robin across the
// active frontend backends.
type frontendProxy struct {
	listener net.Listener
	server   *grpc.Server

	mu       sync.RWMutex
	active   []*frontendBackend
	backends map[string]*frontendBackend
	conns    []*grpc.ClientConn
	next     atomic.Uint64

	wg sync.WaitGroup
}

func startFrontendProxy(t *testing.T) *frontendProxy {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &frontendProxy{
		listener: listener,
		backends: make(map[string]*frontendBackend),
	}
	codec := proxy.Codec()
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

func (p *frontendProxy) AddBackend(name, version, address string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, ok := p.backends[name]; ok {
		for _, active := range p.active {
			if active == existing {
				return errBackendAlreadyActive
			}
		}
		if existing.version != version {
			return errBackendVersionChanged
		}
	}

	codec := proxy.Codec()
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(codec)),
	)
	if err != nil {
		return err
	}
	backend := &proxy.SingleBackend{
		GetConn: func(ctx context.Context) (context.Context, *grpc.ClientConn, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			return metadata.NewOutgoingContext(ctx, md.Copy()), conn, nil
		},
	}

	entry, ok := p.backends[name]
	if !ok {
		entry = &frontendBackend{name: name, version: version}
		p.backends[name] = entry
	}
	entry.backend = backend
	p.active = append(p.active, entry)
	p.conns = append(p.conns, conn)
	return nil
}

func (p *frontendProxy) RemoveBackend(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.backends[name]
	if !ok {
		return errBackendNotFound
	}
	for i, active := range p.active {
		if active == entry {
			p.active = append(p.active[:i], p.active[i+1:]...)
			return nil
		}
	}
	return errBackendNotFound
}

func (p *frontendProxy) BackendCallCount(name string) (int64, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	backend, ok := p.backends[name]
	if !ok {
		return 0, false
	}
	return backend.count.Load(), true
}

func (p *frontendProxy) VersionCallCount(version string) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var count int64
	for _, backend := range p.backends {
		if backend.version == version {
			count += backend.count.Load()
		}
	}
	return count
}

func (p *frontendProxy) stop() {
	p.server.Stop()
	p.mu.Lock()
	for _, conn := range p.conns {
		_ = conn.Close()
	}
	p.active = nil
	p.mu.Unlock()
	_ = p.listener.Close()
	p.wg.Wait()
}

func (p *frontendProxy) director(context.Context, string) (proxy.Mode, []proxy.Backend, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.active) == 0 {
		return proxy.One2One, nil, status.Error(codes.Unavailable, "no frontend backends are active")
	}

	idx := p.next.Add(1) - 1
	backend := p.active[idx%uint64(len(p.active))]
	backend.count.Add(1)
	return proxy.One2One, []proxy.Backend{backend.backend}, nil
}
