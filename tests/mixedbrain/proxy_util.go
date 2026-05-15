package mixedbrain

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// connRotateInterval is how often the proxy force-closes all open client
// connections so gRPC clients (which would otherwise multiplex every RPC over
// one HTTP/2 conn) are forced to redial. Each redial round-robins through the
// backends, breaking the frontend pinning for at minute-scale resolution.
const connRotateInterval = 15 * time.Second

// frontendProxy is a TCP proxy that distributes connections round-robin across
// multiple frontend backends, ensuring Omes exercises both servers.
type frontendProxy struct {
	listener  net.Listener
	backends  []string
	connCount []atomic.Int64
	next      atomic.Int64
	wg        sync.WaitGroup

	mu      sync.Mutex
	active  map[net.Conn]struct{}
	stopped bool

	cancel context.CancelFunc
}

func startFrontendProxy(t *testing.T, backends ...string) *frontendProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	p := &frontendProxy{
		listener:  listener,
		backends:  backends,
		connCount: make([]atomic.Int64, len(backends)),
		active:    make(map[net.Conn]struct{}),
		cancel:    cancel,
	}
	go p.serve()
	go p.rotate(ctx)
	return p
}

func (p *frontendProxy) addr() string {
	return p.listener.Addr().String()
}

func (p *frontendProxy) stop() {
	p.cancel()
	p.mu.Lock()
	p.stopped = true
	for c := range p.active {
		_ = c.Close()
	}
	p.active = nil
	p.mu.Unlock()
	_ = p.listener.Close()
	p.wg.Wait()
}

func (p *frontendProxy) serve() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		idx := int(p.next.Add(1)-1) % len(p.backends)
		p.connCount[idx].Add(1)
		if !p.track(conn) {
			_ = conn.Close()
			continue
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer p.untrack(conn)
			p.proxyConn(conn, p.backends[idx])
		}()
	}
}

// rotate periodically closes every active client connection so gRPC clients
// are forced to redial through the round-robin Accept path.
func (p *frontendProxy) rotate(ctx context.Context) {
	ticker := time.NewTicker(connRotateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.Lock()
			for c := range p.active {
				_ = c.Close()
			}
			p.mu.Unlock()
		}
	}
}

func (p *frontendProxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return false
	}
	p.active[c] = struct{}{}
	return true
}

func (p *frontendProxy) untrack(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.active, c)
}

func (p *frontendProxy) proxyConn(client net.Conn, backend string) {
	server, err := net.DialTimeout("tcp", backend, 5*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	p.wg.Go(func() {
		_, _ = io.Copy(server, client)
		_ = server.Close()
	})
	_, _ = io.Copy(client, server)
	_ = client.Close()
}
