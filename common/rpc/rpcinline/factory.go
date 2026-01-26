package rpcinline

import (
	"fmt"
	"net"
	"sync"

	"go.temporal.io/server/common"
	"google.golang.org/grpc"
)

var _ common.RPCFactory = (*Factory)(nil)

// Factory wraps an RPCFactory to use inline connections for inter-service calls.
// It panics if an inline connection is requested but not available.
type Factory struct {
	base    common.RPCFactory
	parent  *Factory
	mu      sync.RWMutex
	servers map[string]*Server
}

// NewFactory creates a new inline RPC factory.
func NewFactory() *Factory {
	return &Factory{
		servers: make(map[string]*Server),
	}
}

// AddServer registers a server for inline RPC.
func (f *Factory) AddServer(s *Server) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.servers[s.Addr()] = s
}

// WrapFactory creates a child factory that delegates server lookups to this factory.
func (f *Factory) WrapFactory(base common.RPCFactory) *Factory {
	return &Factory{
		base:   base,
		parent: f,
	}
}

func (f *Factory) getInlineConn(addr string) grpc.ClientConnInterface {
	if f.parent != nil {
		return f.parent.getInlineConn(addr)
	}

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		panic(fmt.Sprintf("rpcinline: invalid address %q: %v", addr, err))
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	// Match by port since host representations vary (127.0.0.1 vs [::] vs 0.0.0.0)
	for serverAddr, s := range f.servers {
		_, serverPort, err := net.SplitHostPort(serverAddr)
		if err != nil {
			continue
		}
		if port == serverPort {
			return s.Conn()
		}
	}

	// List registered servers for debugging
	var registered []string
	for addr := range f.servers {
		registered = append(registered, addr)
	}
	panic(fmt.Sprintf("rpcinline: no server registered for %q (registered: %v)", addr, registered))
}

func (f *Factory) GetFrontendGRPCServerOptions() ([]grpc.ServerOption, error) {
	return f.base.GetFrontendGRPCServerOptions()
}

func (f *Factory) GetInternodeGRPCServerOptions() ([]grpc.ServerOption, error) {
	return f.base.GetInternodeGRPCServerOptions()
}

func (f *Factory) GetGRPCListener() net.Listener {
	return f.base.GetGRPCListener()
}

func (f *Factory) CreateRemoteFrontendGRPCConnection(rpcAddress string) grpc.ClientConnInterface {
	// Frontend connections use network - frontend may not be started yet when other services initialize
	return f.base.CreateRemoteFrontendGRPCConnection(rpcAddress)
}

func (f *Factory) CreateLocalFrontendGRPCConnection() grpc.ClientConnInterface {
	// Frontend connections use network - frontend may not be started yet when other services initialize
	return f.base.CreateLocalFrontendGRPCConnection()
}

func (f *Factory) CreateLocalFrontendHTTPClient() (*common.FrontendHTTPClient, error) {
	return f.base.CreateLocalFrontendHTTPClient()
}

func (f *Factory) CreateHistoryGRPCConnection(rpcAddress string) grpc.ClientConnInterface {
	return f.getInlineConn(rpcAddress)
}

func (f *Factory) CreateMatchingGRPCConnection(rpcAddress string) grpc.ClientConnInterface {
	return f.getInlineConn(rpcAddress)
}
