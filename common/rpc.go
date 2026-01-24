//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination rpc_mock.go

package common

import (
	"net"
	"net/http"

	"google.golang.org/grpc"
)

// RPCFactory creates gRPC listeners and connections, and frontend HTTP clients.
type RPCFactory interface {
	GetFrontendGRPCServerOptions() ([]grpc.ServerOption, error)
	GetInternodeGRPCServerOptions() ([]grpc.ServerOption, error)
	GetGRPCListener() net.Listener
	CreateRemoteFrontendGRPCConnection(rpcAddress string) grpc.ClientConnInterface
	CreateLocalFrontendGRPCConnection() grpc.ClientConnInterface
	CreateHistoryGRPCConnection(rpcAddress string) grpc.ClientConnInterface
	CreateMatchingGRPCConnection(rpcAddress string) grpc.ClientConnInterface
	CreateLocalFrontendHTTPClient() (*FrontendHTTPClient, error)
}

type FrontendHTTPClient struct {
	http.Client
	// Address is the host:port pair of this HTTP client.
	Address string
	// Scheme is the URL scheme of this HTTP client.
	Scheme string
}

// BaseURL is the scheme and address of this HTTP client.
func (c *FrontendHTTPClient) BaseURL() string {
	return c.Scheme + "://" + c.Address
}
