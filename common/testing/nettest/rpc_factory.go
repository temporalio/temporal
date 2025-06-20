package nettest

import (
	"context"
	"net"

	"go.temporal.io/server/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RPCFactory is a common.RPCFactory implementation that uses a PipeListener to create connections. It is useful for
// testing gRPC servers.
type RPCFactory struct {
	listener    *PipeListener
	dialOptions []grpc.DialOption
}

var _ common.RPCFactory = (*RPCFactory)(nil)

// NewRPCFactory creates a new RPCFactory backed by a PipeListener.
func NewRPCFactory(listener *PipeListener, dialOptions ...grpc.DialOption) *RPCFactory {
	return &RPCFactory{
		listener:    listener,
		dialOptions: dialOptions,
	}
}

func (f *RPCFactory) GetFrontendGRPCServerOptions() ([]grpc.ServerOption, error) {
	return nil, nil
}

func (f *RPCFactory) GetInternodeGRPCServerOptions() ([]grpc.ServerOption, error) {
	return nil, nil
}

func (f *RPCFactory) GetGRPCListener() net.Listener {
	return f.listener
}

func (f *RPCFactory) CreateRemoteFrontendGRPCConnection(rpcAddress string) *grpc.ClientConn {
	return f.dial(rpcAddress)
}

func (f *RPCFactory) CreateLocalFrontendGRPCConnection() *grpc.ClientConn {
	return f.dial(f.listener.Addr().String())
}

func (f *RPCFactory) CreateLocalFrontendHTTPClient() (*common.FrontendHTTPClient, error) {
	panic("unimplemented in the nettest package")
}

func (f *RPCFactory) CreateHistoryGRPCConnection(rpcAddress string) *grpc.ClientConn {
	return f.dial(rpcAddress)
}

func (f *RPCFactory) CreateMatchingGRPCConnection(rpcAddress string) *grpc.ClientConn {
	return f.dial(rpcAddress)
}

func (f *RPCFactory) dial(rpcAddress string) *grpc.ClientConn {
	dialOptions := append(f.dialOptions,
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return f.listener.Connect(ctx.Done())
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	conn, err := grpc.NewClient(
		rpcAddress,
		dialOptions...,
	)
	if err != nil {
		panic(err)
	}

	return conn
}
