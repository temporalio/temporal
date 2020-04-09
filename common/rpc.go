package common

import (
	"net"

	"github.com/uber/tchannel-go"
	"google.golang.org/grpc"
)

type (
	// RPCFactory creates gRPC listener and connection.
	RPCFactory interface {
		GetGRPCListener() net.Listener
		GetRingpopChannel() *tchannel.Channel
		CreateGRPCConnection(hostName string) *grpc.ClientConn
	}
)
