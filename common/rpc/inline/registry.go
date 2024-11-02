package inline

import (
	"sync"

	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

var inlineConns sync.Map

func RegisterInlineServer(
	selfHostName string,
	qualifiedServerName string,
	server any,
	clientInterceptors []grpc.UnaryClientInterceptor,
	serverInterceptors []grpc.UnaryServerInterceptor,
	requestCounter metrics.CounterIface,
	namespaceRegistry namespace.Registry,
) {
	v, _ := inlineConns.LoadOrStore(selfHostName, NewInlineClientConn())
	cc := v.(*inlineClientConn)
	cc.RegisterServer(qualifiedServerName, server, clientInterceptors, serverInterceptors, requestCounter, namespaceRegistry)
}

func GetInlineConn(hostName string) grpc.ClientConnInterface {
	if v, ok := inlineConns.Load(hostName); ok {
		return v.(grpc.ClientConnInterface)
	}
	return nil
}
