package interceptor

import (
	"context"
	"crypto/md5"
	"fmt"

	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

type (
	NamespaceLogInterceptor struct {
		namespaceRegistry namespace.Registry
		logger            log.Logger
	}
)

var _ grpc.UnaryServerInterceptor = (*NamespaceLogInterceptor)(nil).Intercept

func NewNamespaceLogInterceptor(namespaceRegistry namespace.Registry, logger log.Logger) *NamespaceLogInterceptor {

	return &NamespaceLogInterceptor{
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
	}
}

func (nli *NamespaceLogInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	if nli.logger != nil {
		methodName := api.MethodName(info.FullMethod)
		namespace := MustGetNamespaceName(nli.namespaceRegistry, req)
		tlsInfo := authorization.TLSInfoFromContext(ctx)
		var serverName string
		var certThumbprint string
		if tlsInfo != nil {
			serverName = tlsInfo.State.ServerName
			cert := authorization.PeerCert(tlsInfo)
			if cert != nil {
				certThumbprint = fmt.Sprintf("%x", md5.Sum(cert.Raw))
			}
		}
		nli.logger.Debug(
			"Frontend method invoked.",
			tag.WorkflowNamespace(namespace.String()),
			tag.Operation(methodName),
			tag.ServerName(serverName),
			tag.CertThumbprint(certThumbprint))
	}
	return handler(ctx, req)
}
