package interceptor

import (
	"context"
	"crypto/md5"
	"fmt"

	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/interceptor/nexus"
	"go.temporal.io/server/common/rpc/tlsinfo"
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
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {

	if nli.logger != nil {
		methodName := api.MethodName(info.FullMethod)
		namespace := MustGetNamespaceName(nli.namespaceRegistry, req)
		tlsInfo := tlsinfo.FromContext(ctx)
		var serverName string
		var certThumbprint string
		if tlsInfo != nil {
			serverName = tlsInfo.State.ServerName
			cert := tlsinfo.PeerCert(tlsInfo)
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

func (nli *NamespaceLogInterceptor) InterceptNexus(
	ctx context.Context,
	in nexus.InterceptorInput,
	next nexus.HandlerFunc,
) (any, error) {
	if nli.logger != nil {
		methodName := api.MethodName(in.APIName())
		namespaceName := MustGetNamespaceName(nli.namespaceRegistry, in)
		tlsInfo := tlsinfo.FromContext(ctx)
		var serverName string
		var certThumbprint string
		if tlsInfo != nil {
			serverName = tlsInfo.State.ServerName
			cert := tlsinfo.PeerCert(tlsInfo)
			if cert != nil {
				certThumbprint = fmt.Sprintf("%x", md5.Sum(cert.Raw))
			}
		}
		nli.logger.Debug(
			"Frontend method invoked.",
			tag.WorkflowNamespace(namespaceName.String()),
			tag.Operation(methodName),
			tag.ServerName(serverName),
			tag.CertThumbprint(certThumbprint))
	}
	return next(ctx, in)
}
