package interceptor

import (
	"context"

	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

type (
	CallerInfoInterceptor struct {
		namespaceRegistry namespace.Registry
	}
)

var _ grpc.UnaryServerInterceptor = (*CallerInfoInterceptor)(nil).Intercept

func NewCallerInfoInterceptor(
	namespaceRegistry namespace.Registry,
) *CallerInfoInterceptor {
	return &CallerInfoInterceptor{
		namespaceRegistry: namespaceRegistry,
	}
}

func (i *CallerInfoInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	ctx = PopulateCallerInfo(
		ctx,
		func() string { return string(MustGetNamespaceName(i.namespaceRegistry, req)) },
		func() string { return api.MethodName(info.FullMethod) },
	)

	return handler(ctx, req)
}

// PopulateCallerInfo gets current caller info value from the context and updates any that are missing.
// Namespace name and method are passed as functions to avoid expensive lookups if those values are already set.
func PopulateCallerInfo(
	ctx context.Context,
	nsNameGetter func() string,
	methodGetter func() string,
) context.Context {
	callerInfo := headers.GetCallerInfo(ctx)

	infoUpdated := false

	nsName := nsNameGetter()
	if callerInfo.CallerName != nsName {
		callerInfo.CallerName = nsName
		infoUpdated = true
	}

	_, isValidCallerType := headers.ValidCallerTypes[callerInfo.CallerType]
	if !isValidCallerType {
		callerInfo.CallerType = headers.CallerTypeAPI
		infoUpdated = true
	}

	if callerInfo.CallerType == headers.CallerTypeAPI ||
		callerInfo.CallerType == headers.CallerTypeOperator {
		methodName := methodGetter()
		if callerInfo.CallOrigin != methodName {
			callerInfo.CallOrigin = methodName
			infoUpdated = true
		}
	}

	if infoUpdated {
		ctx = headers.SetCallerInfo(ctx, callerInfo)
	}

	return ctx
}
