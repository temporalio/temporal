package frontend

import (
	"context"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/rpc/interceptor/nexus"
	"google.golang.org/grpc"
)

// Interceptor is a unified interface for gRPC and Nexus interceptors
type Interceptor interface {
	// gRPC Interceptor
	Intercept(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error)
	// Nexus Interceptor
	InterceptNexus(
		ctx context.Context,
		in nexus.InterceptorInput,
		next nexus.HandlerFunc,
	) (any, error)
}

type interceptorsProvider struct {
	interceptors   []Interceptor
	nexusTelemetry nexus.Interceptor // required to be first in the Nexus chain
}

func newInterceptorsProvider(
	maskInternalErrorDetailsInterceptor *interceptor.MaskInternalErrorDetailsInterceptor,
	serviceErrorInterceptor *interceptor.ServiceErrorInterceptor,
	frontendServiceErrorInterceptor *interceptor.FrontendServiceErrorInterceptor,
	businessIDInterceptor *interceptor.RoutingKeyInterceptor,
	namespaceValidatorInterceptor *interceptor.NamespaceValidatorInterceptor,
	namespaceLogInterceptor *interceptor.NamespaceLogInterceptor,
	authInterceptor *authorization.Interceptor,
	namespaceHandoverInterceptor *interceptor.NamespaceHandoverInterceptor,
	redirectionInterceptor *interceptor.Redirection,
	nexusForwarder *nexusForwardingInterceptor,
	telemetryInterceptor *interceptor.TelemetryInterceptor,
	healthInterceptor *interceptor.HealthInterceptor,
	namespaceLengthValidatorInterceptor *interceptor.NamespaceLengthValidatorInterceptor,
	namespaceCountLimiterInterceptor *interceptor.ConcurrentRequestLimitInterceptor,
	namespaceRateLimiterInterceptorWrapper *interceptor.NamespaceRateLimitInterceptorWrapper,
	rateLimitInterceptor *interceptor.RateLimitInterceptor,
	sdkVersionInterceptor *interceptor.SDKVersionInterceptor,
	callerInfoInterceptor *interceptor.CallerInfoInterceptor,
	slowRequestLoggerInterceptor *interceptor.SlowRequestLoggerInterceptor,
	chasmRequestVisibilityInterceptor *chasm.ChasmVisibilityInterceptor,
	contextMetadataInterceptor *interceptor.ContextMetadataInterceptor,
	customGRPCInterceptors []grpc.UnaryServerInterceptor,
	customInterceptors []Interceptor,
	retryableInterceptor *interceptor.RetryableInterceptor,
	faultsInterceptor *grpcfaults.FaultsInterceptor,
) *interceptorsProvider {

	metricsCtxInjectorInterceptor := &interceptorWrapper{
		grpcInterceptor:  metrics.NewServerMetricsContextInjectorInterceptor(),
		nexusInterceptor: nexusNoOpInterceptor, // added by telemetryInterceptor.InterceptNexusOutermost
	}

	// redirectionWrapper is one chain position for both transports: gRPC DC redirection
	// and Nexus HTTP forwarding. The implementations stay separate but are wrapped together
	// for canonical ordering of interceptors for both gRPC and Nexus
	redirectionWrapper := &interceptorWrapper{
		grpcInterceptor:  redirectionInterceptor.Intercept,
		nexusInterceptor: nexusForwarder.InterceptNexus,
	}

	// Order is important. Error interceptors must stay outermost, routing must precede namespace
	// access, and telemetry must follow redirection to attribute requests to the serving cluster.
	// Nexus interceptors outward of error producers must preserve InterceptorError.
	interceptors := []Interceptor{
		maskInternalErrorDetailsInterceptor,
		serviceErrorInterceptor,
		frontendServiceErrorInterceptor,
		businessIDInterceptor,
		namespaceLengthValidatorInterceptor,
		namespaceLogInterceptor,
		metricsCtxInjectorInterceptor,
		authInterceptor,
		namespaceHandoverInterceptor,
		redirectionWrapper,
		telemetryInterceptor,
		healthInterceptor,
		namespaceValidatorInterceptor,
		namespaceCountLimiterInterceptor,
		namespaceRateLimiterInterceptorWrapper,
		rateLimitInterceptor,
		sdkVersionInterceptor,
		callerInfoInterceptor,
		slowRequestLoggerInterceptor,
		chasmRequestVisibilityInterceptor,
		contextMetadataInterceptor,
	}
	for _, grpcInterceptor := range customGRPCInterceptors {
		interceptors = append(interceptors, &interceptorWrapper{
			grpcInterceptor:  grpcInterceptor,
			nexusInterceptor: nexusNoOpInterceptor,
		})
	}
	interceptors = append(interceptors, customInterceptors...)

	interceptors = append(interceptors, faultsInterceptor)
	interceptors = append(interceptors, retryableInterceptor)

	return &interceptorsProvider{
		interceptors:   interceptors,
		nexusTelemetry: telemetryInterceptor.InterceptNexusOutermost,
	}
}

func (n *interceptorsProvider) grpcInterceptors() []grpc.UnaryServerInterceptor {
	grpcInterceptors := make([]grpc.UnaryServerInterceptor, 0, len(n.interceptors))
	for _, i := range n.interceptors {
		grpcInterceptors = append(grpcInterceptors, i.Intercept)
	}
	return grpcInterceptors
}

func (n *interceptorsProvider) nexusInterceptors() []nexus.Interceptor {
	nexusInterceptors := make([]nexus.Interceptor, 0, len(n.interceptors)+1)
	// telemetry is the outermost in chain for Nexus requests to allow recording
	// all metrics and retain behavior. In the future, gRPC will also move telemetry
	// to outermost after an impact evaluation- this will allow gRPC to also capture
	// all metrics from authz/redirection related failures as well.
	nexusInterceptors = append(nexusInterceptors, n.nexusTelemetry)
	for _, i := range n.interceptors {
		nexusInterceptors = append(nexusInterceptors, i.InterceptNexus)
	}
	return nexusInterceptors
}

type interceptorWrapper struct {
	grpcInterceptor  grpc.UnaryServerInterceptor
	nexusInterceptor nexus.Interceptor
}

func (i interceptorWrapper) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	return i.grpcInterceptor(ctx, req, info, handler)
}

func (i interceptorWrapper) InterceptNexus(
	ctx context.Context,
	in nexus.InterceptorInput,
	next nexus.HandlerFunc,
) (any, error) {
	return i.nexusInterceptor(ctx, in, next)
}

func nexusNoOpInterceptor(
	ctx context.Context,
	in nexus.InterceptorInput,
	next nexus.HandlerFunc,
) (any, error) {
	return next(ctx, in)
}
