package frontend

import (
	"context"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/metrics"
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

type InterceptorsProvider struct {
	interceptors           []Interceptor
	nexusTelemetry         nexus.Interceptor                 // required to be first in the Nexus chain
	retryableInterceptor   *interceptor.RetryableInterceptor // required to be last in chain after custom interceptors
	customGRPCInterceptors []grpc.UnaryServerInterceptor     // required for legacy reasons
}

func NewInterceptorsProvider(
	maskInternalErrorDetailsInterceptor *interceptor.MaskInternalErrorDetailsInterceptor,
	serviceErrorInterceptor *interceptor.ServiceErrorInterceptor,
	frontendServiceErrorInterceptor *interceptor.FrontendServiceErrorInterceptor,
	businessIDInterceptor *interceptor.RoutingKeyInterceptor,
	namespaceValidatorInterceptor *interceptor.NamespaceValidatorInterceptor,
	namespaceLogInterceptor *interceptor.NamespaceLogInterceptor,
	metricsCtxInjectorInterceptor *metricsCtxInjectorInterceptor,
	authInterceptor *authorization.Interceptor,
	namespaceHandoverInterceptor *interceptor.NamespaceHandoverInterceptor,
	redirectionSlot *redirectionWrapper,
	telemetryInterceptor *interceptor.TelemetryInterceptor,
	healthInterceptor *interceptor.HealthInterceptor,
	namespaceStateValidatorInterceptor *interceptor.NamespaceStateValidatorInterceptor,
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
) *InterceptorsProvider {

	interceptors := []Interceptor{
		maskInternalErrorDetailsInterceptor,
		serviceErrorInterceptor,
		frontendServiceErrorInterceptor,
		businessIDInterceptor,
		namespaceStateValidatorInterceptor,
		namespaceLogInterceptor,
		metricsCtxInjectorInterceptor,
		authInterceptor,
		namespaceHandoverInterceptor,
		redirectionSlot,
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
	// it is debatable if this should be *after* customGRPCInterceptors that are
	// in use today. We will opt for this instead because relative ordering remains
	// unchanged and anyone using customInterceptors should deprecate customGRPCInterceptors entirely
	interceptors = append(interceptors, customInterceptors...)

	return &InterceptorsProvider{
		interceptors:           interceptors,
		nexusTelemetry:         telemetryInterceptor.InterceptNexusOutermost,
		customGRPCInterceptors: customGRPCInterceptors,
		retryableInterceptor:   retryableInterceptor,
	}
}

func (n *InterceptorsProvider) GrpcInterceptors() []grpc.UnaryServerInterceptor {
	grpcInterceptors := make([]grpc.UnaryServerInterceptor, 0, len(n.interceptors)+len(n.customGRPCInterceptors)+1)
	for _, i := range n.interceptors {
		grpcInterceptors = append(grpcInterceptors, i.Intercept)
	}
	// custom interceptors chain after system interceptors
	grpcInterceptors = append(grpcInterceptors, n.customGRPCInterceptors...)
	grpcInterceptors = append(grpcInterceptors, n.retryableInterceptor.Intercept)
	return grpcInterceptors
}

func (n *InterceptorsProvider) NexusInterceptors() []nexus.Interceptor {
	nexusInterceptors := make([]nexus.Interceptor, 0, len(n.interceptors)+2)
	// telemetry is the outermost in chain for Nexus requests to allow recording
	// all metrics and retain behavior. In the future, gRPC will also move telemetry
	// to outermost after an impact evaluation- this will allow gRPC to also capture
	// all metrics from authz/redirection related failures as well.
	nexusInterceptors = append(nexusInterceptors, n.nexusTelemetry)
	for _, i := range n.interceptors {
		nexusInterceptors = append(nexusInterceptors, i.InterceptNexus)
	}

	nexusInterceptors = append(nexusInterceptors, n.retryableInterceptor.InterceptNexus)
	return nexusInterceptors
}

// redirectionWrapper is one chain position for both transports: gRPC DC redirection
// and Nexus HTTP forwarding. The implementations stay separate but are wrapped together
// for canonical ordering of interceptors for both gRPC and Nexus
type redirectionWrapper struct {
	grpc  *interceptor.Redirection
	nexus *nexusForwardingInterceptor
}

func (s *redirectionWrapper) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	return s.grpc.Intercept(ctx, req, info, handler)
}

func (s *redirectionWrapper) InterceptNexus(
	ctx context.Context,
	in nexus.InterceptorInput,
	next nexus.HandlerFunc,
) (any, error) {
	return s.nexus.InterceptNexus(ctx, in, next)
}

// tiny wrapper to inject metrics context and avoid
// cyclical dependencies in metrics/interceptors packages
type metricsCtxInjectorInterceptor struct{}

func (m *metricsCtxInjectorInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	ctxWithMetricsBaggage := metrics.AddMetricsContext(ctx)
	return handler(ctxWithMetricsBaggage, req)
}

func (m *metricsCtxInjectorInterceptor) InterceptNexus(
	ctx context.Context,
	in nexus.InterceptorInput,
	next nexus.HandlerFunc,
) (any, error) {
	ctxWithMetricsBaggage := metrics.AddMetricsContext(ctx)
	return next(ctxWithMetricsBaggage, in)
}
