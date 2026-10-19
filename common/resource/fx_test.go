package resource_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/faultinjection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/auth"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

// These outputs supply only the dependencies required before configured faults were added.
// ValidateApp checks the graph without invoking their constructors.
type standaloneDependencies struct {
	fx.Out
	Config                  *config.Config
	TLS                     encryption.TLSConfigProvider
	Metrics                 metrics.Handler
	Logger                  log.Logger
	ServiceLogger           log.SnTaggedLogger
	Resolver                *membership.GRPCResolver
	DynamicConfig           *dynamicconfig.Collection
	Service                 primitives.ServiceName
	DialOptions             map[primitives.ServiceName][]grpc.DialOption
	Monitor                 membership.Monitor
	TokenProvider           auth.TokenProvider
	Tracing                 telemetry.ClientStatsHandler
	Instrumenter            telemetry.HTTPClientTransportInstrumenter
	Metadata                cluster.Metadata
	ServiceErrorInterceptor *interceptor.ServiceErrorInterceptor
	RetryableInterceptor    *interceptor.RetryableInterceptor
	TelemetryInterceptor    *interceptor.TelemetryInterceptor
	RateLimitInterceptor    *interceptor.RateLimitInterceptor
	ServerTracing           telemetry.ServerStatsHandler
	ServerMetrics           metrics.ServerStatsHandler
	TestHooks               testhooks.TestHooks
}

func TestStandaloneProvidersWithoutFaultInjection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		providers []any
		consume   any
	}{
		{"SDK client factory", []any{resource.SdkClientFactoryProvider}, func(sdk.ClientFactory) {}},
		{"RPC factory", []any{resource.RPCFactoryProvider}, func(common.RPCFactory) {}},
		{"HTTP client cache", []any{resource.FrontendHTTPClientCacheProvider}, func(*cluster.FrontendHTTPClientCache) {}},
		{"gRPC server", []any{resource.RPCFactoryProvider, service.GrpcServerOptionsProvider}, func([]grpc.ServerOption) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, fx.ValidateApp(fx.NopLogger,
				fx.Provide(func() standaloneDependencies { return standaloneDependencies{} }),
				fx.Provide(tc.providers...),
				fx.Invoke(tc.consume),
			))
		})
	}
}

func TestGRPCFaultParamsPreserveStandaloneHooks(t *testing.T) {
	t.Parallel()
	for _, configured := range []bool{false, true} {
		name := "standalone"
		if configured {
			name = "configured"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			hooks := testhooks.NewTestHooks()
			injected := errors.New("runtime gRPC fault")
			unregister := faultinjectiontest.NewServiceCallbackGenerator(hooks).RegisterRequestCallback(grpcfaults.Scope{NamespaceName: "namespace"}, func(context.Context, string, any) *grpcfaults.Outcome { return &grpcfaults.Outcome{Error: injected} })
			defer unregister()
			opts := []fx.Option{fx.NopLogger, fx.Supply(&config.Config{FaultInjection: config.RPCFaultInjection{GRPC: &config.TransportFaultInjection{Inbound: &config.CallFaultInjection{Request: faultinjection.Config{Errors: map[string]float64{"Unavailable": 1}}}}}}, hooks)}
			if configured {
				opts = append(opts, fx.Provide(resource.GRPCFaultGeneratorsProvider))
			}
			var params resource.GRPCFaultParams
			opts = append(opts, fx.Populate(&params))
			require.NoError(t, fx.New(opts...).Err())
			req := &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: "namespace"}
			require.ErrorIs(t, params.Inbound().GenerateRequest(t.Context(), "operation", req).Error, injected)
			unregister()
			outcome := params.Inbound().GenerateRequest(t.Context(), "operation", req)
			if configured {
				require.ErrorContains(t, outcome.Error, "fault injection: Unavailable")
			} else {
				require.Nil(t, outcome)
			}
		})
	}
}
