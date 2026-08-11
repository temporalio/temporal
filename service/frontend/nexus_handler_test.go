package frontend

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/rpc/interceptor"
)

type mockAuthorizer struct{}

// Authorize implements authorization.Authorizer.
func (mockAuthorizer) Authorize(ctx context.Context, caller *authorization.Claims, target *authorization.CallTarget) (authorization.Result, error) {
	return authorization.Result{Decision: authorization.DecisionAllow}, nil
}

var _ authorization.Authorizer = mockAuthorizer{}

type mockRateLimiter struct {
	allow bool
}

// Allow implements quotas.RequestRateLimiter.
func (r mockRateLimiter) Allow(now time.Time, request quotas.Request) bool {
	return r.allow
}

// Reserve implements quotas.RequestRateLimiter.
func (mockRateLimiter) Reserve(now time.Time, request quotas.Request) quotas.Reservation {
	panic("unimplemented for test")
}

// Wait implements quotas.RequestRateLimiter.
func (mockRateLimiter) Wait(ctx context.Context, request quotas.Request) error {
	panic("unimplemented for test")
}

var _ quotas.RequestRateLimiter = mockRateLimiter{}

type mockNamespaceChecker namespace.Name

func (n mockNamespaceChecker) Exists(name namespace.Name) error {
	if name == namespace.Name(n) {
		return nil
	}
	return errors.New("doesn't exist")
}

type contextOptions struct {
	namespaceState          enumspb.NamespaceState
	namespacePassive        bool
	quota                   int
	namespaceRateLimitAllow bool
	rateLimitAllow          bool
	redirectAllow           bool
	headersBlacklist        []string
}

func newOperationContext(options contextOptions) *operationContext {
	oc := &operationContext{
		nexusContext: &nexusContext{},
	}
	oc.logger = log.NewTestLogger()
	mh := metricstest.NewCaptureHandler()
	oc.metricsHandlerForInterceptors = mh
	oc.metricsHandler = mh
	oc.clientVersionChecker = headers.NewDefaultVersionChecker()
	oc.apiName = "/temporal.api.nexusservice.v1.NexusService/DispatchNexusTask"
	oc.responseHeaders = make(map[string]string)

	oc.namespaceName = "test-namespace"
	activeClusterName := cluster.TestCurrentClusterName
	if options.namespacePassive {
		activeClusterName = cluster.TestAlternativeClusterName
	}
	oc.namespace = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id:    uuid.NewString(),
			Name:  oc.namespaceName,
			State: options.namespaceState,
		},
		&persistencespb.NamespaceConfig{
			Retention:                    timestamp.DurationFromDays(1),
			CustomSearchAttributeAliases: make(map[string]string),
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: activeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1,
	)

	checker := mockNamespaceChecker(oc.namespace.Name())
	oc.auth = authorization.NewInterceptor(
		nil,
		mockAuthorizer{},
		oc.metricsHandler,
		oc.logger,
		checker,
		nil,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(false), // exposeAuthorizerErrors
		dynamicconfig.GetBoolPropertyFn(false), // enableCrossNamespaceCommands
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false), // enablePrincipalPropagation
		dynamicconfig.GetBoolPropertyFn(false),                    // disableStreamingAuthorizer
	)
	oc.namespaceConcurrencyLimitInterceptor = interceptor.NewConcurrentRequestLimitInterceptor(
		nil,
		nil,
		oc.logger,
		func(ns string) int { return options.quota },
		func(ns string) int { return options.quota },
		map[string]int{
			oc.apiName: 1,
		},
	)
	oc.namespaceRateLimitInterceptor = interceptor.NewNamespaceRateLimitInterceptor(
		nil,
		mockRateLimiter{options.namespaceRateLimitAllow},
		map[string]struct{}{},
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		metrics.NoopMetricsHandler,
	)
	oc.rateLimitInterceptor = interceptor.NewRateLimitInterceptor(
		mockRateLimiter{options.rateLimitAllow},
		make(map[string]int),
	)

	oc.clusterMetadata = clustertest.NewMetadataForTest(
		cluster.NewTestClusterMetadataConfig(true, !options.namespacePassive),
	)
	oc.forwardingEnabledForNamespace = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(
		options.redirectAllow,
	)
	re, err := dynamicconfig.ConvertWildcardStringListToRegexp(options.headersBlacklist)
	if err != nil {
		panic(err) // nolint:forbidigo
	}
	oc.headersBlacklist = dynamicconfig.GetTypedPropertyFn(re)
	oc.redirectionInterceptor = interceptor.NewRedirection(
		nil,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		nil,
		config.DCRedirectionPolicy{Policy: interceptor.DCRedirectionPolicyAllAPIsForwarding},
		oc.logger,
		nil,
		oc.metricsHandlerForInterceptors,
		clock.NewRealTimeSource(),
		oc.clusterMetadata,
	)

	return oc
}
