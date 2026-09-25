package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/temporalnexus"
	"go.temporal.io/server/api/matchingservice/v1"
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
	"go.temporal.io/server/common/util"
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

func TestNexusInterceptRequest_InvalidNamespaceState_ResultsInBadRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_DELETED,
		quota:                   1,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          true,
	})
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, nexus.Header{})
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerError.Type)
	require.Equal(t, "bad request", handlerError.Message)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Len(t, snap["test"], 1)
	require.Equal(t, map[string]string{"outcome": "invalid_namespace_state"}, snap["test"][0].Tags)
}

func TestNexusInterceptRequest_NamespaceConcurrencyLimited_ResultsInResourceExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		quota:                   0,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          true,
	})
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, nexus.Header{})
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeResourceExhausted, handlerError.Type)
	require.Equal(t, "resource exhausted", handlerError.Message)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Len(t, snap["test"], 1)
	require.Equal(t, map[string]string{"outcome": "namespace_concurrency_limited"}, snap["test"][0].Tags)
}

func TestNexusInterceptRequest_NamespaceRateLimited_ResultsInResourceExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		quota:                   1,
		namespaceRateLimitAllow: false,
		rateLimitAllow:          true,
	})
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, nexus.Header{})
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeResourceExhausted, handlerError.Type)
	require.Equal(t, "namespace rate limit exceeded", handlerError.Message)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Len(t, snap["test"], 1)
	require.Equal(t, map[string]string{"outcome": "namespace_rate_limited"}, snap["test"][0].Tags)
}

func TestNexusInterceptRequest_GlobalRateLimited_ResultsInResourceExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		quota:                   1,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          false,
	})
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, nexus.Header{})
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeResourceExhausted, handlerError.Type)
	require.Equal(t, "service rate limit exceeded", handlerError.Message)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Len(t, snap["test"], 1)
	require.Equal(t, map[string]string{"outcome": "global_rate_limited"}, snap["test"][0].Tags)
}

func TestNexusInterceptRequest_ForwardingDisabled_ResultsInUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		namespacePassive:        true,
		quota:                   1,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          true,
		redirectAllow:           false,
	})
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, nexus.Header{})
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeUnavailable, handlerError.Type)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Len(t, snap["test"], 1)
	require.Equal(t, map[string]string{"outcome": "namespace_inactive_forwarding_disabled"}, snap["test"][0].Tags)
}

func TestNexusInterceptRequest_ForwardingEnabled_ResultsInNotActiveError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		namespacePassive:        true,
		quota:                   1,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          true,
		redirectAllow:           true,
	})
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, nexus.Header{})
	var notActiveErr *serviceerror.NamespaceNotActive
	require.ErrorAs(t, err, &notActiveErr)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Len(t, snap["test"], 1)
	require.Equal(t, map[string]string{"outcome": "request_forwarded"}, snap["test"][0].Tags)
}

func TestForwardStartOperation_PreservesResponseLinks(t *testing.T) {
	handlerLink := temporalnexus.ConvertLinkWorkflowEventToNexusLink(&commonpb.Link_WorkflowEvent{
		Namespace:  "handler-namespace",
		WorkflowId: "handler-workflow-id",
		RunId:      "handler-run-id",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	})
	responseBody, err := json.Marshal(nexus.OperationInfo{
		Token: "operation-token",
		State: nexus.OperationStateRunning,
	})
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Add("Nexus-Link", fmt.Sprintf("<%s>; type=%q", handlerLink.URL.String(), handlerLink.Type))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(responseBody)
	}))
	t.Cleanup(server.Close)

	clusterConfig := cluster.NewTestClusterMetadataConfig(true, true)
	clusterConfig.ClusterInformation = maps.Clone(clusterConfig.ClusterInformation)
	activeCluster := clusterConfig.ClusterInformation[cluster.TestAlternativeClusterName]
	activeCluster.HTTPAddress = strings.TrimPrefix(server.URL, "http://")
	clusterConfig.ClusterInformation[cluster.TestAlternativeClusterName] = activeCluster
	clusterMetadata := clustertest.NewMetadataForTest(clusterConfig)
	h := &nexusHandler{
		clusterMetadata:      clusterMetadata,
		forwardingClients:    cluster.NewFrontendHTTPClientCache(clusterMetadata, nil),
		useForwardByEndpoint: dynamicconfig.GetBoolPropertyFn(false),
	}
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		namespacePassive:        true,
		quota:                   1,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          true,
		redirectAllow:           true,
	})
	oc.clusterMetadata = clusterMetadata

	content, err := nexus.DefaultSerializer().Serialize("input")
	require.NoError(t, err)
	input := nexus.NewLazyValue(nexus.DefaultSerializer(), &nexus.Reader{
		ReadCloser: io.NopCloser(strings.NewReader(string(content.Data))),
		Header:     content.Header,
	})
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	ctx, span := tracerProvider.Tracer("test").Start(t.Context(), "test")
	ctx = nexus.WithHandlerContext(ctx, nexus.HandlerInfo{})
	result, err := h.forwardStartOperation(ctx, "service", "operation", input, nexus.StartOperationOptions{
		Header:    nexus.Header{},
		RequestID: "request-id",
	}, oc)
	span.End()

	require.NoError(t, err)
	asyncResult, ok := result.(*nexus.HandlerStartOperationResultAsync)
	require.True(t, ok)
	require.Equal(t, "operation-token", asyncResult.OperationToken)
	forwardedLinks := nexus.HandlerLinks(ctx)
	require.Len(t, forwardedLinks, 1)
	require.Equal(t, handlerLink.URL.String(), forwardedLinks[0].URL.String())
	require.Equal(t, handlerLink.Type, forwardedLinks[0].Type)
	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Len(t, spans[0].Links(), 1)
}

func TestAnnotateServerSpanLinks_IgnoresUnrelatedWorkflowEvent(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	ctx, span := tracerProvider.Tracer("test").Start(t.Context(), "test")

	link := temporalnexus.ConvertLinkWorkflowEventToNexusLink(&commonpb.Link_WorkflowEvent{
		Namespace:  "namespace",
		WorkflowId: "workflow-id",
		RunId:      "run-id",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			},
		},
	})
	(&operationContext{}).annotateServerSpanLinks(ctx, "request-id", []nexus.Link{link})
	span.End()

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Empty(t, spans[0].Links())
	require.Empty(t, spans[0].Attributes())
}

func TestAnnotateServerSpanLinks_IgnoresUnrelatedRequestIDReference(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	ctx, span := tracerProvider.Tracer("test").Start(t.Context(), "test")

	link := temporalnexus.ConvertLinkWorkflowEventToNexusLink(&commonpb.Link_WorkflowEvent{
		Namespace:  "namespace",
		WorkflowId: "workflow-id",
		RunId:      "run-id",
		Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
			RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
				RequestId: "other-request-id",
			},
		},
	})
	(&operationContext{}).annotateServerSpanLinks(ctx, "request-id", []nexus.Link{link})
	span.End()

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Empty(t, spans[0].Links())
	require.Empty(t, spans[0].Attributes())
}

func TestNexusInterceptRequest_InvalidSDKVersion_ResultsInBadRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		namespacePassive:        false,
		quota:                   1,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          true,
		redirectAllow:           true,
	})
	header := nexus.Header{headerUserAgent: "Nexus-go-sdk/v99.0.0"}
	ctx = oc.augmentContext(ctx, header)
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, header)
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerError.Type)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Len(t, snap["test"], 1)
	require.Equal(t, map[string]string{"outcome": "unsupported_client"}, snap["test"][0].Tags)
}

func TestNexusInterceptRequest_HeadersSanitization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		namespacePassive:        false,
		quota:                   1,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          true,
		headersBlacklist:        []string{"delete-*", "remove-*"},
	})
	initialHeader := nexus.Header{
		"ok-header":  "ok",
		"delete-foo": "foo",
		"delete-bar": "bar",
		"remove-zzz": "zzz",
	}
	header := util.CloneMapNonNil(initialHeader)
	ctx = oc.augmentContext(ctx, header)
	request := &matchingservice.DispatchNexusTaskRequest{
		Request: &nexuspb.Request{Header: header},
	}
	err = oc.interceptRequest(ctx, request, header)
	require.NoError(t, err)
	require.Equal(t, initialHeader, header)
	require.Equal(t, map[string]string{"ok-header": "ok"}, request.Request.Header)
}
