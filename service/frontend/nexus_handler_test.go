package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/draft"
	"go.temporal.io/server/common/draft/testfixture"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/util"
	"google.golang.org/grpc"
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

type draftNexusRegistry struct {
	namespace.Registry
	ns *namespace.Namespace
}

func (r draftNexusRegistry) GetNamespace(namespace.Name) (*namespace.Namespace, error) {
	return r.ns, nil
}
func (r draftNexusRegistry) GetNamespaceByID(namespace.ID) (*namespace.Namespace, error) {
	return r.ns, nil
}

type draftNexusMatching struct {
	matchingservice.MatchingServiceClient
	dispatch func(*matchingservice.DispatchNexusTaskRequest) (*matchingservice.DispatchNexusTaskResponse, error)
}

func (m draftNexusMatching) DispatchNexusTask(_ context.Context, r *matchingservice.DispatchNexusTaskRequest, _ ...grpc.CallOption) (*matchingservice.DispatchNexusTaskResponse, error) {
	return m.dispatch(r)
}

func newDraftNexusHandler(t *testing.T) (*nexusHandler, context.Context) {
	t.Helper()
	oc := newOperationContext(contextOptions{namespaceState: enumspb.NAMESPACE_STATE_REGISTERED, quota: 1, namespaceRateLimitAllow: true, rateLimitAllow: true})
	h := &nexusHandler{
		logger: oc.logger, metricsHandler: oc.metricsHandler, clusterMetadata: oc.clusterMetadata, namespaceRegistry: draftNexusRegistry{ns: oc.namespace}, auth: oc.auth, telemetryInterceptor: oc.telemetryInterceptor, requestErrorHandler: oc.requestErrorHandler, redirectionInterceptor: oc.redirectionInterceptor, forwardingEnabledForNamespace: oc.forwardingEnabledForNamespace, headersBlacklist: oc.headersBlacklist,
		payloadSizeLimit: dynamicconfig.GetIntPropertyFnFilteredByNamespace(2 * 1024 * 1024), metricTagConfig: dynamicconfig.GetTypedPropertyFn(chasmnexus.NexusMetricTagConfig{}),
	}
	oc.requestStartTime = time.Now()
	oc.taskQueue = "task-queue"
	ctx := context.WithValue(context.Background(), nexusContextKey{}, oc.nexusContext)
	return h, nexus.WithHandlerContext(ctx, nexus.HandlerInfo{})
}

func draftNexusInput() *nexus.LazyValue {
	return nexus.NewLazyValue(commonnexus.PayloadSerializer, &nexus.Reader{ReadCloser: io.NopCloser(strings.NewReader("opaque input")), Header: nexus.Header{"content-type": "application/octet-stream"}})
}

func TestNexusDraftOperationSelections(t *testing.T) {
	for _, features := range [][]string{nil, {"feature-a"}, {"feature-b"}, {"feature-a", "feature-b"}} {
		for _, cancel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/cancel=%v", features, cancel), func(t *testing.T) {
				h, ctx := newDraftNexusHandler(t)
				reads := 0
				h.enabledDraftFeatureAPIs = func(ns string) []string { require.Equal(t, "test-namespace", ns); reads++; return features }
				var selection *draft.Selection
				var roots []any
				wanted := &testfixture.Message{Stable: "stable", FeatureA: "a", FeatureB: "b"}
				h.omitDraftFields = func(root any, s *draft.Selection) (bool, error) {
					if selection == nil {
						selection = s
					} else {
						require.Same(t, selection, s)
					}
					roots = append(roots, root)
					value := &testfixture.Message{Stable: "stable", FeatureA: "a", FeatureB: "b"}
					_, err := draft.Omit(value, s)
					require.NoError(t, err)
					wanted = value
					switch r := root.(type) {
					case *nexuspb.Request:
						if cancel {
							r.GetCancelOperation().Service = value.FeatureA
							r.GetCancelOperation().Operation = value.FeatureB
						} else {
							r.GetStartOperation().Service = value.FeatureA
							r.GetStartOperation().Operation = value.FeatureB
						}
					case *nexuspb.Response:
						if !cancel {
							r.GetStartOperation().GetSyncSuccess().Payload.Metadata["a"] = []byte(value.FeatureA)
							r.GetStartOperation().GetSyncSuccess().Payload.Metadata["b"] = []byte(value.FeatureB)
						}
					default:
						t.Fatalf("unexpected root %T", root)
					}
					return true, nil
				}
				h.matchingClient = draftNexusMatching{dispatch: func(r *matchingservice.DispatchNexusTaskRequest) (*matchingservice.DispatchNexusTaskResponse, error) {
					require.Equal(t, "task-queue", r.TaskQueue.Name)
					if cancel {
						require.Equal(t, wanted.FeatureA, r.Request.GetCancelOperation().Service)
						require.Equal(t, wanted.FeatureB, r.Request.GetCancelOperation().Operation)
						return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Response{Response: &nexuspb.Response{}}}, nil
					}
					require.Equal(t, wanted.FeatureA, r.Request.GetStartOperation().Service)
					require.Equal(t, wanted.FeatureB, r.Request.GetStartOperation().Operation)
					require.Equal(t, []byte("opaque input"), r.Request.GetStartOperation().Payload.Data)
					return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Response{Response: &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{StartOperation: &nexuspb.StartOperationResponse{Variant: &nexuspb.StartOperationResponse_SyncSuccess{SyncSuccess: &nexuspb.StartOperationResponse_Sync{Payload: &commonpb.Payload{Data: []byte("opaque output"), Metadata: map[string][]byte{"stable": []byte("metadata")}}}}}}}}}, nil
				}}
				if cancel {
					require.NoError(t, h.CancelOperation(ctx, "service", "operation", "token", nexus.CancelOperationOptions{Header: nexus.Header{}}))
				} else {
					result, err := h.StartOperation(ctx, "service", "operation", draftNexusInput(), nexus.StartOperationOptions{Header: nexus.Header{}})
					require.NoError(t, err)
					payload := result.(*nexus.HandlerStartOperationResultSync[any]).Value.(*commonpb.Payload)
					require.Equal(t, []byte("opaque output"), payload.Data)
					require.Equal(t, []byte("metadata"), payload.Metadata["stable"])
					require.Equal(t, []byte(wanted.FeatureA), payload.Metadata["a"])
					require.Equal(t, []byte(wanted.FeatureB), payload.Metadata["b"])
				}
				require.Len(t, roots, 2)
				require.Equal(t, 1, reads)
			})
		}
	}
}

type draftNexusOutcome struct {
	name    string
	outcome func() *matchingservice.DispatchNexusTaskResponse
	// types lists the omission roots in the order the handler visits them.
	types      []any
	startOnly  bool
	cancelOnly bool
	// success outcomes convert to a successful result instead of an error.
	success bool
}

// draftNexusOutcomes covers every worker outcome that has its own omission root.
func draftNexusOutcomes() []draftNexusOutcome {
	handlerFailure := func() *failurepb.Failure {
		return &failurepb.Failure{Message: "original", FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{Type: string(nexus.HandlerErrorTypeBadRequest)}}}
	}
	return []draftNexusOutcome{
		{name: "failure", outcome: func() *matchingservice.DispatchNexusTaskResponse {
			return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{Failure: handlerFailure()}}
		}, types: []any{&nexuspb.Request{}, &failurepb.Failure{}}},
		{name: "deprecated handler error", outcome: func() *matchingservice.DispatchNexusTaskResponse {
			//nolint:staticcheck // Exercise deprecated worker outcomes retained for backward compatibility.
			return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{HandlerError: &nexuspb.HandlerError{ErrorType: string(nexus.HandlerErrorTypeBadRequest), Failure: &nexuspb.Failure{Message: "original"}}}}
		}, types: []any{&nexuspb.Request{}, &nexuspb.Failure{}}},
		{name: "operation error", startOnly: true, outcome: func() *matchingservice.DispatchNexusTaskResponse {
			//nolint:staticcheck // Exercise deprecated worker outcomes retained for backward compatibility.
			return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Response{Response: &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{StartOperation: &nexuspb.StartOperationResponse{Variant: &nexuspb.StartOperationResponse_OperationError{OperationError: &nexuspb.UnsuccessfulOperationError{OperationState: string(nexus.OperationStateFailed), Failure: &nexuspb.Failure{Message: "original"}}}}}}}}
		}, types: []any{&nexuspb.Request{}, &nexuspb.Response{}, &nexuspb.Failure{}}},
		{name: "response failure", startOnly: true, outcome: func() *matchingservice.DispatchNexusTaskResponse {
			return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Response{Response: &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{StartOperation: &nexuspb.StartOperationResponse{Variant: &nexuspb.StartOperationResponse_Failure{Failure: handlerFailure()}}}}}}
		}, types: []any{&nexuspb.Request{}, &nexuspb.Response{}, &failurepb.Failure{}}},
		{name: "async success", startOnly: true, success: true, outcome: func() *matchingservice.DispatchNexusTaskResponse {
			return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Response{Response: &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{StartOperation: &nexuspb.StartOperationResponse{Variant: &nexuspb.StartOperationResponse_AsyncSuccess{AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: "original"}}}}}}}
		}, types: []any{&nexuspb.Request{}, &nexuspb.Response{}}},
		{name: "cancel response", cancelOnly: true, success: true, outcome: func() *matchingservice.DispatchNexusTaskResponse {
			return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Response{Response: &nexuspb.Response{}}}
		}, types: []any{&nexuspb.Request{}, &nexuspb.Response{}}},
	}
}

func (o draftNexusOutcome) modes() []bool {
	switch {
	case o.startOnly:
		return []bool{false}
	case o.cancelOnly:
		return []bool{true}
	default:
		return []bool{false, true}
	}
}

func runDraftNexusOperation(ctx context.Context, h *nexusHandler, cancel bool) (nexus.HandlerStartOperationResult[any], error) {
	if cancel {
		return nil, h.CancelOperation(ctx, "service", "operation", "token", nexus.CancelOperationOptions{Header: nexus.Header{}})
	}
	return h.StartOperation(ctx, "service", "operation", draftNexusInput(), nexus.StartOperationOptions{Header: nexus.Header{}})
}

func TestNexusDraftOperationOutcomeRoots(t *testing.T) {
	for _, tc := range draftNexusOutcomes() {
		for _, cancel := range tc.modes() {
			t.Run(fmt.Sprintf("%s/cancel=%v", tc.name, cancel), func(t *testing.T) {
				h, ctx := newDraftNexusHandler(t)
				response := tc.outcome()
				h.matchingClient = draftNexusMatching{dispatch: func(*matchingservice.DispatchNexusTaskRequest) (*matchingservice.DispatchNexusTaskResponse, error) {
					return response, nil
				}}
				var roots []any
				var selected *draft.Selection
				h.omitDraftFields = func(root any, s *draft.Selection) (bool, error) {
					if selected == nil {
						selected = s
					} else {
						require.Same(t, selected, s)
					}
					roots = append(roots, root)
					switch f := root.(type) {
					case *nexuspb.Failure:
						f.Message = "omitted"
					case *failurepb.Failure:
						f.Message = "omitted"
					case *nexuspb.Response:
						if async := f.GetStartOperation().GetAsyncSuccess(); async != nil {
							async.OperationToken = "omitted"
						}
					case *nexuspb.Request:
					default:
						t.Fatalf("unexpected root %T", root)
					}
					return true, nil
				}
				result, err := runDraftNexusOperation(ctx, h, cancel)
				require.Len(t, roots, len(tc.types))
				for i, kind := range tc.types {
					require.IsType(t, kind, roots[i])
				}
				if tc.success {
					require.NoError(t, err)
					if !cancel {
						require.Equal(t, "omitted", result.(*nexus.HandlerStartOperationResultAsync).OperationToken)
					}
					return
				}
				require.Error(t, err)
				converted, convErr := nexusrpc.DefaultFailureConverter().ErrorToFailure(err)
				require.NoError(t, convErr)
				serialized, marshalErr := json.Marshal(converted)
				require.NoError(t, marshalErr)
				require.Contains(t, string(serialized), "omitted")
			})
		}
	}
}

// An Omit error at any root, including each outcome root, becomes a
// payload-free internal handler error.
func TestNexusDraftOmissionError(t *testing.T) {
	for _, tc := range draftNexusOutcomes() {
		for _, cancel := range tc.modes() {
			for failAt := 1; failAt <= len(tc.types); failAt++ {
				t.Run(fmt.Sprintf("%s/cancel=%v/failAt=%d", tc.name, cancel, failAt), func(t *testing.T) {
					h, ctx := newDraftNexusHandler(t)
					calls := 0
					h.omitDraftFields = func(root any, _ *draft.Selection) (bool, error) {
						calls++
						require.IsType(t, tc.types[calls-1], root)
						if calls == failAt {
							return true, errors.New("private payload")
						}
						return true, nil
					}
					dispatched := false
					h.matchingClient = draftNexusMatching{dispatch: func(*matchingservice.DispatchNexusTaskRequest) (*matchingservice.DispatchNexusTaskResponse, error) {
						dispatched = true
						return tc.outcome(), nil
					}}
					result, err := runDraftNexusOperation(ctx, h, cancel)
					require.Nil(t, result)
					var he *nexus.HandlerError
					require.ErrorAs(t, err, &he)
					require.Equal(t, nexus.HandlerErrorTypeInternal, he.Type)
					require.Equal(t, "internal error", he.Message)
					require.NotContains(t, err.Error(), "private payload")
					require.Equal(t, failAt > 1, dispatched)
					require.Equal(t, failAt, calls)
				})
			}
		}
	}
}

type draftNexusTransport func(*http.Request) (*http.Response, error)

func (f draftNexusTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func draftNexusForwardingClient(t *testing.T, h *nexusHandler) *common.FrontendHTTPClient {
	t.Helper()
	passive := newOperationContext(contextOptions{namespaceState: enumspb.NAMESPACE_STATE_REGISTERED, namespacePassive: true, quota: 1, namespaceRateLimitAllow: true, rateLimitAllow: true, redirectAllow: true})
	h.namespaceRegistry = draftNexusRegistry{ns: passive.namespace}
	h.forwardingEnabledForNamespace = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	h.useForwardByEndpoint = dynamicconfig.GetBoolPropertyFn(false)
	h.forwardingClients = cluster.NewFrontendHTTPClientCache(h.clusterMetadata, nil)
	client, err := h.forwardingClients.Get(cluster.TestAlternativeClusterName)
	require.NoError(t, err)
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func TestNexusDraftOperationForwarding(t *testing.T) {
	for _, mode := range []string{"sync", "async", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			h, ctx := newDraftNexusHandler(t)
			client := draftNexusForwardingClient(t, h)
			h.omitDraftFields = func(any, *draft.Selection) (bool, error) {
				t.Fatal("forwarding must not omit public roots")
				return false, nil
			}
			h.enabledDraftFeatureAPIs = func(string) []string { t.Fatal("forwarding must not read exposure config"); return nil }
			client.Transport = draftNexusTransport(func(r *http.Request) (*http.Response, error) {
				if mode != "cancel" {
					raw, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					require.Equal(t, "opaque input", string(raw))
				}
				require.Equal(t, "true", r.Header.Get(interceptor.DCRedirectionAPIHeaderName))
				header := http.Header{"Content-Type": []string{"application/octet-stream"}, "Nexus-Link": []string{`<https://example.test/retained>; type="example.retained"`}, "Content-Stable": []string{"metadata"}}
				code, body := http.StatusOK, "opaque output"
				if mode == "cancel" {
					code = http.StatusAccepted
					body = ""
				}
				if mode == "async" {
					code = http.StatusCreated
					body = `{"token":"retained-token","state":"running"}`
					header.Set("Content-Type", "application/json")
				}
				return &http.Response{StatusCode: code, Header: header, Body: io.NopCloser(strings.NewReader(body))}, nil
			})
			if mode == "cancel" {
				require.NoError(t, h.CancelOperation(ctx, "service", "operation", "token", nexus.CancelOperationOptions{Header: nexus.Header{}}))
				return
			}
			result, err := h.StartOperation(ctx, "service", "operation", draftNexusInput(), nexus.StartOperationOptions{Header: nexus.Header{}})
			require.NoError(t, err)
			links := nexus.HandlerLinks(ctx)
			require.Len(t, links, 1)
			require.Equal(t, "https://example.test/retained", links[0].URL.String())
			require.Equal(t, "example.retained", links[0].Type)
			if mode == "sync" {
				reader := result.(*nexus.HandlerStartOperationResultSync[any]).Value.(*nexus.Reader)
				data, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.NoError(t, reader.Close())
				require.Equal(t, "opaque output", string(data))
				require.Equal(t, "metadata", reader.Header.Get("stable"))
			} else {
				require.Equal(t, "retained-token", result.(*nexus.HandlerStartOperationResultAsync).OperationToken)
			}
		})
	}
}
