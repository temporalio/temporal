package frontend

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/frontend/configs"
	"go.uber.org/mock/gomock"
)

func TestNexusStagePollCancellationFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		taskType enumspb.TaskQueueType
		stage    string
	}{
		{enumspb.TASK_QUEUE_TYPE_NEXUS, "handler-task-delivery"},
		{enumspb.TASK_QUEUE_TYPE_WORKFLOW, ""},
	} {
		t.Run(tc.taskType.String(), func(t *testing.T) {
			t.Parallel()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			logger.Expect(testlogger.Warn, "Failed to cancel outstanding poller.")
			capture := logger.StartCapture()
			defer logger.StopCapture(capture)
			client := matchingservicemock.NewMockMatchingServiceClient(gomock.NewController(t))
			client.EXPECT().CancelOutstandingPoll(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewInternal("broken"))
			handler := &WorkflowHandler{logger: logger, matchingClient: client}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.True(t, handler.cancelOutstandingPoll(ctx, namespace.ID("namespace-id"), tc.taskType, &taskqueuepb.TaskQueue{Name: "queue"}, "poller"))
			records := capture.Snapshot()
			require.Len(t, records, 1)
			tags := map[string]any{}
			for _, tag := range records[0].Tags {
				tags[tag.Key()] = tag.Value()
			}
			if tc.stage == "" {
				require.NotContains(t, tags, "nexus-stage")
			} else {
				require.Equal(t, tc.stage, tags["nexus-stage"])
			}
		})
	}
}

func TestNexusStageHandlerHTTPServiceFailures(t *testing.T) {
	t.Parallel()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	logger.Expect(testlogger.Error, "service failures")
	capture := logger.StartCapture()
	defer logger.StopCapture(capture)
	oc := newOperationContext(contextOptions{namespaceState: enumspb.NAMESPACE_STATE_REGISTERED, quota: 1, namespaceRateLimitAllow: true, rateLimitAllow: true})
	handler := interceptor.NewRequestErrorHandler(logger, dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false))
	oc.apiName = configs.DispatchNexusTaskByEndpointAPIName
	oc.requestErrorHandler = handler
	require.NoError(t, oc.interceptRequest(t.Context(), &matchingservice.DispatchNexusTaskRequest{}, nexus.Header{}))
	for _, cleanup := range oc.cleanupFunctions {
		cleanup(map[string]string{commonnexus.FailureSourceHeaderName: "frontend"}, serviceerror.NewInternal("broken"))
	}
	capture.RequireContains(t, testlogger.CapturedLogPattern{Level: testlogger.Error, Message: "service failures", Tags: map[string]any{"nexus-stage": "handler-inbound"}})
}

func TestNexusStageCallerHTTPServiceFailures(t *testing.T) {
	t.Parallel()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	logger.Expect(testlogger.Error, "service failures")
	capture := logger.StartCapture()
	defer logger.StopCapture(capture)
	oc := newOperationContext(contextOptions{namespaceState: enumspb.NAMESPACE_STATE_REGISTERED, quota: 1, namespaceRateLimitAllow: true, rateLimitAllow: true})
	handler := interceptor.NewRequestErrorHandler(logger, dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false))
	c := &requestContext{
		nexusCompletionHandler: &nexusCompletionHandler{
			AuthInterceptor: oc.auth, ClusterMetadata: oc.clusterMetadata,
			NamespaceValidationInterceptor:       oc.namespaceValidationInterceptor,
			NamespaceConcurrencyLimitInterceptor: oc.namespaceConcurrencyLimitInterceptor,
			NamespaceRateLimitInterceptor:        oc.namespaceRateLimitInterceptor, RateLimitInterceptor: oc.rateLimitInterceptor,
			RequestErrorHandler: handler, clientVersionChecker: oc.clientVersionChecker,
		},
		logger: logger, namespace: oc.namespace, metricsHandler: metrics.NoopMetricsHandler, metricsHandlerForInterceptors: metrics.NoopMetricsHandler,
	}
	require.NoError(t, c.interceptRequest(t.Context(), &nexusrpc.CompletionRequest{HTTPRequest: httptest.NewRequest("POST", "/", nil)}))
	for _, cleanup := range c.cleanupFunctions {
		cleanup(serviceerror.NewInternal("broken"))
	}
	capture.RequireContains(t, testlogger.CapturedLogPattern{Level: testlogger.Error, Message: "service failures", Tags: map[string]any{"nexus-stage": "caller-inbound"}})
}
