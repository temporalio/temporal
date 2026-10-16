package interceptor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
)

func TestNexusTaskDeliveryRPCErrorLogs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		method string
		nexus  bool
	}{
		{api.WorkflowServicePrefix + "PollNexusTaskQueue", true},
		{api.WorkflowServicePrefix + "RespondNexusTaskCompleted", true},
		{api.WorkflowServicePrefix + "RespondNexusTaskFailed", true},
		{api.MatchingServicePrefix + "DispatchNexusTask", true},
		{api.MatchingServicePrefix + "PollNexusTaskQueue", true},
		{api.MatchingServicePrefix + "RespondNexusTaskCompleted", true},
		{api.MatchingServicePrefix + "RespondNexusTaskFailed", true},
		{api.WorkflowServicePrefix + "PollWorkflowTaskQueue", false},
		{api.MatchingServicePrefix + "PollActivityTaskQueue", false},
		{api.WorkflowServicePrefix + "StartNexusOperationExecution", false},
		{api.MatchingServicePrefix + "CreateNexusEndpoint", false},
	} {
		t.Run(tc.method, func(t *testing.T) {
			t.Parallel()
			for _, masked := range []bool{false, true} {
				message := "service failures"
				if masked {
					message = "masked service failures"
				}
				t.Run(message, func(t *testing.T) {
					t.Parallel()
					logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
					logger.Expect(testlogger.Error, message)
					capture := logger.StartCapture()
					defer logger.StopCapture(capture)
					scoped := log.With(logger, tag.ComponentRPCHandler)
					err := serviceerror.NewInternal("task delivery failed")
					if masked {
						handler := NewMaskInternalErrorDetailsInterceptor(dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true), nil, scoped)
						require.Error(t, handler.maskUnknownOrInternalErrors(nil, tc.method, err))
					} else {
						handler := NewRequestErrorHandler(scoped, dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false))
						_, tags := CreateUnaryMetricsHandlerLogTags(metrics.NoopMetricsHandler, nil, tc.method, api.MethodName(tc.method), "")
						handler.HandleError(nil, tc.method, metrics.NoopMetricsHandler, tags, err, "")
					}
					want := map[string]any{"component": "rpc-handler", "operation": api.MethodName(tc.method)}
					if tc.nexus {
						want["nexus-stage"] = "handler-task-delivery"
					}
					capture.RequireContains(t, testlogger.CapturedLogPattern{Level: testlogger.Error, Message: message, Tags: want})
					if !tc.nexus {
						for _, record := range capture.Snapshot() {
							for _, logTag := range record.Tags {
								require.NotEqual(t, "nexus-stage", logTag.Key())
							}
						}
					}
				})
			}
		})
	}
}
