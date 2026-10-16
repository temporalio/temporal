package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/grpc"
)

func TestNexusStageRPCErrorLogs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		method string
		stage  string
	}{
		{api.WorkflowServicePrefix + "PollNexusTaskQueue", "handler-task-delivery"},
		{api.WorkflowServicePrefix + "RespondNexusTaskCompleted", "handler-task-delivery"},
		{api.WorkflowServicePrefix + "RespondNexusTaskFailed", "handler-task-delivery"},
		{api.MatchingServicePrefix + "DispatchNexusTask", "handler-task-delivery"},
		{api.MatchingServicePrefix + "PollNexusTaskQueue", "handler-task-delivery"},
		{api.MatchingServicePrefix + "RespondNexusTaskCompleted", "handler-task-delivery"},
		{api.MatchingServicePrefix + "RespondNexusTaskFailed", "handler-task-delivery"},
		{api.WorkflowServicePrefix + "PollWorkflowTaskQueue", ""},
		{api.MatchingServicePrefix + "PollActivityTaskQueue", ""},
		{api.WorkflowServicePrefix + "StartNexusOperationExecution", "caller-outbound"},
		{api.MatchingServicePrefix + "CreateNexusEndpoint", ""},
		{api.WorkflowServicePrefix + "DescribeNexusOperationExecution", "caller-outbound"},
		{api.WorkflowServicePrefix + "PollNexusOperationExecution", "caller-outbound"},
		{api.WorkflowServicePrefix + "RequestCancelNexusOperationExecution", "caller-outbound"},
		{api.WorkflowServicePrefix + "TerminateNexusOperationExecution", "caller-outbound"},
		{api.WorkflowServicePrefix + "DeleteNexusOperationExecution", "caller-outbound"},
		{api.WorkflowServicePrefix + "ListNexusOperationExecutions", "caller-outbound"},
		{api.WorkflowServicePrefix + "CountNexusOperationExecutions", "caller-outbound"},
		{api.HistoryServicePrefix + "CompleteNexusOperation", "caller-inbound"},
		{api.HistoryServicePrefix + "CompleteNexusOperationChasm", "caller-inbound"},
		{api.HistoryServicePrefix + "StartNexusOperation", "handler-inbound"},
		{api.HistoryServicePrefix + "CancelNexusOperation", "handler-inbound"},
		{"/temporal.server.chasm.lib.nexusoperation.proto.v1.NexusOperationService/StartNexusOperation", "caller-outbound"},
		{api.NexusServicePrefix + "DispatchByEndpoint", "handler-inbound"},
		{api.NexusServicePrefix + "DispatchByNamespaceAndTaskQueue", "handler-inbound"},
		{api.NexusServicePrefix + "CompleteNexusOperation", "caller-inbound"},
		{"/unrelated.Service/StartNexusOperationExecution", ""},
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
					if tc.stage != "" {
						want["nexus-stage"] = tc.stage
					}
					capture.RequireContains(t, testlogger.CapturedLogPattern{Level: testlogger.Error, Message: message, Tags: want})
					if tc.stage == "" {
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

func TestNexusStagePanicLogs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ method, stage string }{
		{api.WorkflowServicePrefix + "PollNexusTaskQueue", "handler-task-delivery"},
		{api.WorkflowServicePrefix + "StartNexusOperationExecution", "caller-outbound"},
		{api.HistoryServicePrefix + "CompleteNexusOperation", "caller-inbound"},
	} {
		t.Run(tc.method, func(t *testing.T) {
			t.Parallel()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			logger.Expect(testlogger.Error, "Panic is captured")
			capture := logger.StartCapture()
			defer logger.StopCapture(capture)
			i := NewServiceErrorInterceptor(dynamicconfig.GetIntPropertyFn(1024), metrics.NoopMetricsHandler, logger)
			_, err := i.Intercept(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: tc.method}, func(context.Context, any) (any, error) { panic("broken") })
			require.Error(t, err)
			capture.RequireContains(t, testlogger.CapturedLogPattern{Level: testlogger.Error, Message: "Panic is captured", Tags: map[string]any{"nexus-stage": tc.stage}})
		})
	}
}

func TestNexusStageSharedPollCancellationLogs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		request any
		stage   string
	}{
		{"nexus", &matchingservice.CancelOutstandingPollRequest{TaskQueueType: enumspb.TASK_QUEUE_TYPE_NEXUS}, "handler-task-delivery"},
		{"workflow", &matchingservice.CancelOutstandingPollRequest{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}, ""},
		{"activity", &matchingservice.CancelOutstandingPollRequest{TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY}, ""},
		{"missing request", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			logger.Expect(testlogger.Error, "service failures")
			capture := logger.StartCapture()
			defer logger.StopCapture(capture)
			handler := NewRequestErrorHandler(logger, dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false))
			handler.HandleError(tc.request, api.MatchingServicePrefix+"CancelOutstandingPoll", metrics.NoopMetricsHandler, nil, serviceerror.NewInternal("broken"), "")
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

func TestNexusStageForwardingPanicLogs(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"redirection", "handover"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			logger.Expect(testlogger.Error, "Panic is captured")
			capture := logger.StartCapture()
			defer logger.StopCapture(capture)
			intercept := (&Redirection{logger: logger}).Intercept
			if name == "handover" {
				intercept = (&NamespaceHandoverInterceptor{logger: logger}).Intercept
			}
			_, err := intercept(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: api.MatchingServicePrefix + "PollNexusTaskQueue"}, func(context.Context, any) (any, error) { panic("broken") })
			require.Error(t, err)
			capture.RequireContains(t, testlogger.CapturedLogPattern{Level: testlogger.Error, Message: "Panic is captured", Tags: map[string]any{"nexus-stage": "handler-task-delivery"}})
		})
	}
}
