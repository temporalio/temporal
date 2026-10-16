package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
)

func TestNexusTaskDeliveryFrontendLogs(t *testing.T) {
	t.Parallel()
	taskToken, err := (&tokenspb.NexusTask{NamespaceId: "namespace-id", TaskQueue: "queue", TaskId: "task-id"}).Marshal()
	require.NoError(t, err)
	for _, tc := range []struct {
		operation string
		invoke    func(context.Context, *WorkflowHandler) error
	}{
		{"PollNexusTaskQueue", func(ctx context.Context, h *WorkflowHandler) error {
			_, err := h.PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{})
			return err
		}},
		{"RespondNexusTaskCompleted", func(ctx context.Context, h *WorkflowHandler) error {
			_, err := h.RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{TaskToken: taskToken})
			return err
		}},
		{"RespondNexusTaskFailed", func(ctx context.Context, h *WorkflowHandler) error {
			_, err := h.RespondNexusTaskFailed(ctx, &workflowservice.RespondNexusTaskFailedRequest{
				TaskToken: taskToken,
				Failure: &failurepb.Failure{FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
					NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{},
				}},
			})
			return err
		}},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			t.Parallel()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			logger.Expect(testlogger.Error, "Panic is captured")
			capture := logger.StartCapture()
			defer logger.StopCapture(capture)
			handler := &WorkflowHandler{logger: log.With(logger, tag.ComponentRPCHandler), throttledLogger: logger}
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			require.Error(t, tc.invoke(ctx, handler))
			capture.RequireContains(t, testlogger.CapturedLogPattern{
				Level: testlogger.Error, Message: "Panic is captured",
				Tags: map[string]any{"nexus-stage": "handler-task-delivery", "operation": tc.operation, "component": "rpc-handler"},
			})
		})
	}
}

func TestNexusTaskDeliveryFrontendLongPollLog(t *testing.T) {
	t.Parallel()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	logger.Expect(testlogger.Error, "Context timeout not set for long poll API.")
	capture := logger.StartCapture()
	defer logger.StopCapture(capture)
	handler := &WorkflowHandler{logger: logger, throttledLogger: logger}
	_, err := handler.PollNexusTaskQueue(context.Background(), &workflowservice.PollNexusTaskQueueRequest{})
	require.ErrorIs(t, err, common.ErrContextTimeoutNotSet)
	capture.RequireContains(t, testlogger.CapturedLogPattern{
		Level: testlogger.Error, Message: "Context timeout not set for long poll API.",
		Tags: map[string]any{"nexus-stage": "handler-task-delivery", "operation": "PollNexusTaskQueue"},
	})
}
