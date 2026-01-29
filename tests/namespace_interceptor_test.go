package tests

import (
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestNamespaceInterceptor(t *testing.T) {
	t.Run("ServerRejectsInvalidRequests", func(t *testing.T) {
		s := testcore.NewEnv(t)
		tv := s.Tv()

		// Start workflow
		startReq := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           tv.Any().String(),
			Namespace:           s.Namespace().String(),
			WorkflowId:          tv.WorkflowID(),
			WorkflowType:        tv.WorkflowType(),
			TaskQueue:           tv.TaskQueue(),
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(20 * time.Second),
			WorkflowTaskTimeout: durationpb.New(3 * time.Second),
			Identity:            tv.WorkerIdentity(),
		}
		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startReq)
		s.NoError(err)

		// Poll for workflow task
		pollReq := &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: tv.TaskQueue(),
			Identity:  tv.WorkerIdentity(),
		}
		pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), pollReq)
		s.NoError(err)
		taskToken := pollResp.GetTaskToken()

		// Customer tries to use another namespace - should fail
		invalidResp := &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: namespace.Name("another-namespace").String(),
			TaskToken: taskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
						CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
							Result: payloads.EncodeString("efg"),
						},
					},
				}},
			StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
				WorkerTaskQueue:        tv.StickyTaskQueue(),
				ScheduleToStartTimeout: durationpb.New(5 * time.Second),
			},
			ReturnNewWorkflowTask:      true,
			ForceCreateNewWorkflowTask: false,
		}
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), invalidResp)
		s.Error(err, "Invalid request was processed successfully, make sure NamespaceRequestValidator interceptor is used")

		// Valid request with correct namespace - should succeed
		validResp := &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: taskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
						CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
							Result: payloads.EncodeString("efg"),
						},
					},
				}},
			StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
				WorkerTaskQueue:        tv.StickyTaskQueue(),
				ScheduleToStartTimeout: durationpb.New(5 * time.Second),
			},
			ReturnNewWorkflowTask:      true,
			ForceCreateNewWorkflowTask: false,
		}
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), validResp)
		s.NoError(err, "Valid request was rejected")
	})
}
