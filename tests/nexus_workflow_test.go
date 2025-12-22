package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics/metricstest"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/frontend/configs"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type NexusWorkflowTestSuite struct {
	NexusTestBaseSuite
}

func TestNexusWorkflowTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(NexusWorkflowTestSuite))
}

func (s *NexusWorkflowTestSuite) TestNexusOperationCancelation() {
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	firstCancelSeen := false
	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			if service != "service" {
				return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, `expected service to equal "service"`)
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test"}, nil
		},
		OnCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
			if !firstCancelSeen {
				// Fail cancel request once to test NexusOperationCancelRequestFailed event is recorded and request is retried.
				firstCancelSeen = true
				return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "intentional non-retyrable cancel error for test")
			}
			return nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: time.Second,
	}, "workflow")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		require.NoError(t, err)
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
					Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
						ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
							Endpoint:  endpointName,
							Service:   "service",
							Operation: "operation",
							Input:     s.mustToPayload("input"),
						},
					},
				},
			},
		})
		require.NoError(t, err)
	}, time.Second*20, time.Millisecond*200)

	// Poll and wait for the "started" event to be recorded.
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)

	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Greater(startedEventIdx, 0)

	// Get the scheduleEventId to issue the cancel command.
	scheduledEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationScheduledEventAttributes() != nil
	})
	s.Greater(scheduledEventIdx, 0)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
				Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
					RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
						ScheduledEventId: pollResp.History.Events[scheduledEventIdx].EventId,
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and verify first cancel request failed and allowed workflow to make progress.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	cancelFailedIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCancelRequestFailedEventAttributes() != nil
	})
	s.Greater(cancelFailedIdx, 0)

	// Start new operation to successfully cancel.
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  endpointName,
						Service:   "service",
						Operation: "operation",
						Input:     s.mustToPayload("input"),
					},
				},
			},
		},
	})
	s.NoError(err)
	// Poll and wait for the "started" event to be recorded.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	// Get the second scheduleEventId to issue the cancel command.
	var secondScheduledEventID int64
	for _, event := range pollResp.History.Events[cancelFailedIdx:] {
		if event.GetNexusOperationScheduledEventAttributes() != nil {
			secondScheduledEventID = event.EventId
			break
		}
	}
	s.Greater(secondScheduledEventID, int64(0))
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
				Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
					RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
						ScheduledEventId: secondScheduledEventID,
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and wait for the cancelation request to go through.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		require.NoError(t, err)
		require.Equal(t, 2, len(desc.PendingNexusOperations))
		op1 := desc.PendingNexusOperations[0]
		require.Equal(t, pollResp.History.Events[scheduledEventIdx].EventId, op1.ScheduledEventId)
		require.Equal(t, endpointName, op1.Endpoint)
		require.Equal(t, "service", op1.Service)
		require.Equal(t, "operation", op1.Operation)
		require.Equal(t, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED, op1.State)
		require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED, op1.CancellationInfo.State)
		op2 := desc.PendingNexusOperations[1]
		require.Equal(t, secondScheduledEventID, op2.ScheduledEventId)
		require.Equal(t, endpointName, op2.Endpoint)
		require.Equal(t, "service", op2.Service)
		require.Equal(t, "operation", op2.Operation)
		require.Equal(t, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED, op2.State)
		require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SUCCEEDED, op2.CancellationInfo.State)
	}, time.Second*5, time.Millisecond*30)

	err = s.SdkClient().TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "test")
	s.NoError(err)

	hist := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	})
	s.ContainsHistoryEvents(`NexusOperationCancelRequestFailed`, hist)
	s.ContainsHistoryEvents(`NexusOperationCancelRequestCompleted`, hist)
}

func (s *NexusWorkflowTestSuite) TestNexusOperationSyncCompletion() {
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	handlerLink := &commonpb.Link_WorkflowEvent{
		Namespace:  "handler-ns",
		WorkflowId: "handler-wf-id",
		RunId:      "handler-run-id",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	}
	handlerNexusLink := nexusoperations.ConvertLinkWorkflowEventToNexusLink(handlerLink)

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			nexus.AddHandlerLinks(ctx, handlerNexusLink)
			return &nexus.HandlerStartOperationResultSync[any]{Value: "result"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		require.NoError(t, err)
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
					Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
						ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
							Endpoint:  endpointName,
							Service:   "service",
							Operation: "operation",
							Input:     s.mustToPayload("input"),
						},
					},
				},
			},
		})
		require.NoError(t, err)
	}, time.Second*20, time.Millisecond*200)

	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Greater(completedEventIdx, 0)
	s.Len(pollResp.History.Events[completedEventIdx].GetLinks(), 1)
	protorequire.ProtoEqual(s.T(), handlerLink, pollResp.History.Events[completedEventIdx].GetLinks()[0].GetWorkflowEvent())

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								pollResp.History.Events[completedEventIdx].GetNexusOperationCompletedEventAttributes().Result,
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)

	// Use this test case to verify that the state machine is actually deleted, the workflowservice
	// DescribeWorkflowExecution API filters out operations in terminal state in case they complete in a server version
	// without state machine deletion enabled, hence the use of the adminservice API here.
	desc, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
		},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	s.Len(desc.DatabaseMutableState.GetExecutionInfo().SubStateMachinesByType, 0)
}

func (s *NexusWorkflowTestSuite) TestNexusOperationSyncCompletion_LargePayload() {
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			// Use -10 to avoid hitting MaxNexusAPIRequestBodyBytes. Actual payload will still exceed limit because of
			// additional Content headers. See common/rpc/grpc.go:66
			return &nexus.HandlerStartOperationResultSync[any]{Value: strings.Repeat("a", (2*1024*1024)-10)}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		require.NoError(t, err)
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
					Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
						ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
							Endpoint:  endpointName,
							Service:   "service",
							Operation: "operation",
							Input:     s.mustToPayload("input"),
						},
					},
				},
			},
		})
		require.NoError(t, err)
	}, time.Second*20, time.Millisecond*200)

	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	failedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationFailedEventAttributes() != nil
	})
	s.Greater(failedEventIdx, 0)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								s.mustToPayload(pollResp.History.Events[failedEventIdx].GetNexusOperationFailedEventAttributes().Failure.Cause.Message),
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)

	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("http: response body too large", result)
}

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletion() {
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	testClusterInfo, err := s.FrontendClient().GetClusterInfo(ctx, &workflowservice.GetClusterInfoRequest{})
	s.NoError(err)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	var callbackToken, publicCallbackURL string

	handlerLink := &commonpb.Link_WorkflowEvent{
		Namespace:  "handler-ns",
		WorkflowId: "handler-wf-id",
		RunId:      "handler-run-id",
		Reference: &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			},
		},
	}
	handlerNexusLink := nexusoperations.ConvertLinkWorkflowEventToNexusLink(handlerLink)

	h := nexustest.Handler{
		OnStartOperation: func(
			ctx context.Context,
			service, operation string,
			input *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			if options.CallbackURL == commonnexus.PathCompletionCallbackNoIdentifier {
				s.Equal(testClusterInfo.GetClusterId(), options.CallbackHeader.Get("source"))
			}

			s.Len(options.Links, 1)
			var links []*commonpb.Link
			for _, nexusLink := range options.Links {
				link, err := nexusoperations.ConvertNexusLinkToLinkWorkflowEvent(nexusLink)
				s.NoError(err)
				links = append(links, &commonpb.Link{
					Variant: &commonpb.Link_WorkflowEvent_{
						WorkflowEvent: link,
					},
				})
			}
			s.NotNil(links[0].GetWorkflowEvent())
			protorequire.ProtoEqual(s.T(), &commonpb.Link_WorkflowEvent{
				Namespace:  s.Namespace().String(),
				WorkflowId: run.GetID(),
				RunId:      run.GetRunID(),
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						// Event history:
						// 1 WORKFLOW_EXECUTION_STARTED
						// 2 WORKFLOW_TASK_SCHEDULED
						// 3 WORKFLOW_TASK_STARTED
						// 4 WORKFLOW_TASK_COMPLETED
						// 5 NEXUS_OPERATION_SCHEDULED
						EventId:   5,
						EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
					},
				},
			}, links[0].GetWorkflowEvent())

			callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			publicCallbackURL = options.CallbackURL
			nexus.AddHandlerLinks(ctx, handlerNexusLink)
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: "test",
			}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err = s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  endpointName,
						Service:   "service",
						Operation: "operation",
						Input:     s.mustToPayload("input"),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and verify that the "started" event was recorded.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	// Remember the workflow task completed event ID (next after the last WFT started), we'll use it to test reset
	// below.
	wftCompletedEventID := int64(len(pollResp.History.Events))

	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Greater(startedEventIdx, 0)

	s.Len(pollResp.History.Events[startedEventIdx].Links, 1)
	l := pollResp.History.Events[startedEventIdx].Links[0].GetWorkflowEvent()
	protorequire.ProtoEqual(s.T(), handlerLink, l)

	// Completion request fails if the result payload is too large.
	largeCompletion, err := nexusrpc.NewOperationCompletionSuccessful(
		// Use -10 to avoid hitting MaxNexusAPIRequestBodyBytes. Actual payload will still exceed limit because of
		// additional Content headers. See common/rpc/grpc.go:66
		s.mustToPayload(strings.Repeat("a", (2*1024*1024)-10)),
		nexusrpc.OperationCompletionSuccessfulOptions{Serializer: commonnexus.PayloadSerializer},
	)
	s.NoError(err)
	res, snap, _ := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, largeCompletion, callbackToken)
	s.Equal(http.StatusBadRequest, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "outcome": "error_bad_request"})

	completion, err := nexusrpc.NewOperationCompletionSuccessful(s.mustToPayload(nil), nexusrpc.OperationCompletionSuccessfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)

	invalidNamespace := testcore.RandomizeStr("ns")
	_, err = s.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        invalidNamespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(time.Hour * 24),
	})
	s.NoError(err)

	// Send an invalid completion request and verify that we get an error that the namespace in the URL doesn't match the namespace in the token.
	invalidCallbackURL := "http://" + s.HttpAPIAddress() + "/" + commonnexus.RouteCompletionCallback.Path(invalidNamespace)

	res, _, body := s.sendNexusCompletionRequest(ctx, s.T(), invalidCallbackURL, completion, callbackToken)

	// Verify we get the correct error response
	s.Equal(http.StatusBadRequest, res.StatusCode)
	s.Contains(body, "invalid callback token", "Response should indicate namespace mismatch")

	// Manipulate the token to verify we get the expected errors in the API.
	gen := &commonnexus.CallbackTokenGenerator{}
	decodedToken, err := commonnexus.DecodeCallbackToken(callbackToken)
	s.NoError(err)
	completionToken, err := gen.DecodeCompletion(decodedToken)
	s.NoError(err)

	// Request fails if the workflow is not found.
	workflowNotFoundToken := common.CloneProto(completionToken)
	workflowNotFoundToken.WorkflowId = "not-found"
	callbackToken, err = gen.Tokenize(workflowNotFoundToken)
	s.NoError(err)

	res, snap, _ = s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, callbackToken)
	s.Equal(http.StatusNotFound, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "outcome": "error_not_found"})

	// Request fails if the state machine reference is stale.
	staleToken := common.CloneProto(completionToken)
	staleToken.Ref.MachineInitialVersionedTransition.NamespaceFailoverVersion++
	callbackToken, err = gen.Tokenize(staleToken)
	s.NoError(err)

	res, snap, _ = s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, callbackToken)
	s.Equal(http.StatusNotFound, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "outcome": "error_not_found"})

	// Send a valid - successful completion request.
	completion, err = nexusrpc.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexusrpc.OperationCompletionSuccessfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)

	callbackToken, err = gen.Tokenize(completionToken)
	s.NoError(err)

	res, snap, _ = s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, callbackToken)
	s.Equal(http.StatusOK, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "outcome": "success"})
	// Ensure that CompleteOperation request is tracked as part of normal service telemetry metrics
	idx := slices.IndexFunc(snap["service_requests"], func(m *metricstest.CapturedRecording) bool {
		opTag, ok := m.Tags["operation"]
		return ok && opTag == "CompleteNexusOperation"
	})
	s.Greater(idx, -1)

	// Resend the request and verify we get a not found error since the operation has already completed.
	res, snap, _ = s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, callbackToken)
	s.Equal(http.StatusNotFound, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "outcome": "error_not_found"})

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Greater(completedEventIdx, 0)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								pollResp.History.Events[completedEventIdx].GetNexusOperationCompletedEventAttributes().Result,
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)

	// Reset the workflow and check that the completion event has been reapplied.
	resp, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         pollResp.WorkflowExecution,
		Reason:                    "test",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
	})
	s.NoError(err)

	hist := s.SdkClient().GetWorkflowHistory(ctx, run.GetID(), resp.RunId, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	seenCompletedEvent := false
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED {
			seenCompletedEvent = true
			break
		}
	}
	s.True(seenCompletedEvent)

	// Reset the workflow again to the same point with enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS option
	// and verify that the completion event has been excluded.
	resp, err = s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         pollResp.WorkflowExecution,
		Reason:                    "test",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
		ResetReapplyExcludeTypes:  []enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS},
	})
	s.NoError(err)

	hist = s.SdkClient().GetWorkflowHistory(ctx, run.GetID(), resp.RunId, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	seenCompletedEvent = false
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED {
			seenCompletedEvent = true
			break
		}
	}
	s.False(seenCompletedEvent)
}

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletionBeforeStart() {
	s.OverrideDynamicConfig(dynamicconfig.EnableRequestIdRefLinks, true)

	ctx := testcore.NewContext()
	taskQueues := []string{testcore.RandomizeStr(s.T().Name()), testcore.RandomizeStr(s.T().Name())}
	wfRuns := []client.WorkflowRun{}
	nexusTasks := []*workflowservice.PollNexusTaskQueueResponse{}

	completionWFType := "completion_wf"
	completionWFID := testcore.RandomizeStr(s.T().Name())
	completionWFTaskQueue := testcore.RandomizeStr(s.T().Name())
	completionWFStartReq := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                s.Namespace().String(),
		WorkflowId:               completionWFID,
		WorkflowType:             &commonpb.WorkflowType{Name: completionWFType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: completionWFTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:                    nil,
		WorkflowRunTimeout:       durationpb.New(100 * time.Second),
		Identity:                 "test",
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		OnConflictOptions: &workflowpb.OnConflictOptions{
			AttachRequestId:           true,
			AttachCompletionCallbacks: true,
		},
	}
	completionWFStartRequestIDs := []string{}
	completionWfRunIDs := []string{}

	// Start two workflows starting the same Nexus operation.
	// The first workflow will start a new workflow on the handler side.
	// The second workflow will have its callback attached to the running workflow.
	for _, tq := range taskQueues {
		endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
		_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: endpointName,
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_Worker_{
						Worker: &nexuspb.EndpointTarget_Worker{
							Namespace: s.Namespace().String(),
							TaskQueue: tq,
						},
					},
				},
			},
		})
		s.NoError(err)

		run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			TaskQueue: tq,
		}, "workflow")
		s.NoError(err)
		wfRuns = append(wfRuns, run)

		// Poll workflow task, and schedule Nexus operation.
		pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tq,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		s.NoError(err)
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
					Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
						ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
							Endpoint:  endpointName,
							Service:   "test-service",
							Operation: "my-operation",
							Input:     s.mustToPayload("input"),
						},
					},
				},
			},
		})
		s.NoError(err)

		// Poll Nexus task
		nexusTask, err := s.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: s.Namespace().String(),
			Identity:  uuid.NewString(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tq,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
		})
		s.NoError(err)
		nexusTasks = append(nexusTasks, nexusTask)

		// Get the Nexus request, and populate the start workflow request with the callback
		start := nexusTask.Request.Variant.(*nexuspb.Request_StartOperation).StartOperation
		s.Equal(op.Name(), start.Operation)
		start.CallbackHeader[nexus.HeaderOperationToken] = completionWFID
		completionWFStartReq.CompletionCallbacks = []*commonpb.Callback{
			{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url:    start.Callback,
						Header: start.CallbackHeader,
					},
				},
			},
		}
		// Make sure each request has a different request ID so they won't be deduped
		completionWFStartReq.RequestId = uuid.NewString()
		completionWFStartRequestIDs = append(completionWFStartRequestIDs, completionWFStartReq.RequestId)

		// Start workflow (first request) or attach callback (second request)
		completionRun, err := s.FrontendClient().StartWorkflowExecution(ctx, completionWFStartReq)
		s.NoError(err)
		completionWfRunIDs = append(completionWfRunIDs, completionRun.RunId)
	}

	s.Len(wfRuns, 2)
	s.Len(nexusTasks, 2)
	s.Len(completionWFStartRequestIDs, 2)
	s.NotEqual(completionWFStartRequestIDs[0], completionWFStartRequestIDs[1])
	s.Len(completionWfRunIDs, 2)
	// Check the handler wf run IDs are the same, ie., the second request didn't start a new workflow
	s.Equal(completionWfRunIDs[0], completionWfRunIDs[1])

	// Complete workflow containing callback
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: completionWFTaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								s.mustToPayload("result"),
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)

	expectedLinks := []*commonpb.Link_WorkflowEvent{
		&commonpb.Link_WorkflowEvent{
			Namespace:  s.Namespace().String(),
			WorkflowId: completionWFID,
			RunId:      completionWfRunIDs[0],
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   common.FirstEventID,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
		},
		&commonpb.Link_WorkflowEvent{
			Namespace:  s.Namespace().String(),
			WorkflowId: completionWFID,
			RunId:      completionWfRunIDs[1],
			Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
				RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
					RequestId: completionWFStartRequestIDs[1],
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				},
			},
		},
	}

	for i, tq := range taskQueues {
		// Poll and verify the fabricated start event and completion event are recorded and triggers workflow progress.
		pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tq,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		s.NoError(err)
		startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
			return e.GetNexusOperationStartedEventAttributes() != nil
		})
		s.NotEqual(-1, startedEventIdx)
		nexusOpStartedEvent := pollResp.History.Events[startedEventIdx]
		s.Equal(completionWFID, nexusOpStartedEvent.GetNexusOperationStartedEventAttributes().OperationToken)
		s.Len(nexusOpStartedEvent.Links, 1)
		s.ProtoEqual(expectedLinks[i], nexusOpStartedEvent.Links[0].GetWorkflowEvent())
		completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
			return e.GetNexusOperationCompletedEventAttributes() != nil
		})
		s.Greater(completedEventIdx, 0)

		// Complete start request to verify response is ignored.
		_, err = s.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			Identity:  uuid.NewString(),
			TaskToken: nexusTasks[i].TaskToken,
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
							AsyncSuccess: &nexuspb.StartOperationResponse_Async{
								OperationToken: completionWFID,
							},
						},
					},
				},
			},
		})
		s.NoErrorf(err, "Duplicate start response should be ignored.")

		// Complete caller workflow
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
						CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
							Result: &commonpb.Payloads{
								Payloads: []*commonpb.Payload{
									pollResp.History.Events[completedEventIdx].GetNexusOperationCompletedEventAttributes().Result,
								},
							},
						},
					},
				},
			},
		})
		s.NoError(err)

		var result string
		s.NoError(wfRuns[i].Get(ctx, &result))
		s.Equal("result", result)
	}
}

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncFailure() {
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	var callbackToken, publicCallbackURL string

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			publicCallbackURL = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		require.NoError(t, err)
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
					Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
						ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
							Endpoint:  endpointName,
							Service:   "service",
							Operation: "operation",
							Input:     s.mustToPayload("input"),
						},
					},
				},
			},
		})
		require.NoError(t, err)
	}, time.Second*20, time.Millisecond*200)

	// Poll and verify that the "started" event was recorded.
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Greater(startedEventIdx, 0)

	// Send a valid - failed completion request.
	completion, err := nexusrpc.NewOperationCompletionUnsuccessful(nexus.NewOperationFailedError("test operation failed"), nexusrpc.OperationCompletionUnsuccessfulOptions{})
	s.NoError(err)
	res, snap, _ := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, callbackToken)
	s.Equal(http.StatusOK, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "outcome": "success"})

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationFailedEventAttributes() != nil
	})
	s.Greater(completedEventIdx, 0)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{
					FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
						Failure: pollResp.History.Events[completedEventIdx].GetNexusOperationFailedEventAttributes().Failure,
					},
				},
			},
		},
	})
	s.NoError(err)
	var result string
	err = run.Get(ctx, &result)
	var wee *temporal.WorkflowExecutionError

	s.ErrorAs(err, &wee)
	s.True(strings.HasPrefix(wee.Unwrap().Error(), "nexus operation completed unsuccessfully"))
}

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletionErrors() {
	ctx := testcore.NewContext()

	completion, err := nexusrpc.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexusrpc.OperationCompletionSuccessfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)

	s.Run("ConfigDisabled", func() {
		s.OverrideDynamicConfig(dynamicconfig.EnableNexus, false)
		publicCallbackURL := "http://" + s.HttpAPIAddress() + "/" + commonnexus.RouteCompletionCallback.Path(s.Namespace().String())
		res, snap, _ := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, "")
		s.Equal(http.StatusNotFound, res.StatusCode)
		s.Equal(1, len(snap["nexus_completion_request_preprocess_errors"]))
	})

	s.Run("ConfigDisabledNoIdentifier", func() {
		s.OverrideDynamicConfig(dynamicconfig.EnableNexus, false)
		publicCallbackURL := "http://" + s.HttpAPIAddress() + commonnexus.PathCompletionCallbackNoIdentifier
		res, snap, _ := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, "")
		s.Equal(http.StatusNotFound, res.StatusCode)
		s.Equal(1, len(snap["nexus_completion_request_preprocess_errors"]))
	})

	s.Run("NamespaceNotFound", func() {
		// Generate a token with a non-existent namespace ID
		tokenWithBadNamespace, err := s.generateValidCallbackToken("namespace-doesnt-exist-id", testcore.RandomizeStr("workflow"), uuid.NewString())
		s.NoError(err)

		publicCallbackURL := "http://" + s.HttpAPIAddress() + "/" + commonnexus.RouteCompletionCallback.Path("namespace-doesnt-exist")
		res, snap, _ := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, tokenWithBadNamespace)
		s.Equal(http.StatusNotFound, res.StatusCode)
		s.Equal(1, len(snap["nexus_completion_request_preprocess_errors"]))
	})

	s.Run("NamespaceNotFoundNoIdentifier", func() {
		// Generate a token with a non-existent namespace ID
		tokenWithBadNamespace, err := s.generateValidCallbackToken("namespace-doesnt-exist-id", testcore.RandomizeStr("workflow"), uuid.NewString())
		s.NoError(err)

		publicCallbackURL := "http://" + s.HttpAPIAddress() + commonnexus.PathCompletionCallbackNoIdentifier
		res, snap, _ := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, tokenWithBadNamespace)
		s.Equal(http.StatusNotFound, res.StatusCode)
		s.Equal(1, len(snap["nexus_completion_request_preprocess_errors"]))
	})

	s.Run("OperationTokenTooLong", func() {
		publicCallbackURL := "http://" + s.HttpAPIAddress() + "/" + commonnexus.RouteCompletionCallback.Path(s.Namespace().String())
		completion, err := nexusrpc.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexusrpc.OperationCompletionSuccessfulOptions{
			Serializer:     commonnexus.PayloadSerializer,
			OperationToken: strings.Repeat("long", 2000),
		})
		s.NoError(err)

		// Generate a valid callback token to get past initial validation
		namespaceID := s.GetNamespaceID(s.Namespace().String())
		validToken, err := s.generateValidCallbackToken(namespaceID, testcore.RandomizeStr("workflow"), uuid.NewString())
		s.NoError(err)

		res, snap, _ := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, validToken)
		s.Equal(http.StatusBadRequest, res.StatusCode)
		s.Equal(0, len(snap["nexus_completion_request_preprocess_errors"]))
		s.Equal(1, len(snap["nexus_completion_requests"]))
		s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "outcome": "error_bad_request"})
	})

	s.Run("OperationTokenTooLongNoIdentifier", func() {
		publicCallbackURL := "http://" + s.HttpAPIAddress() + commonnexus.PathCompletionCallbackNoIdentifier
		completion, err := nexusrpc.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexusrpc.OperationCompletionSuccessfulOptions{
			Serializer:     commonnexus.PayloadSerializer,
			OperationToken: strings.Repeat("long", 2000),
		})
		s.NoError(err)

		// Generate a valid callback token to get past initial validation
		namespaceID := s.GetNamespaceID(s.Namespace().String())
		validToken, err := s.generateValidCallbackToken(namespaceID, testcore.RandomizeStr("workflow"), uuid.NewString())
		s.NoError(err)

		res, snap, _ := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, validToken)
		s.Equal(http.StatusBadRequest, res.StatusCode)
		s.Equal(0, len(snap["nexus_completion_request_preprocess_errors"]))
		s.Equal(1, len(snap["nexus_completion_requests"]))
		s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "outcome": "error_bad_request"})
	})

	s.Run("InvalidCallbackToken", func() {
		publicCallbackURL := "http://" + s.HttpAPIAddress() + "/" + commonnexus.RouteCompletionCallback.Path(s.Namespace().String())
		// metrics collection is not initialized before callback validation
		// Send request without callback token, helper does not add token if blank
		res, _, body := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, "")

		// Verify we get the correct error response
		s.Equal(http.StatusBadRequest, res.StatusCode)
		s.Contains(string(body), "invalid callback token", "Response should indicate invalid callback token")
	})

	s.Run("InvalidCallbackTokenNoIdentifier", func() {
		publicCallbackURL := "http://" + s.HttpAPIAddress() + commonnexus.PathCompletionCallbackNoIdentifier
		// metrics collection is not initialized before callback validation
		// Send request without callback token, helper does not add token if blank
		res, _, body := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, "")

		// Verify we get the correct error response
		s.Equal(http.StatusBadRequest, res.StatusCode)
		s.Contains(string(body), "invalid callback token", "Response should indicate invalid callback token")
	})

	s.Run("InvalidClientVersion", func() {
		publicCallbackURL := "http://" + s.HttpAPIAddress() + "/" + commonnexus.RouteCompletionCallback.Path(s.Namespace().String())
		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		// Generate a valid callback token to get past initial validation
		namespaceID := s.GetNamespaceID(s.Namespace().String())
		validToken, err := s.generateValidCallbackToken(namespaceID, testcore.RandomizeStr("workflow"), uuid.NewString())
		s.NoError(err)

		req, err := nexusrpc.NewCompletionHTTPRequest(ctx, publicCallbackURL, completion)
		s.NoError(err)
		req.Header.Set("User-Agent", "Nexus-go-sdk/v99.0.0")
		req.Header.Add(commonnexus.CallbackTokenHeader, validToken)

		res, err := http.DefaultClient.Do(req)
		s.NoError(err)
		_, err = io.ReadAll(res.Body)
		s.NoError(err)
		defer func() {
			err := res.Body.Close()
			s.NoError(err)
		}()

		snap := capture.Snapshot()
		s.Equal(http.StatusBadRequest, res.StatusCode)
		s.Equal(1, len(snap["nexus_completion_requests"]))
		s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "outcome": "unsupported_client"})
	})

	s.Run("InvalidClientVersionNoIdentifier", func() {
		publicCallbackURL := "http://" + s.HttpAPIAddress() + commonnexus.PathCompletionCallbackNoIdentifier
		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		// Generate a valid callback token to get past initial validation
		namespaceID := s.GetNamespaceID(s.Namespace().String())
		validToken, err := s.generateValidCallbackToken(namespaceID, testcore.RandomizeStr("workflow"), uuid.NewString())
		s.NoError(err)

		req, err := nexusrpc.NewCompletionHTTPRequest(ctx, publicCallbackURL, completion)
		s.NoError(err)
		req.Header.Set("User-Agent", "Nexus-go-sdk/v99.0.0")
		req.Header.Add(commonnexus.CallbackTokenHeader, validToken)

		res, err := http.DefaultClient.Do(req)
		s.NoError(err)
		_, err = io.ReadAll(res.Body)
		s.NoError(err)
		defer res.Body.Close()

		snap := capture.Snapshot()
		s.Equal(http.StatusBadRequest, res.StatusCode)
		s.Equal(1, len(snap["nexus_completion_requests"]))
		s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "outcome": "unsupported_client"})
	})
}

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletionAuthErrors() {
	ctx := testcore.NewContext()

	onAuthorize := func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
		if ct.APIName == configs.CompleteNexusOperation {
			return authorization.Result{Decision: authorization.DecisionDeny, Reason: "unauthorized in test"}, nil
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	}
	s.GetTestCluster().Host().SetOnAuthorize(onAuthorize)
	defer s.GetTestCluster().Host().SetOnAuthorize(nil)

	completion, err := nexusrpc.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexusrpc.OperationCompletionSuccessfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)

	// Generate a valid callback token for testing
	namespaceID := s.GetNamespaceID(s.Namespace().String())
	callbackToken, err := s.generateValidCallbackToken(namespaceID, testcore.RandomizeStr("workflow"), uuid.NewString())
	s.NoError(err)

	publicCallbackURL := "http://" + s.HttpAPIAddress() + "/" + commonnexus.RouteCompletionCallback.Path(s.Namespace().String())
	res, snap, _ := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, callbackToken)
	s.Equal(http.StatusForbidden, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "outcome": "unauthorized"})
}

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletionAuthErrorsNoIdentifier() {
	ctx := testcore.NewContext()

	onAuthorize := func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
		if ct.APIName == configs.CompleteNexusOperation {
			return authorization.Result{Decision: authorization.DecisionDeny, Reason: "unauthorized in test"}, nil
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	}
	s.GetTestCluster().Host().SetOnAuthorize(onAuthorize)
	defer s.GetTestCluster().Host().SetOnAuthorize(nil)

	completion, err := nexusrpc.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexusrpc.OperationCompletionSuccessfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)

	// Generate a valid callback token for testing
	namespaceID := s.GetNamespaceID(s.Namespace().String())
	callbackToken, err := s.generateValidCallbackToken(namespaceID, testcore.RandomizeStr("workflow"), uuid.NewString())
	s.NoError(err)

	publicCallbackURL := "http://" + s.HttpAPIAddress() + commonnexus.PathCompletionCallbackNoIdentifier
	res, snap, _ := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackURL, completion, callbackToken)
	s.Equal(http.StatusForbidden, res.StatusCode)
	s.Equal(1, len(snap["nexus_completion_requests"]))
	s.Subset(snap["nexus_completion_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "outcome": "unauthorized"})
}

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletionInternalAuth() {
	// Set URL template with invalid host
	s.OverrideDynamicConfig(
		nexusoperations.CallbackURLTemplate,
		"http://INTERNAL/namespaces/{{.NamespaceName}}/nexus/callback")

	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.Namespace().String(),
						TaskQueue: taskQueue,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	completionWFType := "completion_wf"
	completionWFTaskQueue := testcore.RandomizeStr(s.T().Name())
	completionWFStartReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         testcore.RandomizeStr(s.T().Name()),
		WorkflowType:       &commonpb.WorkflowType{Name: completionWFType},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: completionWFTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           "test",
	}

	go s.nexusTaskPoller(ctx, taskQueue, func(res *workflowservice.PollNexusTaskQueueResponse) (*nexuspb.Response, *nexuspb.HandlerError) {
		start := res.Request.Variant.(*nexuspb.Request_StartOperation).StartOperation
		s.Equal(op.Name(), start.Operation)

		completionWFStartReq.CompletionCallbacks = []*commonpb.Callback{
			{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url:    start.Callback,
						Header: start.CallbackHeader,
					},
				},
			},
		}

		_, err := s.FrontendClient().StartWorkflowExecution(ctx, completionWFStartReq)
		s.NoError(err)

		return &nexuspb.Response{
			Variant: &nexuspb.Response_StartOperation{
				StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
						AsyncSuccess: &nexuspb.StartOperationResponse_Async{
							OperationToken: "test-token",
						},
					},
				},
			},
		}, nil
	})

	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  endpointName,
						Service:   "test-service",
						Operation: "my-operation",
						Input:     s.mustToPayload("input"),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and verify that the "started" event was recorded.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)
	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Greater(startedEventIdx, 0)

	// Complete workflow containing callback
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: completionWFTaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								s.mustToPayload("result"),
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Greater(completedEventIdx, 0)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								pollResp.History.Events[completedEventIdx].GetNexusOperationCompletedEventAttributes().Result,
							},
						},
					},
				},
			},
		},
	})

	s.NoError(err)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)
}

func (s *NexusWorkflowTestSuite) TestNexusOperationCancelBeforeStarted_CancelationEventuallyDelivered() {
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	canStartCh := make(chan struct{})
	cancelSentCh := make(chan struct{})

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			select {
			case <-canStartCh:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test"}, nil
		},
		OnCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
			cancelSentCh <- struct{}{}
			return nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  endpointName,
						Service:   "service",
						Operation: "operation",
						Input:     s.mustToPayload("input"),
					},
				},
			},
			// Also wake the workflow up so it can cancel the operation.
			{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{
					StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
						TimerId:            "1",
						StartToFireTimeout: durationpb.New(time.Millisecond),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and cancel the operation.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)

	// Get the scheduleEventId to issue the cancel command.
	scheduledEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationScheduledEventAttributes() != nil
	})
	s.Greater(scheduledEventIdx, 0)
	scheduledEventID := pollResp.History.Events[scheduledEventIdx].EventId

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
				Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
					RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
						ScheduledEventId: scheduledEventID,
					},
				},
			},
		},
	})
	s.NoError(err)

	canStartCh <- struct{}{}
	s.WaitForChannel(ctx, cancelSentCh)

	// Terminate the workflow for good measure.
	err = s.SdkClient().TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "test")
	s.NoError(err)

	// Assert that we did not send a cancel request until after the operation was started.
	hist := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	})
	s.ContainsHistoryEvents(`
NexusOperationCancelRequested
NexusOperationStarted`, hist)
}

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletionAfterReset() {
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	var callbackToken, publicCallbackUrl string

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			publicCallbackUrl = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  endpointName,
						Service:   "service",
						Operation: "operation",
						Input:     s.mustToPayload("input"),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and verify that the "started" event was recorded.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Greater(startedEventIdx, 0)

	// Remember the workflow task completed event ID (next after the last WFT started), we'll use it to test reset
	// below.
	wftCompletedEventID := int64(len(pollResp.History.Events))

	// Reset the workflow and check that the started event has been reapplied.
	resetResp, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         pollResp.WorkflowExecution,
		Reason:                    "test",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
	})
	s.NoError(err)

	hist := s.SdkClient().GetWorkflowHistory(ctx, run.GetID(), resetResp.RunId, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	seenStartedEvent := false
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED {
			seenStartedEvent = true
		}
	}
	s.True(seenStartedEvent)
	completion, err := nexusrpc.NewOperationCompletionSuccessful(s.mustToPayload("result"), nexusrpc.OperationCompletionSuccessfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)

	res, _, _ := s.sendNexusCompletionRequest(ctx, s.T(), publicCallbackUrl, completion, callbackToken)
	s.Equal(http.StatusOK, res.StatusCode)

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Greater(completedEventIdx, 0)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								pollResp.History.Events[completedEventIdx].GetNexusOperationCompletedEventAttributes().Result,
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)
	var result string
	run = s.SdkClient().GetWorkflow(ctx, run.GetID(), resetResp.RunId)
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)
}

func (s *NexusWorkflowTestSuite) TestNexusAsyncOperationWithNilIO() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	callerTaskQueue := testcore.RandomizeStr("caller_" + s.T().Name())
	handlerWorkflowTaskQueue := testcore.RandomizeStr("handler_" + s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	handlerWorkflowID := testcore.RandomizeStr(s.T().Name())

	_, err := s.SdkClient().OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.Namespace().String(),
						TaskQueue: callerTaskQueue,
					},
				},
			},
		},
	})
	s.NoError(err)

	w := worker.New(
		s.SdkClient(),
		callerTaskQueue,
		worker.Options{},
	)

	svc := nexus.NewService("test")
	wf := func(ctx workflow.Context, input nexus.NoValue) (nexus.NoValue, error) {
		return nil, nil
	}
	op := temporalnexus.NewWorkflowRunOperation("op", wf, func(ctx context.Context, nv nexus.NoValue, opts nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
		return client.StartWorkflowOptions{
			ID:        handlerWorkflowID,
			TaskQueue: handlerWorkflowTaskQueue,
		}, nil
	})
	svc.Register(op)

	callerWF := func(ctx workflow.Context, input nexus.NoValue) (nexus.NoValue, error) {
		c := workflow.NewNexusClient(endpointName, svc.Name)
		fut := c.ExecuteOperation(ctx, op, nil, workflow.NexusOperationOptions{})
		var opExec workflow.NexusOperationExecution
		err := fut.GetNexusOperationExecution().Get(ctx, &opExec)
		s.NoError(err)
		s.NotEmpty(opExec.OperationToken)
		return nil, fut.Get(ctx, nil)
	}

	w.RegisterNexusService(svc)
	w.RegisterWorkflow(wf)
	w.RegisterWorkflow(callerWF)
	w.Start()
	defer w.Stop()

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: callerTaskQueue,
	}, callerWF, nil)
	s.NoError(err)

	pollRes, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: handlerWorkflowTaskQueue,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollRes.TaskToken,
		Identity:  "test",
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: nil, // Return nil here to verify that the conversion to nexus content works as expected.
					},
				},
			},
		},
	})
	s.NoError(err)

	s.NoError(run.Get(ctx, nil))
	history := s.SdkClient().GetWorkflowHistory(ctx, run.GetID(), "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for history.HasNext() {
		ev, err := history.Next()
		s.NoError(err)
		if attr := ev.GetNexusOperationCompletedEventAttributes(); attr != nil {
			protorequire.ProtoEqual(s.T(), s.mustToPayload(nil), attr.GetResult())
			break
		}
	}
}

func (s *NexusWorkflowTestSuite) TestNexusSyncOperationErrorRehydration() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	taskQueue := testcore.RandomizeStr("caller_" + s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	converter := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{})

	_, err := s.SdkClient().OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.Namespace().String(),
						TaskQueue: taskQueue,
					},
				},
			},
		},
	})
	s.NoError(err)

	w := worker.New(
		s.SdkClient(),
		taskQueue,
		worker.Options{},
	)

	svc := nexus.NewService("test")
	op := nexus.NewSyncOperation("op", func(ctx context.Context, outcome string, soo nexus.StartOperationOptions) (nexus.NoValue, error) {
		switch outcome {
		case "fail-handler-internal":
			return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional internal error")
		case "fail-handler-app-error":
			return nil, temporal.NewApplicationError("app error", "TestError", "details")
		case "fail-handler-bad-request":
			return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "bad request")
		case "fail-operation":
			return nil, nexus.NewOperationFailedError("some error")
		case "fail-operation-app-error":
			return nil, temporal.NewNonRetryableApplicationError("app error", "TestError", nil, "details")
		}
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "unexpected outcome: %s", outcome)
	})
	s.NoError(svc.Register(op))

	callerWF := func(ctx workflow.Context, outcome string) (nexus.NoValue, error) {
		c := workflow.NewNexusClient(endpointName, svc.Name)
		fut := c.ExecuteOperation(ctx, op, outcome, workflow.NexusOperationOptions{})
		return nil, fut.Get(ctx, nil)
	}

	w.RegisterNexusService(svc)
	w.RegisterWorkflow(callerWF)
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	cases := []struct {
		outcome            string
		metricsOutcome     string
		checkPendingError  func(t *testing.T, pendingErr error)
		checkWorkflowError func(t *testing.T, wfErr error)
	}{
		{
			outcome:        "fail-handler-internal",
			metricsOutcome: "handler-error:INTERNAL",
			checkPendingError: func(t *testing.T, pendingErr error) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, pendingErr, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
				var appErr *temporal.ApplicationError
				require.ErrorAs(t, handlerErr.Cause, &appErr)
				require.Equal(t, "intentional internal error", appErr.Message())
			},
		},
		{
			outcome:        "fail-handler-app-error",
			metricsOutcome: "handler-error:INTERNAL",
			checkPendingError: func(t *testing.T, pendingErr error) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, pendingErr, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
				var appErr *temporal.ApplicationError
				require.ErrorAs(t, handlerErr.Cause, &appErr)
				require.Equal(t, "app error", appErr.Message())
				require.Equal(t, "TestError", appErr.Type())
				var details string
				require.NoError(t, appErr.Details(&details))
				require.Equal(t, "details", details)
			},
		},
		{
			outcome:        "fail-handler-bad-request",
			metricsOutcome: "handler-error:BAD_REQUEST",
			checkWorkflowError: func(t *testing.T, wfErr error) {
				var opErr *temporal.NexusOperationError
				require.ErrorAs(t, wfErr, &opErr)
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, opErr, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
				var appErr *temporal.ApplicationError
				require.ErrorAs(t, handlerErr.Cause, &appErr)
				require.Equal(t, "bad request", appErr.Message())
			},
		},
		{
			outcome:        "fail-operation",
			metricsOutcome: "operation-unsuccessful:failed",
			checkWorkflowError: func(t *testing.T, wfErr error) {
				var opErr *temporal.NexusOperationError
				require.ErrorAs(t, wfErr, &opErr)
				var appErr *temporal.ApplicationError
				require.ErrorAs(t, opErr, &appErr)
				require.Equal(t, "some error", appErr.Message())
			},
		},
		{
			outcome:        "fail-operation-app-error",
			metricsOutcome: "handler-error:INTERNAL",
			checkWorkflowError: func(t *testing.T, wfErr error) {
				var opErr *temporal.NexusOperationError
				require.ErrorAs(t, wfErr, &opErr)
				var appErr *temporal.ApplicationError
				require.ErrorAs(t, opErr, &appErr)
				require.Equal(t, "app error", appErr.Message())
				require.Equal(t, "TestError", appErr.Type())
				var details string
				require.NoError(t, appErr.Details(&details))
				require.Equal(t, "details", details)
			},
		},
	}

	for _, tc := range cases {
		s.T().Run(tc.outcome, func(t *testing.T) {
			capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
			run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
				TaskQueue: taskQueue,
			}, callerWF, tc.outcome)
			s.NoError(err)

			if tc.checkPendingError != nil {
				var f *failurepb.Failure
				require.EventuallyWithT(t, func(t *assert.CollectT) {
					desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
					require.NoError(t, err)
					require.Len(t, desc.PendingNexusOperations, 1)
					f = desc.PendingNexusOperations[0].LastAttemptFailure
					require.NotNil(t, f)

				}, 10*time.Second, 100*time.Millisecond)
				s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)
				tc.checkPendingError(t, converter.FailureToError(f))
				s.NoError(s.SdkClient().TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "test cleanup"))
			} else {
				wfErr := run.Get(ctx, nil)
				s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)
				tc.checkWorkflowError(t, wfErr)
			}

			snap := capture.Snapshot()
			require.Len(t, snap["nexus_outbound_requests"], 1)
			require.Subset(
				t,
				snap["nexus_outbound_requests"][0].Tags,
				map[string]string{
					"namespace":      s.Namespace().String(),
					"method":         "StartOperation",
					"failure_source": "worker",
					"outcome":        tc.metricsOutcome,
				},
			)
		})

	}
}

func (s *NexusWorkflowTestSuite) TestNexusAsyncOperationErrorRehydration() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	testCtx := ctx
	taskQueue := testcore.RandomizeStr("caller_" + s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	handlerWorkflowID := testcore.RandomizeStr(s.T().Name())

	_, err := s.SdkClient().OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.Namespace().String(),
						TaskQueue: taskQueue,
					},
				},
			},
		},
	})
	s.NoError(err)

	w := worker.New(
		s.SdkClient(),
		taskQueue,
		worker.Options{},
	)

	svc := nexus.NewService("test")

	handlerWF := func(ctx workflow.Context, outcome string) (nexus.NoValue, error) {
		switch outcome {
		case "wait", "timeout":
			// Wait for the workflow to be canceled.
			return nil, workflow.Await(ctx, func() bool { return false })
		case "fail":
			return nil, temporal.NewApplicationError("app error", "TestError", "details")
		}
		return nil, fmt.Errorf("unexpected outcome: %s", outcome)
	}

	op := temporalnexus.NewWorkflowRunOperation("op", handlerWF, func(ctx context.Context, outcome string, soo nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
		var workflowExecutionTimeout time.Duration
		if outcome == "timeout" {
			workflowExecutionTimeout = time.Second
		}
		return client.StartWorkflowOptions{ID: handlerWorkflowID, WorkflowExecutionTimeout: workflowExecutionTimeout}, nil
	})
	s.NoError(svc.Register(op))

	callerWF := func(ctx workflow.Context, outcome, action string) (nexus.NoValue, error) {
		opCtx, cancel := workflow.WithCancel(ctx)
		defer cancel()
		c := workflow.NewNexusClient(endpointName, svc.Name)
		fut := c.ExecuteOperation(opCtx, op, outcome, workflow.NexusOperationOptions{})
		var exec workflow.NexusOperationExecution
		if err := fut.GetNexusOperationExecution().Get(ctx, &exec); err != nil {
			return nil, err
		}
		switch action {
		case "terminate":
			// Lazy man's version of a local activity, don't try this at home.
			workflow.SideEffect(ctx, func(ctx workflow.Context) any {
				err := s.SdkClient().TerminateWorkflow(testCtx, handlerWorkflowID, "", "")
				if err != nil {
					panic(err)
				}
				return nil
			})
		case "cancel":
			cancel()
			err := fut.Get(ctx, nil)
			// The Go SDK unwraps CanceledErrors when an error is returned from the workflow, assert in-workflow.
			var opErr *temporal.NexusOperationError
			if !errors.As(err, &opErr) {
				return nil, fmt.Errorf("expected NexusOperationError, got %w", err)
			}
			var canceledErr *temporal.CanceledError
			if !errors.As(opErr, &canceledErr) {
				return nil, fmt.Errorf("expected CanceledError, got %w", err)
			}
		}
		return nil, fut.Get(ctx, nil)
	}

	w.RegisterNexusService(svc)
	w.RegisterWorkflow(callerWF)
	w.RegisterWorkflow(handlerWF)
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	cases := []struct {
		outcome, action    string
		checkWorkflowError func(t *testing.T, wfErr error)
	}{
		{
			outcome: "fail",
			checkWorkflowError: func(t *testing.T, wfErr error) {
				var opErr *temporal.NexusOperationError
				require.ErrorAs(t, wfErr, &opErr)
				var appErr *temporal.ApplicationError
				require.ErrorAs(t, opErr, &appErr)
				require.Equal(t, "app error", appErr.Message())
				require.Equal(t, "TestError", appErr.Type())
				var details string
				require.NoError(t, appErr.Details(&details))
				require.Equal(t, "details", details)
			},
		},
		{
			outcome: "wait",
			action:  "terminate",
			checkWorkflowError: func(t *testing.T, wfErr error) {
				var opErr *temporal.NexusOperationError
				require.ErrorAs(t, wfErr, &opErr)
				var termErr *temporal.TerminatedError
				require.ErrorAs(t, opErr, &termErr)
			},
		},
		{
			outcome: "wait",
			action:  "cancel",
			checkWorkflowError: func(t *testing.T, wfErr error) {
				// The Go SDK loses the NexusOperationError (as well as any other error if it wraps a CanceledError),
				// assertions done in workflow.
				var canceledErr *temporal.CanceledError
				require.ErrorAs(t, wfErr, &canceledErr)
			},
		},
		{
			outcome: "timeout",
			checkWorkflowError: func(t *testing.T, wfErr error) {
				var opErr *temporal.NexusOperationError
				require.ErrorAs(t, wfErr, &opErr)
				var timeoutErr *temporal.TimeoutError
				require.ErrorAs(t, opErr, &timeoutErr)
			},
		},
	}

	for _, tc := range cases {
		s.T().Run(tc.outcome+"-"+tc.action, func(t *testing.T) {
			capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
			run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
				TaskQueue: taskQueue,
			}, callerWF, tc.outcome, tc.action)
			s.NoError(err)

			wfErr := run.Get(ctx, nil)
			s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)
			tc.checkWorkflowError(t, wfErr)

			snap := capture.Snapshot()
			require.GreaterOrEqual(t, len(snap["nexus_outbound_requests"]), 1)
			require.Subset(t, snap["nexus_outbound_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "method": "StartOperation", "failure_source": "_unknown_", "outcome": "pending"})
		})

	}
}

func (s *NexusWorkflowTestSuite) TestNexusCallbackAfterCallerComplete() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	taskQueue := testcore.RandomizeStr("caller_" + s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	handlerWorkflowID := testcore.RandomizeStr(s.T().Name())

	_, err := s.SdkClient().OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.Namespace().String(),
						TaskQueue: taskQueue,
					},
				},
			},
		},
	})
	s.NoError(err)

	w := worker.New(
		s.SdkClient(),
		taskQueue,
		worker.Options{},
	)

	svc := nexus.NewService("test")

	handlerWF := func(ctx workflow.Context, _ nexus.NoValue) (nexus.NoValue, error) {
		signalChan := workflow.GetSignalChannel(ctx, "test-signal")
		if ok, _ := signalChan.ReceiveWithTimeout(ctx, 10*time.Second, nil); !ok {
			return nil, errors.New("receive signal timed out")
		}

		return nil, nil
	}

	op := temporalnexus.NewWorkflowRunOperation("op", handlerWF, func(ctx context.Context, _ nexus.NoValue, soo nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
		return client.StartWorkflowOptions{ID: handlerWorkflowID}, nil
	})
	s.NoError(svc.Register(op))

	callerWF := func(ctx workflow.Context) error {
		c := workflow.NewNexusClient(endpointName, svc.Name)
		fut := c.ExecuteOperation(ctx, op, nil, workflow.NexusOperationOptions{})
		return fut.GetNexusOperationExecution().Get(ctx, nil)
	}

	w.RegisterNexusService(svc)
	w.RegisterWorkflow(callerWF)
	w.RegisterWorkflow(handlerWF)
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, callerWF)
	s.NoError(err)
	s.NoError(run.Get(ctx, nil))

	err = s.SdkClient().SignalWorkflow(ctx, handlerWorkflowID, "", "test-signal", nil)
	s.NoError(err)

	s.EventuallyWithT(func(ct *assert.CollectT) {
		resp, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: handlerWorkflowID,
			},
		})
		require.NoError(ct, err)
		require.Len(ct, resp.Callbacks, 1)
		require.Equal(ct, enumspb.CALLBACK_STATE_FAILED, resp.Callbacks[0].State)
		require.NotNil(ct, resp.Callbacks[0].LastAttemptFailure)
		require.Equal(ct, "handler error (NOT_FOUND): workflow execution already completed", resp.Callbacks[0].LastAttemptFailure.Message)
	}, 3*time.Second, 200*time.Millisecond)
}

func (s *NexusWorkflowTestSuite) TestNexusOperationSyncNexusFailure() {
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			return nil, &nexus.HandlerError{
				Type: nexus.HandlerErrorTypeBadRequest,
				Cause: &nexus.FailureError{
					Failure: nexus.Failure{
						Message:  "fail me",
						Metadata: map[string]string{"key": "val"},
						Details:  []byte(`"details"`),
					},
				},
			}
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{
					External: &nexuspb.EndpointTarget_External{
						Url: "http://" + listenAddr,
					},
				},
			},
		},
	})
	s.NoError(err)

	w := worker.New(
		s.SdkClient(),
		taskQueue,
		worker.Options{},
	)

	callerWF := func(ctx workflow.Context) (nexus.NoValue, error) {
		c := workflow.NewNexusClient(endpointName, "dont-care")
		fut := c.ExecuteOperation(ctx, "operation", nil, workflow.NexusOperationOptions{})
		return nil, fut.Get(ctx, nil)
	}

	w.RegisterWorkflow(callerWF)
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, callerWF)
	s.NoError(err)
	wfErr := run.Get(ctx, nil)
	s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	var handlerErr *nexus.HandlerError
	s.ErrorAs(wfErr, &handlerErr)
	s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
	var appErr *temporal.ApplicationError
	s.ErrorAs(handlerErr.Cause, &appErr)
	s.Equal(appErr.Message(), "fail me")
	var failure nexus.Failure
	s.NoError(appErr.Details(&failure))
	s.Equal(map[string]string{"key": "val"}, failure.Metadata)
	var details string
	s.NoError(json.Unmarshal(failure.Details, &details))
	s.Equal("details", details)

	snap := capture.Snapshot()
	s.Len(snap["nexus_outbound_requests"], 1)
	// Confirming that requests which do not go through our frontend are not tagged with `failure_source`
	s.Subset(snap["nexus_outbound_requests"][0].Tags, map[string]string{"namespace": s.Namespace().String(), "method": "StartOperation", "failure_source": "_unknown_", "outcome": "handler-error:BAD_REQUEST"})
}

func (s *NexusWorkflowTestSuite) TestNexusAsyncOperationWithMultipleCallers() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	callerTaskQueue := testcore.RandomizeStr("caller_" + s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	handlerWorkflowID := testcore.RandomizeStr(s.T().Name())

	// number of concurrent Nexus operation calls
	numCalls := 5

	_, err := s.SdkClient().OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.Namespace().String(),
						TaskQueue: callerTaskQueue,
					},
				},
			},
		},
	})
	s.NoError(err)

	w := worker.New(s.SdkClient(), callerTaskQueue, worker.Options{})
	svc := nexus.NewService("test")
	handlerWf := func(ctx workflow.Context, input string) (string, error) {
		workflow.GetSignalChannel(ctx, "terminate").Receive(ctx, nil)
		return "hello " + input, nil
	}

	op := temporalnexus.NewWorkflowRunOperation(
		"op",
		handlerWf,
		func(ctx context.Context, input string, opts nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
			var conflictPolicy enumspb.WorkflowIdConflictPolicy
			if input == "conflict-policy-use-existing" {
				conflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
			}
			return client.StartWorkflowOptions{
				ID:                       handlerWorkflowID,
				WorkflowIDConflictPolicy: conflictPolicy,
			}, nil
		},
	)
	svc.MustRegister(op)

	type CallerWfOutput struct {
		CntOk  int
		CntErr int
	}

	callerWf := func(ctx workflow.Context, input string) (CallerWfOutput, error) {
		output := CallerWfOutput{}
		var retError error

		c := workflow.NewNexusClient(endpointName, svc.Name)

		nexusFutures := []workflow.NexusOperationFuture{}
		for i := 0; i < numCalls; i++ {
			fut := c.ExecuteOperation(ctx, op, input, workflow.NexusOperationOptions{})
			nexusFutures = append(nexusFutures, fut)
		}

		nexusOpStartedFutures := []workflow.NexusOperationFuture{}
		for _, fut := range nexusFutures {
			var exec workflow.NexusOperationExecution
			err := fut.GetNexusOperationExecution().Get(ctx, &exec)
			if err == nil {
				output.CntOk++
				nexusOpStartedFutures = append(nexusOpStartedFutures, fut)
				continue
			}
			output.CntErr++
			var handlerErr *nexus.HandlerError
			var appErr *temporal.ApplicationError
			if !errors.As(err, &handlerErr) {
				retError = err
			} else if !errors.As(handlerErr, &appErr) {
				retError = err
			} else if appErr.Type() != "WorkflowExecutionAlreadyStarted" {
				retError = err
			}
		}

		if output.CntOk > 0 {
			// signal handler workflow so it will complete
			err = workflow.SignalExternalWorkflow(ctx, handlerWorkflowID, "", "terminate", nil).Get(ctx, nil)
			if err != nil {
				return output, err
			}
		}

		for _, fut := range nexusOpStartedFutures {
			var res string
			err := fut.Get(ctx, &res)
			if err != nil {
				retError = err
			} else if res != "hello "+input {
				retError = fmt.Errorf("unexpected result from handler workflow: %q", res)
			}
		}

		return output, retError
	}

	w.RegisterNexusService(svc)
	w.RegisterWorkflow(handlerWf)
	w.RegisterWorkflowWithOptions(callerWf, workflow.RegisterOptions{Name: "caller-wf"})
	s.NoError(w.Start())
	defer w.Stop()

	testCases := []struct {
		input       string
		checkOutput func(t *testing.T, res CallerWfOutput, err error)
	}{
		{
			input: "conflict-policy-fail",
			checkOutput: func(t *testing.T, res CallerWfOutput, err error) {
				require.NoError(t, err)
				require.EqualValues(t, 1, res.CntOk)
				require.EqualValues(t, numCalls-1, res.CntErr)

				// check the handler workflow has the request ID infos map correct
				descResp, err := s.SdkClient().DescribeWorkflowExecution(context.Background(), handlerWorkflowID, "")
				require.NoError(t, err)
				requestIDInfos := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()
				require.NotNil(t, requestIDInfos)
				require.Len(t, requestIDInfos, 1)
				for _, info := range requestIDInfos {
					require.False(t, info.Buffered)
					require.GreaterOrEqual(t, info.EventId, common.FirstEventID)
					require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, info.EventType)
				}
			},
		},
		{
			input: "conflict-policy-use-existing",
			checkOutput: func(t *testing.T, res CallerWfOutput, err error) {
				require.NoError(t, err)
				require.EqualValues(t, numCalls, res.CntOk)
				require.EqualValues(t, 0, res.CntErr)

				// check the handler workflow has the request ID infos map correct
				descResp, err := s.SdkClient().DescribeWorkflowExecution(context.Background(), handlerWorkflowID, "")
				require.NoError(t, err)
				requestIDInfos := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()
				require.NotNil(t, requestIDInfos)
				cntStarted := 0
				cntAttached := 0
				for _, info := range requestIDInfos {
					require.False(t, info.Buffered)
					require.GreaterOrEqual(t, info.EventId, common.FirstEventID)
					switch info.EventType {
					case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
						cntStarted++
					case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED:
						cntAttached++
					default:
						require.Fail(t, "Unexpected event type in request ID info")
					}
				}
				require.Equal(t, 1, cntStarted)
				require.Equal(t, numCalls-1, cntAttached)
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.input, func() {
			run, err := s.SdkClient().ExecuteWorkflow(
				ctx,
				client.StartWorkflowOptions{
					TaskQueue: callerTaskQueue,
				},
				callerWf,
				tc.input,
			)
			s.NoError(err)
			var res CallerWfOutput
			err = run.Get(ctx, &res)
			tc.checkOutput(s.T(), res, err)
		})
	}
}

// generateValidCallbackToken creates a valid callback token for testing with the given namespace, workflow, and run IDs
func (s *NexusWorkflowTestSuite) generateValidCallbackToken(namespaceID, workflowID, runID string) (string, error) {
	gen := &commonnexus.CallbackTokenGenerator{}
	return gen.Tokenize(&tokenspb.NexusOperationCompletion{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
		RunId:       runID,
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{
				{
					Type: "nexusoperations.Operation",
					Id:   uuid.NewString(),
				},
			},
			MutableStateVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          1,
			},
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          0,
			},
		},
		RequestId: uuid.NewString(),
	})
}

func (s *NexusWorkflowTestSuite) sendNexusCompletionRequest(
	ctx context.Context,
	t *testing.T,
	url string,
	completion nexusrpc.OperationCompletion,
	callbackToken string,
) (*http.Response, map[string][]*metricstest.CapturedRecording, string) {
	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)
	req, err := nexusrpc.NewCompletionHTTPRequest(ctx, url, completion)
	require.NoError(t, err)
	if callbackToken != "" {
		req.Header.Add(commonnexus.CallbackTokenHeader, callbackToken)
	}

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	responseBody := res.Body
	body, err := io.ReadAll(responseBody)
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())
	res.Body = io.NopCloser(bytes.NewReader(body))
	return res, capture.Snapshot(), string(body)
}
