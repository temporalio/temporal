package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics/metricstest"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/frontend/configs"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type NexusWorkflowTestSuite struct {
	parallelsuite.Suite[*NexusWorkflowTestSuite]
}

func TestNexusWorkflowTestSuiteHSM(t *testing.T) {
	parallelsuite.Run(t, &NexusWorkflowTestSuite{}, false)
}

func TestNexusWorkflowTestSuiteCHASM(t *testing.T) {
	parallelsuite.Run(t, &NexusWorkflowTestSuite{}, true)
}

func (s *NexusWorkflowTestSuite) newTestEnv(chasmEnabled bool, opts ...testcore.TestOption) *NexusTestEnv {
	return newNexusTestEnv(s.T(), true, append(
		opts,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, chasmEnabled),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, chasmEnabled),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmNexus, chasmEnabled),
	)...)
}

func (s *NexusWorkflowTestSuite) TestNexusOperationCancelation(chasmEnabled bool) {
	if chasmEnabled {
		s.T().Skip("Blocked on CHASM Nexus cancellation support")
	}

	env := s.newTestEnv(chasmEnabled)
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	firstCancelSeen := false
	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			if service != "service" {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, `expected service to equal "service"`)
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test"}, nil
		},
		OnCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
			if !firstCancelSeen {
				// Fail cancel request once to test NexusOperationCancelRequestFailed event is recorded and request is retried.
				firstCancelSeen = true
				return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "intentional non-retyrable cancel error for test")
			}
			return nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
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

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: time.Second,
	}, "workflow")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		require.NoError(t, err)
		_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
							Input:     testcore.MustToPayload(s.T(), "input"),
						},
					},
				},
			},
		})
		require.NoError(t, err)
	}, time.Second*20, time.Millisecond*200)

	// Poll and wait for the "started" event to be recorded.
	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
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
	s.Positive(startedEventIdx)

	// Get the scheduleEventId to issue the cancel command.
	scheduledEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationScheduledEventAttributes() != nil
	})
	s.Positive(scheduledEventIdx)
	scheduledEventID := pollResp.History.Events[scheduledEventIdx].EventId

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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

	// Poll and verify first cancel request failed and allowed workflow to make progress.
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
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
	s.Positive(cancelFailedIdx)

	// Start new operation to successfully cancel.
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
						Input:     testcore.MustToPayload(s.T(), "input"),
					},
				},
			},
		},
	})
	s.NoError(err)
	// Poll and wait for the "started" event to be recorded.
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
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
	s.Positive(secondScheduledEventID)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		require.NoError(t, err)
		require.Len(t, desc.PendingNexusOperations, 2)
		op1 := desc.PendingNexusOperations[0]
		require.Equal(t, scheduledEventID, op1.ScheduledEventId)
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

	err = env.SdkClient().TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "test")
	s.NoError(err)

	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	})
	s.ContainsHistoryEvents(`NexusOperationCancelRequestFailed`, hist)
	s.ContainsHistoryEvents(`NexusOperationCancelRequestCompleted`, hist)
}

func (s *NexusWorkflowTestSuite) TestNexusOperationSyncCompletion(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled)
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
	handlerNexusLink := commonnexus.ConvertLinkWorkflowEventToNexusLink(handlerLink)

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			nexus.AddHandlerLinks(ctx, handlerNexusLink)
			return &nexus.HandlerStartOperationResultSync[any]{Value: "result"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
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

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		require.NoError(t, err)
		_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
							Input:     testcore.MustToPayload(s.T(), "input"),
						},
					},
				},
			},
		})
		require.NoError(t, err)
	}, time.Second*20, time.Millisecond*200)

	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
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
	s.Positive(completedEventIdx)
	s.Len(pollResp.History.Events[completedEventIdx].GetLinks(), 1)
	protorequire.ProtoEqual(s.T(), handlerLink, pollResp.History.Events[completedEventIdx].GetLinks()[0].GetWorkflowEvent())

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
	desc, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
		},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	s.Empty(desc.DatabaseMutableState.GetExecutionInfo().SubStateMachinesByType)
}

func (s *NexusWorkflowTestSuite) TestNexusOperationSyncCompletion_LargePayload(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled)
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

	_, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
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

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		require.NoError(t, err)
		_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
							Input:     testcore.MustToPayload(s.T(), "input"),
						},
					},
				},
			},
		})
		require.NoError(t, err)
	}, time.Second*20, time.Millisecond*200)

	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
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
	s.Positive(failedEventIdx)

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								testcore.MustToPayload(s.T(), pollResp.History.Events[failedEventIdx].GetNexusOperationFailedEventAttributes().Failure.Cause.Message),
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

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletion(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled)
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	testClusterInfo, err := env.FrontendClient().GetClusterInfo(ctx, &workflowservice.GetClusterInfoRequest{})
	s.NoError(err)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
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
	handlerNexusLink := commonnexus.ConvertLinkWorkflowEventToNexusLink(handlerLink)

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

			expectedLinkCount := 1
			if chasmEnabled {
				expectedLinkCount = 2
			}
			s.Len(options.Links, expectedLinkCount)

			// Verify workflow event link is present
			workflowEventLinkIdx := slices.IndexFunc(options.Links, func(link nexus.Link) bool {
				return link.Type == string((&commonpb.Link_WorkflowEvent{}).ProtoReflect().Descriptor().FullName())
			})
			s.NotEqual(-1, workflowEventLinkIdx)
			workflowEventLink, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(options.Links[workflowEventLinkIdx])
			s.NoError(err)
			protorequire.ProtoEqual(s.T(), &commonpb.Link_WorkflowEvent{
				Namespace:  env.Namespace().String(),
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
			}, workflowEventLink)

			// Verify nexus operation link is present
			if chasmEnabled {
				nexusOperationLinkIdx := slices.IndexFunc(options.Links, func(link nexus.Link) bool {
					return link.Type == string((&commonpb.Link_NexusOperation{}).ProtoReflect().Descriptor().FullName())
				})
				s.NotEqual(-1, nexusOperationLinkIdx)
				expectedNexusOperationLink := commonnexus.ConvertLinkNexusOperationToNexusLink(&commonpb.Link_NexusOperation{
					Namespace:   env.Namespace().String(),
					OperationId: run.GetID(),
					RunId:       run.GetRunID(),
				})
				s.Equal(expectedNexusOperationLink.URL.String(), options.Links[nexusOperationLinkIdx].URL.String())
				s.Equal(expectedNexusOperationLink.Type, options.Links[nexusOperationLinkIdx].Type)
			}

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

	_, err = env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
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

	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
						Input:     testcore.MustToPayload(s.T(), "input"),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and verify that the "started" event was recorded.
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
	s.Positive(startedEventIdx)

	s.Len(pollResp.History.Events[startedEventIdx].Links, 1)
	l := pollResp.History.Events[startedEventIdx].Links[0].GetWorkflowEvent()
	protorequire.ProtoEqual(s.T(), handlerLink, l)

	// Completion request fails if the result payload is too large.
	largeCompletion := nexusrpc.CompleteOperationOptions{
		// Use -10 to avoid hitting MaxNexusAPIRequestBodyBytes. Actual payload will still exceed limit because of
		// additional Content headers. See common/rpc/grpc.go:66
		Result: testcore.MustToPayload(s.T(), strings.Repeat("a", (2*1024*1024)-10)),
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callbackToken},
	}
	s.NoError(err)
	capture := env.StartNamespaceMetricCapture()
	err = s.sendNexusCompletionRequest(ctx, publicCallbackURL, largeCompletion)
	completionRequests := capture.Metric("nexus_completion_requests")
	var handlerErr *nexus.HandlerError
	s.ErrorAs(err, &handlerErr)
	s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
	s.Len(completionRequests, 1)
	s.Subset(completionRequests[0].Tags, map[string]string{"namespace": env.Namespace().String(), "outcome": "error_bad_request"})

	invalidNamespace := testcore.RandomizeStr("ns")
	_, err = env.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        invalidNamespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(time.Hour * 24),
	})
	s.NoError(err)

	// Send an invalid completion request and verify that we get an error that the namespace in the URL doesn't match the namespace in the token.
	invalidCallbackURL := "http://" + env.HttpAPIAddress() + "/" + commonnexus.RouteCompletionCallback.Path(invalidNamespace)

	completion := nexusrpc.CompleteOperationOptions{
		Result: testcore.MustToPayload(s.T(), "result"),
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callbackToken},
		// Repeat the handler link on completion to verify callback links do not leak onto the completed event.
		Links: []nexus.Link{handlerNexusLink},
	}
	err = s.sendNexusCompletionRequest(ctx, invalidCallbackURL, completion)
	// Verify we get the correct error response
	s.ErrorAs(err, &handlerErr)
	s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
	s.Contains(handlerErr.Error(), "invalid callback token", "Response should indicate namespace mismatch")

	// Manipulate the token to verify we get the expected errors in the API.
	gen := &commonnexus.CallbackTokenGenerator{}
	decodedToken, err := commonnexus.DecodeCallbackToken(callbackToken)
	s.NoError(err)
	completionToken, err := gen.DecodeCompletion(decodedToken)
	s.NoError(err)

	assertInvalidCompletionTokenRejected := func(
		caseName string,
		mutate func(*tokenspb.NexusOperationCompletion) *tokenspb.NexusOperationCompletion,
		expectedErrorType nexus.HandlerErrorType,
	) {
		s.T().Helper()
		capture = env.StartNamespaceMetricCapture()

		mutatedCompletionToken := mutate(common.CloneProto(completionToken))
		mutatedCallbackToken, err := gen.Tokenize(mutatedCompletionToken)
		s.NoError(err, caseName)
		completion.Header = nexus.Header{commonnexus.CallbackTokenHeader: mutatedCallbackToken}

		err = s.sendNexusCompletionRequest(ctx, publicCallbackURL, completion)
		var handlerErr *nexus.HandlerError
		s.ErrorAs(err, &handlerErr, caseName)
		s.Equal(expectedErrorType, handlerErr.Type, caseName)

		completionRequests = capture.Metric("nexus_completion_requests")
		if expectedErrorType == nexus.HandlerErrorTypeNotFound {
			s.Len(completionRequests, 1, caseName)
			s.Subset(completionRequests[0].Tags, map[string]string{"outcome": "error_not_found"}, caseName)
		} else {
			s.Empty(completionRequests, caseName)
		}
	}

	if chasmEnabled {
		assertInvalidCompletionTokenRejected(
			"missing execution",
			func(token *tokenspb.NexusOperationCompletion) *tokenspb.NexusOperationCompletion {
				s.mutateCompletionComponentRef(token, func(ref *persistencespb.ChasmComponentRef) {
					ref.BusinessId = "not-found"
				})
				return token
			},
			nexus.HandlerErrorTypeNotFound,
		)
		assertInvalidCompletionTokenRejected(
			"missing run",
			func(token *tokenspb.NexusOperationCompletion) *tokenspb.NexusOperationCompletion {
				s.mutateCompletionComponentRef(token, func(ref *persistencespb.ChasmComponentRef) {
					ref.RunId = uuid.NewString()
				})
				return token
			},
			nexus.HandlerErrorTypeNotFound,
		)
		assertInvalidCompletionTokenRejected(
			"wrong archetype",
			func(token *tokenspb.NexusOperationCompletion) *tokenspb.NexusOperationCompletion {
				s.mutateCompletionComponentRef(token, func(ref *persistencespb.ChasmComponentRef) {
					ref.ArchetypeId = chasm.SchedulerArchetypeID
				})
				return token
			},
			nexus.HandlerErrorTypeNotFound,
		)
		assertInvalidCompletionTokenRejected(
			"empty component ref",
			func(token *tokenspb.NexusOperationCompletion) *tokenspb.NexusOperationCompletion {
				token.ComponentRef = nil
				return token
			},
			nexus.HandlerErrorTypeBadRequest,
		)
		assertInvalidCompletionTokenRejected(
			"malformed component ref",
			func(token *tokenspb.NexusOperationCompletion) *tokenspb.NexusOperationCompletion {
				token.ComponentRef = []byte("not-a-proto")
				return token
			},
			nexus.HandlerErrorTypeBadRequest,
		)
	} else {
		assertInvalidCompletionTokenRejected(
			"workflow not found",
			func(token *tokenspb.NexusOperationCompletion) *tokenspb.NexusOperationCompletion {
				token.WorkflowId = "not-found"
				return token
			},
			nexus.HandlerErrorTypeNotFound,
		)
		assertInvalidCompletionTokenRejected(
			"stale state machine ref",
			func(token *tokenspb.NexusOperationCompletion) *tokenspb.NexusOperationCompletion {
				token.Ref.MachineInitialVersionedTransition.NamespaceFailoverVersion++
				return token
			},
			nexus.HandlerErrorTypeNotFound,
		)
	}

	callbackToken, err = gen.Tokenize(completionToken)
	s.NoError(err)
	completion.Header = nexus.Header{commonnexus.CallbackTokenHeader: callbackToken}

	capture = env.StartNamespaceMetricCapture()
	err = s.sendNexusCompletionRequest(ctx, publicCallbackURL, completion)
	s.NoError(err)
	completionRequests = capture.Metric("nexus_completion_requests")
	serviceRequests := capture.Metric("service_requests")
	s.Len(completionRequests, 1)
	s.Subset(completionRequests[0].Tags, map[string]string{"namespace": env.Namespace().String(), "outcome": "success"})
	// Ensure that CompleteOperation request is tracked as part of normal service telemetry metrics
	idx := slices.IndexFunc(serviceRequests, func(m *metricstest.CapturedRecording) bool {
		opTag, ok := m.Tags["operation"]
		return ok && opTag == "CompleteNexusOperation"
	})
	s.Greater(idx, -1)

	// Resend the request and verify we get a not found error since the operation has already completed.
	capture = env.StartNamespaceMetricCapture()
	err = s.sendNexusCompletionRequest(ctx, publicCallbackURL, completion)
	s.ErrorAs(err, &handlerErr)
	s.Equal(nexus.HandlerErrorTypeNotFound, handlerErr.Type)
	completionRequests = capture.Metric("nexus_completion_requests")
	s.Len(completionRequests, 1)
	s.Subset(completionRequests[0].Tags, map[string]string{"namespace": env.Namespace().String(), "outcome": "error_not_found"})

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	startedEventIdx = slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Positive(startedEventIdx)
	s.Len(pollResp.History.Events[startedEventIdx].Links, 1)
	l = pollResp.History.Events[startedEventIdx].Links[0].GetWorkflowEvent()
	protorequire.ProtoEqual(s.T(), handlerLink, l)

	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Positive(completedEventIdx)
	s.Empty(pollResp.History.Events[completedEventIdx].Links)

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
	resp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         pollResp.WorkflowExecution,
		Reason:                    "test",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
	})
	s.NoError(err)

	hist := env.SdkClient().GetWorkflowHistory(ctx, run.GetID(), resp.RunId, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

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
	resp, err = env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         pollResp.WorkflowExecution,
		Reason:                    "test",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
		ResetReapplyExcludeTypes:  []enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS},
	})
	s.NoError(err)

	hist = env.SdkClient().GetWorkflowHistory(ctx, run.GetID(), resp.RunId, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

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

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletionBeforeStart(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled)
	ctx := testcore.NewContext()
	taskQueues := []string{testcore.RandomizeStr(s.T().Name()), testcore.RandomizeStr(s.T().Name())}
	wfRuns := []client.WorkflowRun{}
	nexusTasks := []*workflowservice.PollNexusTaskQueueResponse{}

	completionWFType := "completion_wf"
	completionWFID := testcore.RandomizeStr(s.T().Name())
	completionWFTaskQueue := testcore.RandomizeStr(s.T().Name())
	completionWFStartReq := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                env.Namespace().String(),
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
		_, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: endpointName,
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_Worker_{
						Worker: &nexuspb.EndpointTarget_Worker{
							Namespace: env.Namespace().String(),
							TaskQueue: tq,
						},
					},
				},
			},
		})
		s.NoError(err)

		run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			TaskQueue: tq,
		}, "workflow")
		s.NoError(err)
		wfRuns = append(wfRuns, run)

		// Poll workflow task, and schedule Nexus operation.
		pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tq,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		s.NoError(err)
		_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
							Input:     testcore.MustToPayload(s.T(), "input"),
						},
					},
				},
			},
		})
		s.NoError(err)

		// Poll Nexus task
		nexusTask, err := env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: env.Namespace().String(),
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
		completionRun, err := env.FrontendClient().StartWorkflowExecution(ctx, completionWFStartReq)
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
	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: completionWFTaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	completionWorkflowHistory := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: completionWFID,
		RunId:      completionWfRunIDs[0],
	})
	completionWorkflowStartedEventIdx := slices.IndexFunc(completionWorkflowHistory, func(e *historypb.HistoryEvent) bool {
		return e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
	})
	s.NotEqual(-1, completionWorkflowStartedEventIdx)
	completionWorkflowStartTime := completionWorkflowHistory[completionWorkflowStartedEventIdx].GetEventTime().AsTime()
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								testcore.MustToPayload(s.T(), "result"),
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)

	expectedLinks := []*commonpb.Link_WorkflowEvent{
		{
			Namespace:  env.Namespace().String(),
			WorkflowId: completionWFID,
			RunId:      completionWfRunIDs[0],
			Reference: &commonpb.Link_WorkflowEvent_EventRef{
				EventRef: &commonpb.Link_WorkflowEvent_EventReference{
					EventId:   common.FirstEventID,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				},
			},
		},
		{
			Namespace:  env.Namespace().String(),
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
		pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
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
		s.Equal(
			completionWorkflowStartTime.Truncate(time.Second),
			nexusOpStartedEvent.GetEventTime().AsTime().Truncate(time.Second),
		)
		s.Len(nexusOpStartedEvent.Links, 1)
		s.ProtoEqual(expectedLinks[i], nexusOpStartedEvent.Links[0].GetWorkflowEvent())

		completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
			return e.GetNexusOperationCompletedEventAttributes() != nil
		})
		s.Positive(completedEventIdx)
		s.Less(startedEventIdx, completedEventIdx)
		s.Empty(pollResp.History.Events[completedEventIdx].Links)

		// Complete start request to verify response is ignored.
		_, err = env.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
			Namespace: env.Namespace().String(),
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
		_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
						CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
							Result: &commonpb.Payloads{
								Payloads: []*commonpb.Payload{
									testcore.MustToPayload(s.T(), "result"),
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

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncFailure(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled)
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

	_, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
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

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "test",
		})
		require.NoError(t, err)
		_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
							Input:     testcore.MustToPayload(s.T(), "input"),
						},
					},
				},
			},
		})
		require.NoError(t, err)
	}, time.Second*20, time.Millisecond*200)

	// Poll and verify that the "started" event was recorded.
	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Positive(startedEventIdx)

	// Send a valid - failed completion request.
	completion := nexusrpc.CompleteOperationOptions{
		Error:  nexus.NewOperationFailedErrorf("test operation failed"),
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callbackToken},
	}
	capture := env.StartNamespaceMetricCapture()
	err = s.sendNexusCompletionRequest(ctx, publicCallbackURL, completion)
	completionRequests := capture.Metric("nexus_completion_requests")
	s.NoError(err)
	s.Len(completionRequests, 1)
	s.Subset(completionRequests[0].Tags, map[string]string{"namespace": env.Namespace().String(), "outcome": "success"})

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
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
	s.Positive(completedEventIdx)

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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

	var noe *temporal.NexusOperationError
	s.ErrorAs(wee, &noe)
	s.Contains(noe.Error(), "test operation failed")
}

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletionErrors(chasmEnabled bool) {
	ctx := testcore.NewContext()

	s.Run("NamespaceNotFound", func(s *NexusWorkflowTestSuite) {
		env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
		// Generate a token with a non-existent namespace ID
		tokenWithBadNamespace, err := s.generateValidCallbackToken("namespace-doesnt-exist-id", testcore.RandomizeStr("workflow"), uuid.NewString())
		s.NoError(err)

		publicCallbackURL := "http://" + env.HttpAPIAddress() + "/" + commonnexus.RouteCompletionCallback.Path("namespace-doesnt-exist")
		completion := nexusrpc.CompleteOperationOptions{
			Result: testcore.MustToPayload(s.T(), "result"),
			Header: nexus.Header{commonnexus.CallbackTokenHeader: tokenWithBadNamespace},
		}
		capture := env.StartGlobalMetricCapture()
		err = s.sendNexusCompletionRequest(ctx, publicCallbackURL, completion)
		var handlerErr *nexus.HandlerError
		s.ErrorAs(err, &handlerErr)
		s.Equal(nexus.HandlerErrorTypeNotFound, handlerErr.Type)
		s.Len(capture.Metric("nexus_completion_request_preprocess_errors"), 1)
	})

	s.Run("NamespaceNotFoundNoIdentifier", func(s *NexusWorkflowTestSuite) {
		env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
		// Generate a token with a non-existent namespace ID
		tokenWithBadNamespace, err := s.generateValidCallbackToken("namespace-doesnt-exist-id", testcore.RandomizeStr("workflow"), uuid.NewString())
		s.NoError(err)

		publicCallbackURL := "http://" + env.HttpAPIAddress() + commonnexus.PathCompletionCallbackNoIdentifier
		completion := nexusrpc.CompleteOperationOptions{
			Result: testcore.MustToPayload(s.T(), "result"),
			Header: nexus.Header{commonnexus.CallbackTokenHeader: tokenWithBadNamespace},
		}
		capture := env.StartGlobalMetricCapture()
		err = s.sendNexusCompletionRequest(ctx, publicCallbackURL, completion)
		var handlerErr *nexus.HandlerError
		s.ErrorAs(err, &handlerErr)
		s.Equal(nexus.HandlerErrorTypeNotFound, handlerErr.Type)
		s.Len(capture.Metric("nexus_completion_request_preprocess_errors"), 1)
	})

	s.Run("OperationTokenTooLong", func(s *NexusWorkflowTestSuite) {
		env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
		publicCallbackURL := "http://" + env.HttpAPIAddress() + "/" + commonnexus.RouteCompletionCallback.Path(env.Namespace().String())

		// Generate a valid callback token to get past initial validation
		namespaceID := env.NamespaceID().String()
		validToken, err := s.generateValidCallbackToken(namespaceID, testcore.RandomizeStr("workflow"), uuid.NewString())
		s.NoError(err)
		completion := nexusrpc.CompleteOperationOptions{
			Result:         testcore.MustToPayload(s.T(), "result"),
			OperationToken: strings.Repeat("long", 2000),
			Header:         nexus.Header{commonnexus.CallbackTokenHeader: validToken},
		}

		capture := env.StartGlobalMetricCapture()
		namespaceCapture := env.StartNamespaceMetricCapture()
		err = s.sendNexusCompletionRequest(ctx, publicCallbackURL, completion)
		completionRequests := namespaceCapture.Metric("nexus_completion_requests")
		var handlerErr *nexus.HandlerError
		s.ErrorAs(err, &handlerErr)
		s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		s.Empty(capture.Metric("nexus_completion_request_preprocess_errors"))
		s.Len(completionRequests, 1)
		s.Subset(completionRequests[0].Tags, map[string]string{"namespace": env.Namespace().String(), "outcome": "error_bad_request"})
	})

	s.Run("OperationTokenTooLongNoIdentifier", func(s *NexusWorkflowTestSuite) {
		env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
		publicCallbackURL := "http://" + env.HttpAPIAddress() + commonnexus.PathCompletionCallbackNoIdentifier
		// Generate a valid callback token to get past initial validation
		namespaceID := env.NamespaceID().String()
		validToken, err := s.generateValidCallbackToken(namespaceID, testcore.RandomizeStr("workflow"), uuid.NewString())
		s.NoError(err)

		completion := nexusrpc.CompleteOperationOptions{
			Result:         testcore.MustToPayload(s.T(), "result"),
			OperationToken: strings.Repeat("long", 2000),
			Header:         nexus.Header{commonnexus.CallbackTokenHeader: validToken},
		}

		capture := env.StartGlobalMetricCapture()
		namespaceCapture := env.StartNamespaceMetricCapture()
		err = s.sendNexusCompletionRequest(ctx, publicCallbackURL, completion)
		completionRequests := namespaceCapture.Metric("nexus_completion_requests")
		var handlerErr *nexus.HandlerError
		s.ErrorAs(err, &handlerErr)
		s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		s.Empty(capture.Metric("nexus_completion_request_preprocess_errors"))
		s.Len(completionRequests, 1)
		s.Subset(completionRequests[0].Tags, map[string]string{"namespace": env.Namespace().String(), "outcome": "error_bad_request"})
	})

	s.Run("InvalidCallbackToken", func(s *NexusWorkflowTestSuite) {
		env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
		completion := nexusrpc.CompleteOperationOptions{
			Result: testcore.MustToPayload(s.T(), "result"),
		}
		publicCallbackURL := "http://" + env.HttpAPIAddress() + "/" + commonnexus.RouteCompletionCallback.Path(env.Namespace().String())
		// metrics collection is not initialized before callback validation
		// Send request without callback token, helper does not add token if blank
		err := s.sendNexusCompletionRequest(ctx, publicCallbackURL, completion)
		// Verify we get the correct error response
		var handlerErr *nexus.HandlerError
		s.ErrorAs(err, &handlerErr)
		s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		s.Contains(handlerErr.Error(), "invalid callback token", "Response should indicate invalid callback token")
	})

	s.Run("InvalidCallbackTokenNoIdentifier", func(s *NexusWorkflowTestSuite) {
		env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
		completion := nexusrpc.CompleteOperationOptions{
			Result: testcore.MustToPayload(s.T(), "result"),
		}
		publicCallbackURL := "http://" + env.HttpAPIAddress() + commonnexus.PathCompletionCallbackNoIdentifier
		// metrics collection is not initialized before callback validation
		// Send request without callback token, helper does not add token if blank
		err := s.sendNexusCompletionRequest(ctx, publicCallbackURL, completion)
		// Verify we get the correct error response
		var handlerErr *nexus.HandlerError
		s.ErrorAs(err, &handlerErr)
		s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		s.Contains(handlerErr.Error(), "invalid callback token", "Response should indicate invalid callback token")
	})

	s.Run("InvalidClientVersion", func(s *NexusWorkflowTestSuite) {
		env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
		publicCallbackURL := "http://" + env.HttpAPIAddress() + "/" + commonnexus.RouteCompletionCallback.Path(env.Namespace().String())
		capture := env.StartNamespaceMetricCapture()

		// Generate a valid callback token to get past initial validation
		namespaceID := env.NamespaceID().String()
		validToken, err := s.generateValidCallbackToken(namespaceID, testcore.RandomizeStr("workflow"), uuid.NewString())
		s.NoError(err)

		completion := nexusrpc.CompleteOperationOptions{
			Result: testcore.MustToPayload(s.T(), "result"),
			Header: nexus.Header{
				commonnexus.CallbackTokenHeader: validToken,
				"user-agent":                    "Nexus-go-sdk/v99.0.0",
			},
		}
		client := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
			Serializer: commonnexus.PayloadSerializer,
		})
		err = client.CompleteOperation(ctx, publicCallbackURL, completion)
		completionRequests := capture.Metric("nexus_completion_requests")
		var handlerErr *nexus.HandlerError
		s.ErrorAs(err, &handlerErr)
		s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		s.Len(completionRequests, 1)
		s.Subset(completionRequests[0].Tags, map[string]string{"namespace": env.Namespace().String(), "outcome": "unsupported_client"})
	})

	s.Run("InvalidClientVersionNoIdentifier", func(s *NexusWorkflowTestSuite) {
		env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
		publicCallbackURL := "http://" + env.HttpAPIAddress() + commonnexus.PathCompletionCallbackNoIdentifier
		capture := env.StartNamespaceMetricCapture()

		// Generate a valid callback token to get past initial validation
		namespaceID := env.NamespaceID().String()
		validToken, err := s.generateValidCallbackToken(namespaceID, testcore.RandomizeStr("workflow"), uuid.NewString())
		s.NoError(err)

		completion := nexusrpc.CompleteOperationOptions{
			Result: testcore.MustToPayload(s.T(), "result"),
			Header: nexus.Header{
				commonnexus.CallbackTokenHeader: validToken,
				"user-agent":                    "Nexus-go-sdk/v99.0.0",
			},
		}

		client := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
			Serializer: commonnexus.PayloadSerializer,
		})
		err = client.CompleteOperation(ctx, publicCallbackURL, completion)
		completionRequests := capture.Metric("nexus_completion_requests")
		var handlerErr *nexus.HandlerError
		s.ErrorAs(err, &handlerErr)
		s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
		s.Len(completionRequests, 1)
		s.Subset(completionRequests[0].Tags, map[string]string{"namespace": env.Namespace().String(), "outcome": "unsupported_client"})
	})
}

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletionAuthErrors(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
	ctx := testcore.NewContext()

	onAuthorize := func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
		if ct.APIName == configs.CompleteNexusOperation {
			return authorization.Result{Decision: authorization.DecisionDeny, Reason: "unauthorized in test"}, nil
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	}
	env.GetTestCluster().Host().SetOnAuthorize(onAuthorize)
	defer env.GetTestCluster().Host().SetOnAuthorize(nil)

	// Generate a valid callback token for testing
	namespaceID := env.NamespaceID().String()
	callbackToken, err := s.generateValidCallbackToken(namespaceID, testcore.RandomizeStr("workflow"), uuid.NewString())
	s.NoError(err)

	completion := nexusrpc.CompleteOperationOptions{
		Result: testcore.MustToPayload(s.T(), "result"),
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callbackToken},
	}

	publicCallbackURL := "http://" + env.HttpAPIAddress() + "/" + commonnexus.RouteCompletionCallback.Path(env.Namespace().String())
	capture := env.StartNamespaceMetricCapture()
	err = s.sendNexusCompletionRequest(ctx, publicCallbackURL, completion)
	completionRequests := capture.Metric("nexus_completion_requests")
	var handlerErr *nexus.HandlerError
	s.ErrorAs(err, &handlerErr)
	s.Equal(nexus.HandlerErrorTypeUnauthorized, handlerErr.Type)
	s.Len(completionRequests, 1)
	s.Subset(completionRequests[0].Tags, map[string]string{"namespace": env.Namespace().String(), "outcome": "unauthorized"})
}

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletionAuthErrorsNoIdentifier(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
	ctx := testcore.NewContext()

	onAuthorize := func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
		if ct.APIName == configs.CompleteNexusOperation {
			return authorization.Result{Decision: authorization.DecisionDeny, Reason: "unauthorized in test"}, nil
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	}
	env.GetTestCluster().Host().SetOnAuthorize(onAuthorize)
	defer env.GetTestCluster().Host().SetOnAuthorize(nil)

	// Generate a valid callback token for testing
	namespaceID := env.NamespaceID().String()
	callbackToken, err := s.generateValidCallbackToken(namespaceID, testcore.RandomizeStr("workflow"), uuid.NewString())
	s.NoError(err)

	completion := nexusrpc.CompleteOperationOptions{
		Result: testcore.MustToPayload(s.T(), "result"),
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callbackToken},
	}
	publicCallbackURL := "http://" + env.HttpAPIAddress() + commonnexus.PathCompletionCallbackNoIdentifier
	capture := env.StartNamespaceMetricCapture()
	err = s.sendNexusCompletionRequest(ctx, publicCallbackURL, completion)
	completionRequests := capture.Metric("nexus_completion_requests")
	var handlerErr *nexus.HandlerError
	s.ErrorAs(err, &handlerErr)
	s.Equal(nexus.HandlerErrorTypeUnauthorized, handlerErr.Type)
	s.Len(completionRequests, 1)
	s.Subset(completionRequests[0].Tags, map[string]string{"namespace": env.Namespace().String(), "outcome": "unauthorized"})
}

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletionInternalAuth(chasmEnabled bool) {
	if chasmEnabled {
		s.T().Skip("Blocked on CHASM Nexus async completion and internal auth callback support")
	}
	env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
	// Set URL template with invalid host
	env.OverrideDynamicConfig(
		nexusoperations.CallbackURLTemplate,
		"http://INTERNAL/namespaces/{{.NamespaceName}}/nexus/callback")

	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	_, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: env.Namespace().String(),
						TaskQueue: taskQueue,
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	completionWFType := "completion_wf"
	completionWFTaskQueue := testcore.RandomizeStr(s.T().Name())
	completionWFStartReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         testcore.RandomizeStr(s.T().Name()),
		WorkflowType:       &commonpb.WorkflowType{Name: completionWFType},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: completionWFTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           "test",
	}

	pollerErrCh := env.nexusTaskPoller(ctx, s.T(), taskQueue, func(t *testing.T, res *workflowservice.PollNexusTaskQueueResponse) (*nexusTaskResponse, error) {
		start := res.Request.Variant.(*nexuspb.Request_StartOperation).StartOperation
		require.Equal(t, op.Name(), start.Operation)

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

		_, err := env.FrontendClient().StartWorkflowExecution(ctx, completionWFStartReq)
		if err != nil {
			return nil, err
		}

		return &nexusTaskResponse{StartResult: &nexus.HandlerStartOperationResultAsync{OperationToken: "test-token"}}, nil
	})

	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
						Input:     testcore.MustToPayload(s.T(), "input"),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and verify that the "started" event was recorded.
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)
	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Positive(startedEventIdx)

	// Complete workflow containing callback
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: completionWFTaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{
								testcore.MustToPayload(s.T(), "result"),
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
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
	s.Positive(completedEventIdx)

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
	s.NoError(<-pollerErrCh)
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)
}

func (s *NexusWorkflowTestSuite) TestNexusOperationCancelBeforeStarted_CancelationEventuallyDelivered(chasmEnabled bool) {
	if chasmEnabled {
		s.T().Skip("Blocked on CHASM Nexus cancellation before start support")
	}
	env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
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

	_, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
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

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
						Input:     testcore.MustToPayload(s.T(), "input"),
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
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
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
	s.Positive(scheduledEventIdx)
	scheduledEventID := pollResp.History.Events[scheduledEventIdx].EventId

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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

	env.SendToChannel(ctx, canStartCh)
	env.WaitForChannel(ctx, cancelSentCh)

	// Terminate the workflow for good measure.
	err = env.SdkClient().TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "test")
	s.NoError(err)

	// Assert that we did not send a cancel request until after the operation was started.
	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	})
	s.ContainsHistoryEvents(`
NexusOperationCancelRequested
NexusOperationStarted`, hist)
}

func (s *NexusWorkflowTestSuite) TestNexusOperationAsyncCompletionAfterReset(chasmEnabled bool) {
	if chasmEnabled {
		s.T().Skip("Blocked on CHASM Nexus async completion after reset support")
	}
	env := s.newTestEnv(chasmEnabled)
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

	_, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
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

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
						Input:     testcore.MustToPayload(s.T(), "input"),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll and verify that the "started" event was recorded.
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Positive(startedEventIdx)

	// Remember the workflow task completed event ID (next after the last WFT started), we'll use it to test reset
	// below.
	wftCompletedEventID := int64(len(pollResp.History.Events))

	// Reset the workflow and check that the started event has been reapplied.
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         pollResp.WorkflowExecution,
		Reason:                    "test",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
	})
	s.NoError(err)

	hist := env.SdkClient().GetWorkflowHistory(ctx, run.GetID(), resetResp.RunId, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

	seenStartedEvent := false
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED {
			seenStartedEvent = true
		}
	}
	s.True(seenStartedEvent)
	completion := nexusrpc.CompleteOperationOptions{
		Result: testcore.MustToPayload(s.T(), "result"),
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callbackToken},
	}
	err = s.sendNexusCompletionRequest(ctx, publicCallbackUrl, completion)
	s.NoError(err)

	// Poll again and verify the completion is recorded and triggers workflow progress.
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
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
	s.Positive(completedEventIdx)

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
	run = env.SdkClient().GetWorkflow(ctx, run.GetID(), resetResp.RunId)
	s.NoError(run.Get(ctx, &result))
	s.Equal("result", result)
}

func (s *NexusWorkflowTestSuite) TestNexusAsyncOperationWithNilIO(chasmEnabled bool) {
	if chasmEnabled {
		s.T().Skip("Blocked on CHASM Nexus async completion with nil IO support")
	}
	env := s.newTestEnv(chasmEnabled)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	callerTaskQueue := testcore.RandomizeStr("caller_" + s.T().Name())
	handlerWorkflowTaskQueue := testcore.RandomizeStr("handler_" + s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	handlerWorkflowID := testcore.RandomizeStr(s.T().Name())

	_, err := env.SdkClient().OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: env.Namespace().String(),
						TaskQueue: callerTaskQueue,
					},
				},
			},
		},
	})
	s.NoError(err)

	w := worker.New(
		env.SdkClient(),
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

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: callerTaskQueue,
	}, callerWF, nil)
	s.NoError(err)

	pollRes, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: handlerWorkflowTaskQueue,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: env.Namespace().String(),
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
	history := env.SdkClient().GetWorkflowHistory(ctx, run.GetID(), "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for history.HasNext() {
		ev, err := history.Next()
		s.NoError(err)
		if attr := ev.GetNexusOperationCompletedEventAttributes(); attr != nil {
			protorequire.ProtoEqual(s.T(), testcore.MustToPayload(s.T(), nil), attr.GetResult())
			break
		}
	}
}

func (s *NexusWorkflowTestSuite) TestNexusSyncOperationErrorRehydration(chasmEnabled bool) {
	type testcase struct {
		outcome            string
		metricsOutcome     string
		checkPendingError  func(s *NexusWorkflowTestSuite, pendingErr error)
		checkWorkflowError func(s *NexusWorkflowTestSuite, wfErr error)
	}
	cases := []testcase{
		{
			outcome:        "fail-handler-internal",
			metricsOutcome: "handler-error:INTERNAL",
			checkPendingError: func(s *NexusWorkflowTestSuite, pendingErr error) {
				var handlerErr *nexus.HandlerError
				s.ErrorAs(pendingErr, &handlerErr)
				s.Equal(nexus.HandlerErrorTypeInternal, handlerErr.Type)
				s.Equal("intentional internal error", handlerErr.Message)
			},
		},
		{
			outcome:        "fail-handler-app-error",
			metricsOutcome: "handler-error:INTERNAL",
			checkPendingError: func(s *NexusWorkflowTestSuite, pendingErr error) {
				var handlerErr *nexus.HandlerError
				s.ErrorAs(pendingErr, &handlerErr)
				s.Equal(nexus.HandlerErrorTypeInternal, handlerErr.Type)
				var appErr *temporal.ApplicationError
				s.ErrorAs(handlerErr.Cause, &appErr)
				s.Equal("app error", appErr.Message())
				s.Equal("TestError", appErr.Type())
				var details string
				s.NoError(appErr.Details(&details))
				s.Equal("details", details)
			},
		},
		{
			outcome:        "fail-handler-bad-request",
			metricsOutcome: "handler-error:BAD_REQUEST",
			checkWorkflowError: func(s *NexusWorkflowTestSuite, wfErr error) {
				var opErr *temporal.NexusOperationError
				s.ErrorAs(wfErr, &opErr)
				var handlerErr *nexus.HandlerError
				s.ErrorAs(opErr, &handlerErr)
				s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
				s.Equal("bad request", handlerErr.Message)
			},
		},
		{
			outcome:        "fail-operation",
			metricsOutcome: "operation-unsuccessful:failed",
			checkWorkflowError: func(s *NexusWorkflowTestSuite, wfErr error) {
				var opErr *temporal.NexusOperationError
				s.ErrorAs(wfErr, &opErr)
				s.Equal("nexus operation completed unsuccessfully", opErr.Message)
				var appErr *temporal.ApplicationError
				s.ErrorAs(opErr.Cause, &appErr)
				s.Equal("some error", appErr.Message())
			},
		},
		{
			outcome:        "fail-operation-app-error",
			metricsOutcome: "handler-error:INTERNAL",
			checkWorkflowError: func(s *NexusWorkflowTestSuite, wfErr error) {
				var opErr *temporal.NexusOperationError
				s.ErrorAs(wfErr, &opErr)
				var appErr *temporal.ApplicationError
				s.ErrorAs(opErr, &appErr)
				s.Equal("app error", appErr.Message())
				s.Equal("TestError", appErr.Type())
				var details string
				s.NoError(appErr.Details(&details))
				s.Equal("details", details)
			},
		},
	}

	testFn := func(s *NexusWorkflowTestSuite, tc testcase) {
		env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
		defer cancel()
		taskQueue := testcore.RandomizeStr("caller_" + s.T().Name())
		endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
		converter := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{})

		_, err := env.SdkClient().OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: endpointName,
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_Worker_{
						Worker: &nexuspb.EndpointTarget_Worker{
							Namespace: env.Namespace().String(),
							TaskQueue: taskQueue,
						},
					},
				},
			},
		})
		s.NoError(err)

		w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
		svc := nexus.NewService("test")
		op := nexus.NewSyncOperation("op", func(ctx context.Context, outcome string, soo nexus.StartOperationOptions) (nexus.NoValue, error) {
			switch outcome {
			case "fail-handler-internal":
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional internal error")
			case "fail-handler-app-error":
				return nil, temporal.NewApplicationError("app error", "TestError", "details")
			case "fail-handler-bad-request":
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "bad request")
			case "fail-operation":
				return nil, nexus.NewOperationFailedErrorf("some error")
			case "fail-operation-app-error":
				return nil, temporal.NewNonRetryableApplicationError("app error", "TestError", nil, "details")
			default:
			}
			return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "unexpected outcome: %s", outcome)
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
		defer w.Stop()

		capture := env.StartNamespaceMetricCapture()
		run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			TaskQueue: taskQueue,
		}, callerWF, tc.outcome)
		s.NoError(err)

		if tc.checkPendingError != nil {
			var f *failurepb.Failure
			s.EventuallyWithT(func(t *assert.CollectT) {
				desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
				require.NoError(t, err)
				require.Len(t, desc.PendingNexusOperations, 1)
				if len(desc.PendingNexusOperations) > 0 {
					f = desc.PendingNexusOperations[0].LastAttemptFailure
					require.NotNil(t, f)
				}
			}, 10*time.Second, 100*time.Millisecond)
			tc.checkPendingError(s, converter.FailureToError(f))
			s.NoError(env.SdkClient().TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "test cleanup"))
		} else {
			wfErr := run.Get(ctx, nil)
			tc.checkWorkflowError(s, wfErr)
		}

		if !chasmEnabled {
			// TODO: Assert this for CHASM once sync error rehydration emits nexus_outbound_requests metrics there.
			outboundRequests := capture.Metric("nexus_outbound_requests")
			s.Len(outboundRequests, 1)
			s.Subset(
				outboundRequests[0].Tags,
				map[string]string{
					"namespace":      env.Namespace().String(),
					"method":         "StartOperation",
					"failure_source": "worker",
					"outcome":        tc.metricsOutcome,
				},
			)
		}
	}

	for _, tc := range cases {
		s.Run(tc.outcome, func(s *NexusWorkflowTestSuite) {
			testFn(s, tc)
		})
	}
}

func (s *NexusWorkflowTestSuite) TestNexusAsyncOperationErrorRehydration(chasmEnabled bool) {
	if chasmEnabled {
		s.T().Skip("Blocked on CHASM Nexus async error rehydration support")
	}
	type testcase struct {
		outcome, action    string
		checkWorkflowError func(s *NexusWorkflowTestSuite, wfErr error)
	}
	cases := []testcase{
		{
			outcome: "fail",
			checkWorkflowError: func(s *NexusWorkflowTestSuite, wfErr error) {
				var opErr *temporal.NexusOperationError
				s.ErrorAs(wfErr, &opErr)
				var appErr *temporal.ApplicationError
				s.ErrorAs(opErr, &appErr)
				s.Equal("app error", appErr.Message())
				s.Equal("TestError", appErr.Type())
				var details string
				s.NoError(appErr.Details(&details))
				s.Equal("details", details)
			},
		},
		{
			outcome: "wait",
			action:  "terminate",
			checkWorkflowError: func(s *NexusWorkflowTestSuite, wfErr error) {
				var opErr *temporal.NexusOperationError
				s.ErrorAs(wfErr, &opErr)
				var termErr *temporal.TerminatedError
				s.ErrorAs(opErr, &termErr)
			},
		},
		{
			outcome: "wait",
			action:  "cancel",
			checkWorkflowError: func(s *NexusWorkflowTestSuite, wfErr error) {
				// The Go SDK loses the NexusOperationError (as well as any other error if it wraps a CanceledError),
				// assertions done in workflow.
				var canceledErr *temporal.CanceledError
				s.ErrorAs(wfErr, &canceledErr)
			},
		},
		{
			outcome: "timeout",
			checkWorkflowError: func(s *NexusWorkflowTestSuite, wfErr error) {
				var opErr *temporal.NexusOperationError
				s.ErrorAs(wfErr, &opErr)
				var timeoutErr *temporal.TimeoutError
				s.ErrorAs(opErr, &timeoutErr)
			},
		},
	}

	testFn := func(s *NexusWorkflowTestSuite, tc testcase) {
		env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
		defer cancel()
		testCtx := ctx
		taskQueue := testcore.RandomizeStr("caller_" + s.T().Name())
		endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
		handlerWorkflowID := testcore.RandomizeStr(s.T().Name())

		_, err := env.SdkClient().OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: endpointName,
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_Worker_{
						Worker: &nexuspb.EndpointTarget_Worker{
							Namespace: env.Namespace().String(),
							TaskQueue: taskQueue,
						},
					},
				},
			},
		})
		s.NoError(err)

		w := worker.New(env.SdkClient(), taskQueue, worker.Options{})
		svc := nexus.NewService("test")

		handlerWF := func(ctx workflow.Context, outcome string) (nexus.NoValue, error) {
			switch outcome {
			case "wait", "timeout":
				// Wait for the workflow to be canceled.
				return nil, workflow.Await(ctx, func() bool { return false })
			case "fail":
				return nil, temporal.NewApplicationError("app error", "TestError", "details")
			default:
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
					err := env.SdkClient().TerminateWorkflow(testCtx, handlerWorkflowID, "", "")
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
			default:
			}
			return nil, fut.Get(ctx, nil)
		}

		w.RegisterNexusService(svc)
		w.RegisterWorkflow(callerWF)
		w.RegisterWorkflow(handlerWF)
		s.NoError(w.Start())
		defer w.Stop()

		capture := env.StartNamespaceMetricCapture()
		run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			TaskQueue: taskQueue,
		}, callerWF, tc.outcome, tc.action)
		s.NoError(err)

		wfErr := run.Get(ctx, nil)
		tc.checkWorkflowError(s, wfErr)

		outboundRequests := capture.Metric("nexus_outbound_requests")
		s.GreaterOrEqual(len(outboundRequests), 1)
		s.Subset(outboundRequests[0].Tags, map[string]string{"namespace": env.Namespace().String(), "method": "StartOperation", "failure_source": "_unknown_", "outcome": "pending"})
	}

	for _, tc := range cases {
		s.Run(tc.outcome+"-"+tc.action, func(s *NexusWorkflowTestSuite) {
			testFn(s, tc)
		})
	}
}

func (s *NexusWorkflowTestSuite) TestNexusCallbackAfterCallerComplete(chasmEnabled bool) {
	if chasmEnabled {
		s.T().Skip("Blocked on CHASM Nexus callback failure handling after caller completion")
	}
	env := s.newTestEnv(chasmEnabled)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	taskQueue := testcore.RandomizeStr("caller_" + s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	handlerWorkflowID := testcore.RandomizeStr(s.T().Name())

	_, err := env.SdkClient().OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: env.Namespace().String(),
						TaskQueue: taskQueue,
					},
				},
			},
		},
	})
	s.NoError(err)

	w := worker.New(
		env.SdkClient(),
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

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, callerWF)
	s.NoError(err)
	s.NoError(run.Get(ctx, nil))

	err = env.SdkClient().SignalWorkflow(ctx, handlerWorkflowID, "", "test-signal", nil)
	s.NoError(err)

	s.EventuallyWithT(func(ct *assert.CollectT) {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
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

func (s *NexusWorkflowTestSuite) TestNexusOperationSyncNexusFailure(chasmEnabled bool) {
	if chasmEnabled {
		s.T().Skip("Blocked on CHASM Nexus sync failure conversion support")
	}
	env := s.newTestEnv(chasmEnabled, testcore.WithDedicatedCluster())
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

	_, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
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
		env.SdkClient(),
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

	capture := env.StartNamespaceMetricCapture()
	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, callerWF)
	s.NoError(err)
	wfErr := run.Get(ctx, nil)

	var handlerErr *nexus.HandlerError
	s.ErrorAs(wfErr, &handlerErr)
	s.Equal(nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
	// Old SDK path
	var appErr *temporal.ApplicationError
	s.ErrorAs(handlerErr.Cause, &appErr)
	s.Equal("fail me", appErr.Message())
	var failure nexus.Failure
	s.NoError(appErr.Details(&failure))
	s.Equal(map[string]string{"key": "val"}, failure.Metadata)
	var details string
	s.NoError(json.Unmarshal(failure.Details, &details))
	s.Equal("details", details)

	outboundRequests := capture.Metric("nexus_outbound_requests")
	s.Len(outboundRequests, 1)
	// Confirming that requests which do not go through our frontend are not tagged with `failure_source`
	s.Subset(outboundRequests[0].Tags, map[string]string{"namespace": env.Namespace().String(), "method": "StartOperation", "failure_source": "_unknown_", "outcome": "handler-error:BAD_REQUEST"})
}

func (s *NexusWorkflowTestSuite) TestNexusAsyncOperationWithMultipleCallers(chasmEnabled bool) {
	if chasmEnabled {
		s.T().Skip("Blocked on CHASM Nexus async completion with multiple callers support")
	}
	// number of concurrent Nexus operation calls
	numCalls := 5
	handlerWf := func(ctx workflow.Context, input string) (string, error) {
		workflow.GetSignalChannel(ctx, "terminate").Receive(ctx, nil)
		return "hello " + input, nil
	}
	handlerWorkflowID := testcore.RandomizeStr(s.T().Name())
	callerTaskQueue := testcore.RandomizeStr("caller_" + s.T().Name())

	type CallerWfOutput struct {
		CntOk  int
		CntErr int
	}
	type CallerWfFn = func(ctx workflow.Context, input string) (CallerWfOutput, error)

	buildNexusEnvFn := func(ctx context.Context, s *NexusWorkflowTestSuite) (*NexusTestEnv, CallerWfFn) {
		env := s.newTestEnv(chasmEnabled)
		endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

		_, err := env.SdkClient().OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: endpointName,
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_Worker_{
						Worker: &nexuspb.EndpointTarget_Worker{
							Namespace: env.Namespace().String(),
							TaskQueue: callerTaskQueue,
						},
					},
				},
			},
		})
		s.NoError(err)

		w := worker.New(env.SdkClient(), callerTaskQueue, worker.Options{})
		svc := nexus.NewService("test")
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

		callerWf := func(ctx workflow.Context, input string) (CallerWfOutput, error) {
			output := CallerWfOutput{}
			var retError error

			c := workflow.NewNexusClient(endpointName, svc.Name)

			nexusFutures := []workflow.NexusOperationFuture{}
			for range numCalls {
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
				if !errors.As(err, &handlerErr) || !errors.As(handlerErr, &appErr) || appErr.Type() != "WorkflowExecutionAlreadyStarted" {
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

		// s.T().Cleanup(...) runs after the s.T()'s test finishes, not after this function returns
		s.T().Cleanup(func() { w.Stop() })
		return env, callerWf
	}

	testCases := []struct {
		input       string
		checkOutput func(s *NexusWorkflowTestSuite, env *NexusTestEnv, res CallerWfOutput, err error)
	}{
		{
			input: "conflict-policy-fail",
			checkOutput: func(s *NexusWorkflowTestSuite, env *NexusTestEnv, res CallerWfOutput, err error) {
				s.NoError(err)
				s.Equal(1, res.CntOk)
				s.Equal(numCalls-1, res.CntErr)

				// check the handler workflow has the request ID infos map correct
				descResp, err := env.SdkClient().DescribeWorkflowExecution(context.Background(), handlerWorkflowID, "")
				s.NoError(err)
				requestIDInfos := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()
				s.NotNil(requestIDInfos)
				s.Len(requestIDInfos, 1)
				for _, info := range requestIDInfos {
					s.False(info.Buffered)
					s.GreaterOrEqual(info.EventId, common.FirstEventID)
					s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, info.EventType)
				}
			},
		},
		{
			input: "conflict-policy-use-existing",
			checkOutput: func(s *NexusWorkflowTestSuite, env *NexusTestEnv, res CallerWfOutput, err error) {
				s.NoError(err)
				s.Equal(numCalls, res.CntOk)
				s.Equal(0, res.CntErr)

				// check the handler workflow has the request ID infos map correct
				descResp, err := env.SdkClient().DescribeWorkflowExecution(context.Background(), handlerWorkflowID, "")
				s.NoError(err)
				requestIDInfos := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()
				s.NotNil(requestIDInfos)
				cntStarted := 0
				cntAttached := 0
				for _, info := range requestIDInfos {
					s.False(info.Buffered)
					s.GreaterOrEqual(info.EventId, common.FirstEventID)
					switch info.EventType {
					case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
						cntStarted++
					case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED:
						cntAttached++
					default:
						s.Fail("Unexpected event type in request ID info")
					}
				}
				s.Equal(1, cntStarted)
				s.Equal(numCalls-1, cntAttached)
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.input, func(s *NexusWorkflowTestSuite) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
			defer cancel()
			env, callerWf := buildNexusEnvFn(ctx, s)
			run, err := env.SdkClient().ExecuteWorkflow(
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
			tc.checkOutput(s, env, res, err)
		})
	}
}

func (s *NexusWorkflowTestSuite) TestNexusOperationScheduleToCloseTimeout(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled)
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	_, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: env.Namespace().String(),
						TaskQueue: "unreachable-for-test",
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	// Schedule the operation with a short schedule-to-close timeout
	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:               endpointName,
						Service:                "service",
						Operation:              "operation",
						Input:                  testcore.MustToPayload(s.T(), "input"),
						ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
					},
				},
			},
		},
	})
	s.NoError(err)

	descResp, err := env.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
	s.NoError(err)
	s.Len(descResp.PendingNexusOperations, 1)
	s.Equal(2*time.Second, descResp.PendingNexusOperations[0].ScheduleToCloseTimeout.AsDuration())

	// Now wait for the timeout event
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)

	// Verify we got a timeout event with the correct timeout type
	timedOutEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationTimedOutEventAttributes() != nil
	})
	s.Positive(timedOutEventIdx)
	timedOutEvent := pollResp.History.Events[timedOutEventIdx]
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		timedOutEvent.GetNexusOperationTimedOutEventAttributes().GetFailure().GetCause().GetTimeoutFailureInfo().GetTimeoutType())

	// Complete the workflow
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
				},
			},
		},
	})
	s.NoError(err)
	s.NoError(run.Get(ctx, nil))
}

func (s *NexusWorkflowTestSuite) TestNexusOperationScheduleToStartTimeout(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled)
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	_, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: env.Namespace().String(),
						TaskQueue: "unreachable-for-test",
					},
				},
			},
		},
	})
	s.NoError(err)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	// Schedule the operation with a short schedule-to-close timeout
	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:               endpointName,
						Service:                "service",
						Operation:              "operation",
						Input:                  testcore.MustToPayload(s.T(), "input"),
						ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					},
				},
			},
		},
	})
	s.NoError(err)

	descResp, err := env.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
	s.NoError(err)
	s.Len(descResp.PendingNexusOperations, 1)
	s.Equal(2*time.Second, descResp.PendingNexusOperations[0].ScheduleToStartTimeout.AsDuration())

	// Now wait for the timeout event
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)

	// Verify we got a timeout event with the correct timeout type
	timedOutEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationTimedOutEventAttributes() != nil
	})
	s.Positive(timedOutEventIdx)
	timedOutEvent := pollResp.History.Events[timedOutEventIdx]
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		timedOutEvent.GetNexusOperationTimedOutEventAttributes().GetFailure().GetCause().GetTimeoutFailureInfo().GetTimeoutType())

	// Complete the workflow
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
				},
			},
		},
	})
	s.NoError(err)
	s.NoError(run.Get(ctx, nil))
}

func (s *NexusWorkflowTestSuite) TestNexusOperationStartToCloseTimeout(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled)
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	// Handler that starts quickly (returns async) but never completes
	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			// Return async start immediately
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test-op-token"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress()
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	_, err := env.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
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

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	// Schedule the operation with a short start-to-close timeout
	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:            endpointName,
						Service:             "service",
						Operation:           "operation",
						Input:               testcore.MustToPayload(s.T(), "input"),
						StartToCloseTimeout: durationpb.New(2 * time.Second),
					},
				},
			},
		},
	})
	s.NoError(err)

	descResp, err := env.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
	s.NoError(err)
	s.Len(descResp.PendingNexusOperations, 1)
	s.Equal(2*time.Second, descResp.PendingNexusOperations[0].StartToCloseTimeout.AsDuration())

	// Wait for the started event first
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)

	// Verify we got a started event
	startedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationStartedEventAttributes() != nil
	})
	s.Positive(startedEventIdx)

	// Respond to acknowledge the started event
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
	})
	s.NoError(err)

	// Now wait for the timeout event
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)

	// Verify we got a timeout event with the correct timeout type
	timedOutEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationTimedOutEventAttributes() != nil
	})
	s.Positive(timedOutEventIdx)
	timedOutEvent := pollResp.History.Events[timedOutEventIdx]
	s.Equal(enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		timedOutEvent.GetNexusOperationTimedOutEventAttributes().GetFailure().GetCause().GetTimeoutFailureInfo().GetTimeoutType())
	s.Contains(timedOutEvent.GetNexusOperationTimedOutEventAttributes().GetFailure().GetCause().GetMessage(), "operation timed out")

	// Complete the workflow
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
				},
			},
		},
	})
	s.NoError(err)
	s.NoError(run.Get(ctx, nil))
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
	url string,
	completion nexusrpc.CompleteOperationOptions,
) error {
	c := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	return c.CompleteOperation(ctx, url, completion)
}

// NOTE: This test cannot use the SDK workflow package because there is a restriction that prevents setting the
// __temporal_system endpoint.
func (s *NexusWorkflowTestSuite) TestNexusOperationSystemEndpoint(chasmEnabled bool) {
	env := s.newTestEnv(chasmEnabled)
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueue,
	}, "workflow")
	s.NoError(err)

	pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  commonnexus.SystemEndpoint,
						Service:   "TestService",
						Operation: "TestOperation",
						Input:     testcore.MustToPayload(s.T(), "Temporal"),
					},
				},
			},
		},
	})
	s.NoError(err)

	// Poll for the completion
	pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)

	// Find the NexusOperationCompleted event
	completedEventIdx := slices.IndexFunc(pollResp.History.Events, func(e *historypb.HistoryEvent) bool {
		return e.GetNexusOperationCompletedEventAttributes() != nil
	})
	s.Positive(completedEventIdx, "Should have a NexusOperationCompleted event")

	// Verify the result contains the echoed request ID
	completedEvent := pollResp.History.Events[completedEventIdx]
	result := completedEvent.GetNexusOperationCompletedEventAttributes().Result
	s.NotNil(result)

	// Complete the workflow
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: &commonpb.Payloads{
							Payloads: []*commonpb.Payload{result},
						},
					},
				},
			},
		},
	})
	s.NoError(err)
	var response string
	s.NoError(run.Get(ctx, &response))
	s.Equal("Hello, Temporal", response)
}

func (s *NexusWorkflowTestSuite) mutateCompletionComponentRef(
	token *tokenspb.NexusOperationCompletion,
	mutate func(*persistencespb.ChasmComponentRef),
) {
	s.T().Helper()

	ref := &persistencespb.ChasmComponentRef{}
	s.NoError(ref.Unmarshal(token.GetComponentRef()))

	mutate(ref)

	var err error
	token.ComponentRef, err = ref.Marshal()
	s.NoError(err)
}
