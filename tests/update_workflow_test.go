package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/workflow/update"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func speculativeWorkflowTaskOutcomes(
	snap map[string][]*metricstest.CapturedRecording,
) (commits, rollbacks int) {
	for range snap[metrics.SpeculativeWorkflowTaskCommits.Name()] {
		commits += 1
	}
	for range snap[metrics.SpeculativeWorkflowTaskRollbacks.Name()] {
		rollbacks += 1
	}
	return
}

func clearUpdateRegistryAndAbortPendingUpdates(s testcore.Env, tv *testvars.TestVars) {
	closeShard(s, tv.WorkflowID())
}

func loseUpdateRegistryAndAbandonPendingUpdates(s testcore.Env, tv *testvars.TestVars) {
	cleanup := s.OverrideDynamicConfig(dynamicconfig.ShardFinalizerTimeout, 0)
	defer cleanup()
	closeShard(s, tv.WorkflowID())
}

func closeShard(s testcore.Env, wid string) {
	s.T().Helper()

	resp, err := s.FrontendClient().DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{
		Namespace: s.Namespace().String(),
	})
	if err != nil {
		s.T().Fatalf("Failed to describe namespace: %v", err)
	}

	s.CloseShard(resp.NamespaceInfo.Id, wid)
}

func TestWorkflowUpdateSuite(t *testing.T) {
	t.Run("EmptySpeculativeWorkflowTask_AcceptComplete", func(t *testing.T) {
		testCases := []struct {
			name     string
			useRunID bool
		}{
			{
				name:     "with RunID",
				useRunID: true,
			},
			{
				name:     "without RunID",
				useRunID: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Uses CaptureMetricsHandler which requires a dedicated cluster to avoid metric interference.
				s := testcore.NewEnv(t, testcore.WithDedicatedCluster())
				runID := mustStartWorkflow(s, s.Tv())

				tv := s.Tv()
				if tc.useRunID {
					tv = tv.WithRunID(runID)
				}

				capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
				defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

				wtHandlerCalls := 0
				wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
					wtHandlerCalls++
					switch wtHandlerCalls {
					case 1:
						// Completes first WT with empty command list.
						return nil, nil
					case 2:
						s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT events are not written to the history yet.
  6 WorkflowTaskStarted
`, task.History)
						return s.UpdateAcceptCompleteCommands(s.Tv()), nil
					default:
						s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
						return nil, nil
					}
				}

				msgHandlerCalls := 0
				msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
					msgHandlerCalls++
					switch msgHandlerCalls {
					case 1:
						return nil, nil
					case 2:
						updRequestMsg := task.Messages[0]
						updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

						s.Equal("args-value-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
						s.Equal(s.Tv().HandlerName(), updRequest.GetInput().GetName())
						s.EqualValues(5, updRequestMsg.GetEventId())

						return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
					default:
						s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
						return nil, nil
					}
				}

				//nolint:staticcheck // SA1019 TaskPoller replacement needed
				poller := &testcore.TaskPoller{
					Client:              s.FrontendClient(),
					Namespace:           s.Namespace().String(),
					TaskQueue:           s.Tv().TaskQueue(),
					WorkflowTaskHandler: wtHandler,
					MessageHandler:      msgHandler,
					Logger:              s.Logger,
					T:                   s.T(),
				}

				// Drain first WT.
				_, err := poller.PollAndProcessWorkflowTask()
				s.NoError(err)

				updateResultCh := sendUpdateNoError(s, tv)

				// Process update in workflow.
				res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
				s.NoError(err)
				s.NotNil(res.NewTask)
				updateResult := <-updateResultCh
				s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
				s.EqualValues(0, res.NewTask.ResetHistoryEventId)

				// Test non-blocking poll
				for _, waitPolicy := range []*updatepb.WaitPolicy{{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED}, nil} {
					pollUpdateResp, err := pollUpdate(s, s.Tv(), waitPolicy)
					s.NoError(err)
					s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, pollUpdateResp.Stage)
					s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), pollUpdateResp.Outcome.GetSuccess()))
					// Even if tv doesn't have RunID, it should be returned as part of UpdateRef.
					s.Equal(runID, pollUpdateResp.UpdateRef.GetWorkflowExecution().RunId)
				}

				s.Equal(2, wtHandlerCalls)
				s.Equal(2, msgHandlerCalls)

				commits, rollbacks := speculativeWorkflowTaskOutcomes(capture.Snapshot())
				s.Equal(1, commits)
				s.Equal(0, rollbacks)

				events := s.GetHistory(s.Namespace().String(), tv.WorkflowExecution())

				s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Was speculative WT...
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted // ...and events were written to the history when WT completes.
  8 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 5} // WTScheduled event which delivered update to the worker.
  9 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 8}
`, events)
			})
		}
	})

	t.Run("NotEmptySpeculativeWorkflowTask_AcceptComplete", func(t *testing.T) {
		testCases := []struct {
			name     string
			useRunID bool
		}{
			{
				name:     "with RunID",
				useRunID: true,
			},
			{
				name:     "without RunID",
				useRunID: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				s := testcore.NewEnv(t)
				runID := mustStartWorkflow(s, s.Tv())
				tv := s.Tv()
				if tc.useRunID {
					tv = tv.WithRunID(runID)
				}

				wtHandlerCalls := 0
				wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
					wtHandlerCalls++
					switch wtHandlerCalls {
					case 1:
						// Completes first WT with update unrelated command.
						return []*commandpb.Command{{
							CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
							Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             s.Tv().ActivityID(),
								ActivityType:           s.Tv().ActivityType(),
								TaskQueue:              s.Tv().TaskQueue(),
								ScheduleToCloseTimeout: s.Tv().Any().InfiniteTimeout(),
							}},
						}}, nil
					case 2:
						s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled // Speculative WFT with ActivityTaskScheduled(5) event after WorkflowTaskCompleted(4).
  7 WorkflowTaskStarted
`, task.History)
						return s.UpdateAcceptCompleteCommands(s.Tv()), nil
					default:
						s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
						return nil, nil
					}
				}

				msgHandlerCalls := 0
				msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
					msgHandlerCalls++
					switch msgHandlerCalls {
					case 1:
						return nil, nil
					case 2:
						updRequestMsg := task.Messages[0]
						updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

						s.Equal("args-value-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
						s.Equal(s.Tv().HandlerName(), updRequest.GetInput().GetName())
						s.EqualValues(6, updRequestMsg.GetEventId())

						return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
					default:
						s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
						return nil, nil
					}
				}

				//nolint:staticcheck // SA1019 TaskPoller replacement needed
				poller := &testcore.TaskPoller{
					Client:              s.FrontendClient(),
					Namespace:           s.Namespace().String(),
					TaskQueue:           s.Tv().TaskQueue(),
					Identity:            s.Tv().WorkerIdentity(),
					WorkflowTaskHandler: wtHandler,
					MessageHandler:      msgHandler,
					Logger:              s.Logger,
					T:                   s.T(),
				}

				// Drain first WT.
				_, err := poller.PollAndProcessWorkflowTask()
				s.NoError(err)

				updateResultCh := sendUpdateNoError(s, tv)

				// Process update in workflow.
				res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
				s.NoError(err)
				s.NotNil(res)
				updateResult := <-updateResultCh
				s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
				s.EqualValues(0, res.NewTask.ResetHistoryEventId)

				s.Equal(2, wtHandlerCalls)
				s.Equal(2, msgHandlerCalls)

				events := s.GetHistory(s.Namespace().String(), tv.WorkflowExecution())

				s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled // Speculative WFT was persisted when completed (event 8)
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 6} // WTScheduled event which delivered update to the worker.
 10 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 9}
`, events)
			})
		}
	})

	t.Run("FirstNormalScheduledWorkflowTask_AcceptComplete", func(t *testing.T) {
		testCases := []struct {
			name     string
			useRunID bool
		}{
			{
				name:     "with RunID",
				useRunID: true,
			},
			{
				name:     "without RunID",
				useRunID: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				s := testcore.NewEnv(t)
				runID := mustStartWorkflow(s, s.Tv())
				tv := s.Tv()
				if tc.useRunID {
					tv = tv.WithRunID(runID)
				}

				wtHandlerCalls := 0
				wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
					wtHandlerCalls++
					switch wtHandlerCalls {
					case 1:
						s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted // First normal WT. No speculative WT was created.
`, task.History)
						return s.UpdateAcceptCompleteCommands(s.Tv()), nil
					default:
						s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
						return nil, nil
					}
				}

				msgHandlerCalls := 0
				msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
					msgHandlerCalls++
					switch msgHandlerCalls {
					case 1:
						updRequestMsg := task.Messages[0]
						updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

						s.Equal("args-value-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
						s.Equal(s.Tv().HandlerName(), updRequest.GetInput().GetName())
						s.EqualValues(2, updRequestMsg.GetEventId())

						return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
					default:
						s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
						return nil, nil
					}
				}

				//nolint:staticcheck // SA1019 TaskPoller replacement needed
				poller := &testcore.TaskPoller{
					Client:              s.FrontendClient(),
					Namespace:           s.Namespace().String(),
					TaskQueue:           s.Tv().TaskQueue(),
					Identity:            s.Tv().WorkerIdentity(),
					WorkflowTaskHandler: wtHandler,
					MessageHandler:      msgHandler,
					Logger:              s.Logger,
					T:                   s.T(),
				}

				updateResultCh := sendUpdateNoError(s, tv)

				// Process update in workflow.
				res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
				s.NoError(err)
				s.NotNil(res)

				updateResult := <-updateResultCh
				s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
				s.EqualValues(0, res.NewTask.ResetHistoryEventId)

				s.Equal(1, wtHandlerCalls)
				s.Equal(1, msgHandlerCalls)

				events := s.GetHistory(s.Namespace().String(), tv.WorkflowExecution())

				s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 2} // WTScheduled event which delivered update to the worker.
  6 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 5}
`, events)
			})
		}
	})

	t.Run("NormalScheduledWorkflowTask_AcceptComplete", func(t *testing.T) {
		testCases := []struct {
			name     string
			useRunID bool
		}{
			{
				name:     "with RunID",
				useRunID: true,
			},
			{
				name:     "without RunID",
				useRunID: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				s := testcore.NewEnv(t)
				runID := mustStartWorkflow(s, s.Tv())
				tv := s.Tv()
				if tc.useRunID {
					tv = tv.WithRunID(runID)
				}

				wtHandlerCalls := 0
				wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
					wtHandlerCalls++
					switch wtHandlerCalls {
					case 1:
						// Completes first WT with empty command list.
						return nil, nil
					case 2:
						s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled // This WT was already created by signal and no speculative WT was created.
  7 WorkflowTaskStarted`, task.History)
						return s.UpdateAcceptCompleteCommands(s.Tv()), nil
					default:
						s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
						return nil, nil
					}
				}

				msgHandlerCalls := 0
				msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
					msgHandlerCalls++
					switch msgHandlerCalls {
					case 1:
						return nil, nil
					case 2:
						s.Require().NotEmpty(task.Messages, "Task has no messages", task)
						updRequestMsg := task.Messages[0]
						updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

						s.Equal("args-value-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
						s.Equal(s.Tv().HandlerName(), updRequest.GetInput().GetName())
						s.EqualValues(6, updRequestMsg.GetEventId())

						return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
					default:
						s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
						return nil, nil
					}
				}

				//nolint:staticcheck // SA1019 TaskPoller replacement needed
				poller := &testcore.TaskPoller{
					Client:              s.FrontendClient(),
					Namespace:           s.Namespace().String(),
					TaskQueue:           s.Tv().TaskQueue(),
					Identity:            s.Tv().WorkerIdentity(),
					WorkflowTaskHandler: wtHandler,
					MessageHandler:      msgHandler,
					Logger:              s.Logger,
					T:                   s.T(),
				}

				// Drain first WT.
				_, err := poller.PollAndProcessWorkflowTask()
				s.NoError(err)

				// Send signal to schedule new WT.
				err = s.SendSignal(s.Namespace().String(), s.Tv().WorkflowExecution(), s.Tv().Any().String(), s.Tv().Any().Payloads(), s.Tv().Any().String())
				s.NoError(err)

				updateResultCh := sendUpdateNoError(s, tv)

				// Process update in workflow. It will be attached to existing WT.
				res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
				s.NoError(err)
				s.NotNil(res)

				updateResult := <-updateResultCh
				s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
				s.EqualValues(0, res.NewTask.ResetHistoryEventId)

				s.Equal(2, wtHandlerCalls)
				s.Equal(2, msgHandlerCalls)

				events := s.GetHistory(s.Namespace().String(), tv.WorkflowExecution())

				s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 6} // WTScheduled event which delivered update to the worker.
 10 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 9}
`, events)
			})
		}
	})

	t.Run("RunningWorkflowTask_NewEmptySpeculativeWorkflowTask_Rejected", func(t *testing.T) {
		// Uses CaptureMetricsHandler which requires a dedicated cluster to avoid metric interference.
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		mustStartWorkflow(s, s.Tv())

		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		var updateResultCh <-chan *workflowservice.UpdateWorkflowExecutionResponse

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Send update after 1st WT has started.
				updateResultCh = sendUpdateNoError(s, s.Tv())
				// Completes WT with empty command list to create next WFT w/o events.
				return nil, nil
			case 2:
				s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted`, task.History)
				// Message handled rejects update.
				return nil, nil
			case 3:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted // Speculative WT2 disappeared and new normal WT was created.
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, task.History)
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
				}}, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1, 3:
				return nil, nil
			case 2:
				s.Len(task.Messages, 1)
				updRequestMsg := task.Messages[0]
				s.EqualValues(5, updRequestMsg.GetEventId())

				return s.UpdateRejectMessages(s.Tv(), updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT which starts 1st update.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)
		wt1Resp := res.NewTask

		// Reject update in 2nd WT.
		wt2Resp, err := poller.HandlePartialWorkflowTask(wt1Resp.GetWorkflowTask(), false)
		s.NoError(err)
		updateResult := <-updateResultCh
		s.Equal("rejection-of-"+s.Tv().UpdateID(), updateResult.GetOutcome().GetFailure().GetMessage())
		s.EqualValues(3, wt2Resp.ResetHistoryEventId)

		// Send signal to create WT.
		err = s.SendSignal(s.Namespace().String(), s.Tv().WorkflowExecution(), s.Tv().Any().String(), s.Tv().Any().Payloads(), s.Tv().Any().String())
		s.NoError(err)

		// Complete workflow.
		completeWorkflowResp, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(completeWorkflowResp)

		s.Equal(3, wtHandlerCalls)
		s.Equal(3, msgHandlerCalls)

		commits, rollbacks := speculativeWorkflowTaskOutcomes(capture.Snapshot())
		s.Equal(0, commits)
		s.Equal(1, rollbacks)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionCompleted`, events)
	})

	t.Run("RunningWorkflowTask_NewNotEmptySpeculativeWorkflowTask_Rejected", func(t *testing.T) {
		s := testcore.NewEnv(t)

		mustStartWorkflow(s, s.Tv())

		var updateResultCh <-chan *workflowservice.UpdateWorkflowExecutionResponse

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Send update after 1st WT has started.
				updateResultCh = sendUpdateNoError(s, s.Tv())
				// Completes WT with update unrelated commands to create events that will be in the next speculative WFT.
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             s.Tv().ActivityID(),
						ActivityType:           s.Tv().ActivityType(),
						TaskQueue:              s.Tv().TaskQueue(),
						ScheduleToCloseTimeout: s.Tv().Any().InfiniteTimeout(),
					}},
				}}, nil
			case 2:
				s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, task.History)
				// Message handled rejects update.
				return nil, nil
			case 3:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted // Empty speculative WFT was written in to the history because it shipped events.
  9 ActivityTaskStarted
 10 ActivityTaskCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
`, task.History)

				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
				}}, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1, 3:
				return nil, nil
			case 2:
				s.Len(task.Messages, 1)
				updRequestMsg := task.Messages[0]
				s.EqualValues(6, updRequestMsg.GetEventId())

				return s.UpdateRejectMessages(s.Tv(), updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
			return s.Tv().Any().Payloads(), false, nil
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			ActivityTaskHandler: atHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT which starts 1st update.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		wt1Resp := res.NewTask

		// Reject update in 2nd WT.
		wt2Resp, err := poller.HandlePartialWorkflowTask(wt1Resp.GetWorkflowTask(), false)
		s.NoError(err)
		s.NotNil(wt2Resp)
		updateResult := <-updateResultCh
		s.Equal("rejection-of-"+s.Tv().UpdateID(), updateResult.GetOutcome().GetFailure().GetMessage())
		s.EqualValues(0, wt2Resp.ResetHistoryEventId)

		// Schedule new WFT.
		err = poller.PollAndProcessActivityTask(false)
		s.NoError(err)

		// Complete workflow.
		completeWorkflowResp, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(completeWorkflowResp)
		s.EqualValues(0, completeWorkflowResp.NewTask.ResetHistoryEventId)

		s.Equal(3, wtHandlerCalls)
		s.Equal(3, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 ActivityTaskStarted
 10 ActivityTaskCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted
 14 WorkflowExecutionCompleted`, events)
	})

	t.Run("CompletedWorkflow", func(t *testing.T) {
		t.Run("receive outcome from completed Update", func(t *testing.T) {
			s := testcore.NewEnv(t)
			mustStartWorkflow(s, s.Tv())

			wtHandlerCalls := 0
			wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				wtHandlerCalls++
				switch wtHandlerCalls {
				case 1:
					// Completes first WT with empty command list.
					return nil, nil
				case 2:
					res := s.UpdateAcceptCompleteCommands(s.Tv())
					res = append(res, &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
					})
					return res, nil
				default:
					s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
					return nil, nil
				}
			}

			msgHandlerCalls := 0
			msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
				msgHandlerCalls++
				switch msgHandlerCalls {
				case 1:
					return nil, nil
				case 2:
					return s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]), nil
				default:
					s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
					return nil, nil
				}
			}

			//nolint:staticcheck // SA1019 TaskPoller replacement needed
			poller := &testcore.TaskPoller{
				Client:              s.FrontendClient(),
				Namespace:           s.Namespace().String(),
				TaskQueue:           s.Tv().TaskQueue(),
				Identity:            s.Tv().WorkerIdentity(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			// Drain first WT.
			_, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
			s.NoError(err)

			// Send Update request.
			updateResultCh := sendUpdateNoError(s, s.Tv())

			// Complete Update and Workflow.
			_, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
			s.NoError(err)

			// Receive Update result.
			updateResult1 := <-updateResultCh
			s.NotNil(updateResult1.GetOutcome().GetSuccess())

			// Send same Update request again, receiving the same Update result.
			updateResultCh = sendUpdateNoError(s, s.Tv())
			updateResult2 := <-updateResultCh
			s.Equal(updateResult1, updateResult2)
		})

		t.Run("receive update failure from accepted Update", func(t *testing.T) {
			s := testcore.NewEnv(t)
			mustStartWorkflow(s, s.Tv())

			wtHandlerCalls := 0
			wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				wtHandlerCalls++
				switch wtHandlerCalls {
				case 1:
					// Completes first WT with empty command list.
					return nil, nil
				case 2:
					res := s.UpdateAcceptCommands(s.Tv())
					res = append(res, &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
					})
					return res, nil
				default:
					s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
					return nil, nil
				}
			}

			msgHandlerCalls := 0
			msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
				msgHandlerCalls++
				switch msgHandlerCalls {
				case 1:
					return nil, nil
				case 2:
					return s.UpdateAcceptMessages(s.Tv(), task.Messages[0]), nil
				default:
					s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
					return nil, nil
				}
			}

			//nolint:staticcheck // SA1019 TaskPoller replacement needed
			poller := &testcore.TaskPoller{
				Client:              s.FrontendClient(),
				Namespace:           s.Namespace().String(),
				TaskQueue:           s.Tv().TaskQueue(),
				Identity:            s.Tv().WorkerIdentity(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			// Drain first WT.
			_, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
			s.NoError(err)

			// Send Update request.
			updateResultCh := sendUpdate(testcore.NewContext(), s, s.Tv())

			// Accept Update and complete Workflow.
			_, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
			s.NoError(err)

			// Receive Update result.
			updateResult1 := <-updateResultCh
			s.NoError(updateResult1.err)
			s.Equal("Workflow Update failed because the Workflow completed before the Update completed.", updateResult1.response.GetOutcome().GetFailure().GetMessage())

			// Send same Update request again, receiving the same failure.
			updateResultCh = sendUpdate(testcore.NewContext(), s, s.Tv())
			updateResult2 := <-updateResultCh
			s.NoError(updateResult2.err)
			s.Equal("Workflow Update failed because the Workflow completed before the Update completed.", updateResult2.response.GetOutcome().GetFailure().GetMessage())
		})
	})

	t.Run("ValidateWorkerMessages", func(t *testing.T) {
		testCases := []struct {
			Name                     string
			RespondWorkflowTaskError string
			MessageFn                func(t *testing.T, tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message
			CommandFn                func(t *testing.T, tv *testvars.TestVars, history *historypb.History) []*commandpb.Command
		}{
			{
				Name:                     "message-update-id-not-found-and-accepted-request-not-set",
				RespondWorkflowTaskError: "wasn't found",
				MessageFn: func(t *testing.T, tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
					return []*protocolpb.Message{
						{
							Id:                 tv.MessageID() + "_update-accepted",
							ProtocolInstanceId: tv.UpdateID() + tv.Any().String(),
							SequencingId:       nil,
							Body: protoutils.MarshalAny(t, &updatepb.Acceptance{
								AcceptedRequestMessageId:         reqMsg.GetId(),
								AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
								AcceptedRequest:                  nil, // Important not to pass original request back.
							}),
						},
					}
				},
				CommandFn: func(t *testing.T, tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
					return []*commandpb.Command{
						{
							CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
							Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
								MessageId: tv.MessageID() + "_update-accepted",
							}},
						},
					}
				},
			},
			{
				Name:                     "message-update-id-not-found-and-accepted-request-is-set",
				RespondWorkflowTaskError: "",
				MessageFn: func(t *testing.T, tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
					updRequest := protoutils.UnmarshalAny[*updatepb.Request](t, reqMsg.GetBody())
					return []*protocolpb.Message{
						{
							Id:                 tv.MessageID() + "_update-accepted",
							ProtocolInstanceId: tv.UpdateID() + tv.Any().String(),
							SequencingId:       nil,
							Body: protoutils.MarshalAny(t, &updatepb.Acceptance{
								AcceptedRequestMessageId:         reqMsg.GetId(),
								AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
								AcceptedRequest:                  updRequest, // Update will be resurrected from original request.
							}),
						},
					}
				},
				CommandFn: func(t *testing.T, tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
					return []*commandpb.Command{
						{
							CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
							Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
								MessageId: tv.MessageID() + "_update-accepted",
							}},
						},
					}
				},
			},
			{
				Name:                     "command-reference-missed-message",
				RespondWorkflowTaskError: "referenced absent message ID",
				MessageFn: func(t *testing.T, tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
					updRequest := protoutils.UnmarshalAny[*updatepb.Request](t, reqMsg.GetBody())
					return []*protocolpb.Message{
						{
							Id:                 tv.Any().String(),
							ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
							SequencingId:       nil,
							Body: protoutils.MarshalAny(t, &updatepb.Acceptance{
								AcceptedRequestMessageId:         reqMsg.GetId(),
								AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
								AcceptedRequest:                  updRequest,
							}),
						},
					}
				},
				CommandFn: func(t *testing.T, tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
					return []*commandpb.Command{
						{
							CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
							Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
								MessageId: tv.MessageID() + "_update-accepted",
							}},
						},
					}
				},
			},
			{
				Name:                     "complete-without-accept",
				RespondWorkflowTaskError: "invalid state transition attempted",
				MessageFn: func(t *testing.T, tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
					updRequest := protoutils.UnmarshalAny[*updatepb.Request](t, reqMsg.GetBody())
					return []*protocolpb.Message{
						{
							Id:                 tv.MessageID() + "_update-completed",
							ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
							SequencingId:       nil,
							Body: protoutils.MarshalAny(t, &updatepb.Response{
								Meta: updRequest.GetMeta(),
								Outcome: &updatepb.Outcome{
									Value: &updatepb.Outcome_Success{
										Success: tv.Any().Payloads(),
									},
								},
							}),
						},
					}
				},
				CommandFn: func(t *testing.T, tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
					return []*commandpb.Command{
						{
							CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
							Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
								MessageId: tv.MessageID() + "_update-completed",
							}},
						},
					}
				},
			},
			{
				Name:                     "accept-twice",
				RespondWorkflowTaskError: "invalid state transition attempted",
				MessageFn: func(t *testing.T, tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
					updRequest := protoutils.UnmarshalAny[*updatepb.Request](t, reqMsg.GetBody())
					return []*protocolpb.Message{
						{
							Id:                 tv.WithMessageIDNumber(1).MessageID(),
							ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
							SequencingId:       nil,
							Body: protoutils.MarshalAny(t, &updatepb.Acceptance{
								AcceptedRequestMessageId:         reqMsg.GetId(),
								AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
								AcceptedRequest:                  updRequest,
							}),
						},
						{
							Id:                 tv.WithMessageIDNumber(2).MessageID(),
							ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
							SequencingId:       nil,
							Body: protoutils.MarshalAny(t, &updatepb.Acceptance{
								AcceptedRequestMessageId:         reqMsg.GetId(),
								AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
								AcceptedRequest:                  updRequest,
							}),
						},
					}
				},
				CommandFn: func(t *testing.T, tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
					return []*commandpb.Command{
						{
							CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
							Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
								MessageId: tv.WithMessageIDNumber(1).MessageID(),
							}},
						},
						{
							CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
							Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
								MessageId: tv.WithMessageIDNumber(2).MessageID(),
							}},
						},
					}
				},
			},
			{
				Name:                     "success-case",
				RespondWorkflowTaskError: "",
				MessageFn: func(t *testing.T, tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
					updRequest := protoutils.UnmarshalAny[*updatepb.Request](t, reqMsg.GetBody())
					return []*protocolpb.Message{
						{
							Id:                 tv.MessageID() + "_update-accepted",
							ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
							SequencingId:       nil,
							Body: protoutils.MarshalAny(t, &updatepb.Acceptance{
								AcceptedRequestMessageId:         reqMsg.GetId(),
								AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
								AcceptedRequest:                  updRequest,
							}),
						},
						{
							Id:                 tv.MessageID() + "_update-completed",
							ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
							SequencingId:       nil,
							Body: protoutils.MarshalAny(t, &updatepb.Response{
								Meta: updRequest.GetMeta(),
								Outcome: &updatepb.Outcome{
									Value: &updatepb.Outcome_Success{
										Success: tv.Any().Payloads(),
									},
								},
							}),
						},
					}
				},
				CommandFn: func(t *testing.T, tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
					return []*commandpb.Command{
						{
							CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
							Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
								MessageId: tv.MessageID() + "_update-accepted",
							}},
						},
						{
							CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
							Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
								MessageId: tv.MessageID() + "_update-completed",
							}},
						},
					}
				},
			},
			{
				Name:                     "success-case-no-commands", // PROTOCOL_MESSAGE commands are optional.
				RespondWorkflowTaskError: "",
				MessageFn: func(t *testing.T, tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
					updRequest := protoutils.UnmarshalAny[*updatepb.Request](t, reqMsg.GetBody())
					return []*protocolpb.Message{
						{
							Id:                 tv.Any().String(),
							ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
							SequencingId:       nil,
							Body: protoutils.MarshalAny(t, &updatepb.Acceptance{
								AcceptedRequestMessageId:         reqMsg.GetId(),
								AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
								AcceptedRequest:                  updRequest,
							}),
						},
						{
							Id:                 tv.Any().String(),
							ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
							SequencingId:       nil,
							Body: protoutils.MarshalAny(t, &updatepb.Response{
								Meta: updRequest.GetMeta(),
								Outcome: &updatepb.Outcome{
									Value: &updatepb.Outcome_Success{
										Success: tv.Any().Payloads(),
									},
								},
							}),
						},
					}
				},
			},
			{
				Name:                     "invalid-command-order",
				RespondWorkflowTaskError: "invalid state transition attempted",
				MessageFn: func(t *testing.T, tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
					updRequest := protoutils.UnmarshalAny[*updatepb.Request](t, reqMsg.GetBody())
					return []*protocolpb.Message{
						{
							Id:                 tv.MessageID() + "_update-accepted",
							ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
							SequencingId:       nil,
							Body: protoutils.MarshalAny(t, &updatepb.Acceptance{
								AcceptedRequestMessageId:         reqMsg.GetId(),
								AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
								AcceptedRequest:                  updRequest,
							}),
						},
						{
							Id:                 tv.MessageID() + "_update-completed",
							ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
							SequencingId:       nil,
							Body: protoutils.MarshalAny(t, &updatepb.Response{
								Meta: updRequest.GetMeta(),
								Outcome: &updatepb.Outcome{
									Value: &updatepb.Outcome_Success{
										Success: tv.Any().Payloads(),
									},
								},
							}),
						},
					}
				},
				CommandFn: func(t *testing.T, tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
					return []*commandpb.Command{
						// Complete command goes before Accept command.
						{
							CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
							Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
								MessageId: tv.MessageID() + "_update-completed",
							}},
						},
						{
							CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
							Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
								MessageId: tv.MessageID() + "_update-accepted",
							}},
						},
					}
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.Name, func(t *testing.T) {
				s := testcore.NewEnv(t)
				mustStartWorkflow(s, s.Tv())

				wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
					if tc.CommandFn == nil {
						return nil, nil
					}
					return tc.CommandFn(t, s.Tv(), task.History), nil
				}

				msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
					if tc.MessageFn == nil {
						return nil, nil
					}
					s.Require().NotEmpty(task.Messages, "expected update message in task")
					updRequestMsg := task.Messages[0]
					return tc.MessageFn(t, s.Tv(), updRequestMsg), nil
				}

				//nolint:staticcheck // SA1019 TaskPoller replacement needed
				poller := &testcore.TaskPoller{
					Client:              s.FrontendClient(),
					Namespace:           s.Namespace().String(),
					TaskQueue:           s.Tv().TaskQueue(),
					Identity:            s.Tv().WorkerIdentity(),
					WorkflowTaskHandler: wtHandler,
					MessageHandler:      msgHandler,
					Logger:              s.Logger,
					T:                   t,
				}

				halfSecondTimeoutCtx, cancel := context.WithTimeout(testcore.NewContext(), 500*time.Millisecond)
				defer cancel()
				updateResultCh := sendUpdate(halfSecondTimeoutCtx, s, s.Tv())

				// Process update in workflow.
				_, err := poller.PollAndProcessWorkflowTask()
				updateResult := <-updateResultCh
				if tc.RespondWorkflowTaskError != "" {
					s.Error(err, "RespondWorkflowTaskCompleted should return an error contains `%v`", tc.RespondWorkflowTaskError)
					s.Contains(err.Error(), tc.RespondWorkflowTaskError)

					var wfNotReady *serviceerror.WorkflowNotReady
					s.ErrorAs(updateResult.err, &wfNotReady, "API caller should get serviceerror.WorkflowNotReady, if server got a validation error while processing worker response.")
					s.Contains(updateResult.err.Error(), "Unable to perform workflow execution update due to unexpected workflow task failure.")
					s.Nil(updateResult.response)
				} else {
					s.NoError(err)
					s.NoError(updateResult.err)
				}
			})
		}
	})

	t.Run("StickySpeculativeWorkflowTask_AcceptComplete", func(t *testing.T) {
		testCases := []struct {
			name     string
			useRunID bool
		}{
			{
				name:     "with RunID",
				useRunID: true,
			},
			{
				name:     "without RunID",
				useRunID: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				s := testcore.NewEnv(t)
				runID := mustStartWorkflow(s, s.Tv())
				tv := s.Tv()
				if tc.useRunID {
					tv = tv.WithRunID(runID)
				}

				// Drain existing first WT from regular task queue, but respond with sticky queue enabled response, next WT will go to sticky queue.
				_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
					func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
						return &workflowservice.RespondWorkflowTaskCompletedRequest{
							StickyAttributes: s.Tv().StickyExecutionAttributes(3 * time.Second),
						}, nil
					})
				s.NoError(err)

				go func() {
					// Process update in workflow task (it is sticky).
					res, err := s.TaskPoller().
						PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: s.Tv().StickyTaskQueue()}).
						HandleTask(s.Tv(),
							func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
								// This WT contains partial history because sticky was enabled.
								s.EqualHistory(`
						  4 WorkflowTaskCompleted
						  5 WorkflowTaskScheduled // Speculative WT.
						  6 WorkflowTaskStarted`, task.History)

								updRequestMsg := task.Messages[0]
								updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())
								//nolint:testifylint // callback runs synchronously within HandleTask
								s.Equal("args-value-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
								s.Equal(s.Tv().HandlerName(), updRequest.GetInput().GetName()) //nolint:testifylint // callback runs synchronously within HandleTask
								s.EqualValues(5, updRequestMsg.GetEventId())                   //nolint:testifylint // callback runs synchronously within HandleTask

								return &workflowservice.RespondWorkflowTaskCompletedRequest{
									Commands: s.UpdateAcceptCompleteCommands(s.Tv()),
									Messages: s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg),
								}, nil
							})
					//nolint:testifylint // intentional async polling pattern
					s.NoError(err)
					s.NotNil(res)                             //nolint:testifylint // intentional async polling pattern
					s.EqualValues(0, res.ResetHistoryEventId) //nolint:testifylint // intentional async polling pattern
				}()

				// This is to make sure that sticky poller above reached server first.
				// And when update comes, stick poller is already available.
				time.Sleep(500 * time.Millisecond) //nolint:forbidigo
				updateResult := <-sendUpdateNoError(s, tv)

				s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))

				s.EqualHistoryEvents(`
			  1 WorkflowExecutionStarted
			  2 WorkflowTaskScheduled
			  3 WorkflowTaskStarted
			  4 WorkflowTaskCompleted
			  5 WorkflowTaskScheduled
			  6 WorkflowTaskStarted
			  7 WorkflowTaskCompleted
			  8 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 5} // WTScheduled event which delivered update to the worker.
			  9 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 8}
			`, s.GetHistory(s.Namespace().String(), tv.WorkflowExecution()))
			})
		}
	})

	t.Run("StickySpeculativeWorkflowTask_AcceptComplete_StickyWorkerUnavailable", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				// Worker gets full history because update was issued after sticky worker is gone.
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
				return s.UpdateAcceptCompleteCommands(s.Tv()), nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				return nil, nil
			case 2:
				updRequestMsg := task.Messages[0]
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

				s.Equal("args-value-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
				s.Equal(s.Tv().HandlerName(), updRequest.GetInput().GetName())
				s.EqualValues(5, updRequestMsg.GetEventId())

				return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:                       s.FrontendClient(),
			Namespace:                    s.Namespace().String(),
			TaskQueue:                    s.Tv().TaskQueue(),
			StickyTaskQueue:              s.Tv().StickyTaskQueue(),
			StickyScheduleToStartTimeout: 3 * time.Second,
			Identity:                     s.Tv().WorkerIdentity(),
			WorkflowTaskHandler:          wtHandler,
			MessageHandler:               msgHandler,
			Logger:                       s.Logger,
			T:                            s.T(),
		}

		// Drain existing WT from regular task queue, but respond with sticky enabled response to enable stick task queue.
		_, err := poller.PollAndProcessWorkflowTask(testcore.WithRespondSticky, testcore.WithoutRetries)
		s.NoError(err)

		s.Logger.Info("Sleep 10+ seconds to make sure stickyPollerUnavailableWindow time has passed.")
		time.Sleep(10*time.Second + 100*time.Millisecond) //nolint:forbidigo
		s.Logger.Info("Sleep 10+ seconds is done.")

		// Now send an update. It should try sticky task queue first, but got "StickyWorkerUnavailable" error
		// and resend it to normal.
		// This can be observed in wtHandler: if history is partial => sticky task queue is used.
		updateResultCh := sendUpdateNoError(s, s.Tv())

		// Process update in workflow task from non-sticky task queue.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)
		updateResult := <-updateResultCh
		s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
		s.EqualValues(0, res.NewTask.ResetHistoryEventId)

		s.Equal(2, wtHandlerCalls)
		s.Equal(2, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 5} // WTScheduled event which delivered update to the worker.
  9 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 8}
`, events)
	})

	t.Run("FirstNormalScheduledWorkflowTask_Reject", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, task.History)
				return nil, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				updRequestMsg := task.Messages[0]
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

				s.Equal("args-value-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
				s.Equal(s.Tv().HandlerName(), updRequest.GetInput().GetName())
				s.EqualValues(2, updRequestMsg.GetEventId())

				return s.UpdateRejectMessages(s.Tv(), updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		updateResultCh := sendUpdateNoError(s, s.Tv())

		// Process update in workflow.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		updateResp := res.NewTask
		updateResult := <-updateResultCh
		s.Equal("rejection-of-"+s.Tv().UpdateID(), updateResult.GetOutcome().GetFailure().GetMessage())
		s.EqualValues(0, updateResp.ResetHistoryEventId)

		s.Equal(1, wtHandlerCalls)
		s.Equal(1, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled // First normal WT was scheduled before update and therefore all 3 events have to be written even if update was rejected.
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted // Empty completed WT. No new events were created after it.
`, events)
	})

	t.Run("EmptySpeculativeWorkflowTask_Reject", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(), taskpoller.DrainWorkflowTask)
		s.NoError(err)

		updateResultCh := sendUpdateNoError(s, s.Tv())

		// Process update in workflow.
		res, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.EqualHistory(`
			 1 WorkflowExecutionStarted
			 2 WorkflowTaskScheduled
			 3 WorkflowTaskStarted
			 4 WorkflowTaskCompleted
			 5 WorkflowTaskScheduled // Speculative WT.
			 6 WorkflowTaskStarted
			`, task.History)

				updRequestMsg := task.Messages[0]
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

				s.Equal("args-value-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
				s.Equal(s.Tv().HandlerName(), updRequest.GetInput().GetName())
				s.EqualValues(5, updRequestMsg.GetEventId())

				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Messages: s.UpdateRejectMessages(s.Tv(), updRequestMsg),
				}, nil
			})
		s.NoError(err)
		updateResult := <-updateResultCh
		s.Equal("rejection-of-"+s.Tv().UpdateID(), updateResult.GetOutcome().GetFailure().GetMessage())
		s.EqualValues(3, res.ResetHistoryEventId)

		// Send signal to create WT.
		err = s.SendSignal(s.Namespace().String(), s.Tv().WorkflowExecution(), s.Tv().Any().String(), s.Tv().Any().Payloads(), s.Tv().Any().String())
		s.NoError(err)

		// Process signal and complete workflow.
		res, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.EqualHistory(`
			 1 WorkflowExecutionStarted
			 2 WorkflowTaskScheduled
			 3 WorkflowTaskStarted
			 4 WorkflowTaskCompleted
			 5 WorkflowExecutionSignaled // Speculative WT was dropped and history starts from 5 again.
			 6 WorkflowTaskScheduled
			 7 WorkflowTaskStarted`, task.History)

				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands: []*commandpb.Command{
						{
							CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
							Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
								CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
							},
						},
					},
				}, nil
			})
		s.NoError(err)
		s.NotNil(res)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())
		s.EqualHistoryEvents(`
	 1 WorkflowExecutionStarted
	 2 WorkflowTaskScheduled
	 3 WorkflowTaskStarted
	 4 WorkflowTaskCompleted
	 5 WorkflowExecutionSignaled // Speculative WT is not present in the history.
	 6 WorkflowTaskScheduled
	 7 WorkflowTaskStarted
	 8 WorkflowTaskCompleted
	 9 WorkflowExecutionCompleted
	`, events)
	})

	t.Run("NotEmptySpeculativeWorkflowTask_Reject", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             s.Tv().ActivityID(),
						ActivityType:           s.Tv().ActivityType(),
						TaskQueue:              s.Tv().TaskQueue(),
						ScheduleToCloseTimeout: s.Tv().Any().InfiniteTimeout(),
					}},
				}}, nil
			case 2:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled // Speculative WFT will be written to the history because there is ActivityTaskScheduled(5) event.
  7 WorkflowTaskStarted
`, task.History)
				return nil, nil
			case 3:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted // Empty speculative WFT was written to the history because it shipped events.
  9 ActivityTaskStarted
 10 ActivityTaskCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
`, task.History)
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
				}}, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1, 3:
				return nil, nil
			case 2:
				updRequestMsg := task.Messages[0]
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

				s.Equal("args-value-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
				s.Equal(s.Tv().HandlerName(), updRequest.GetInput().GetName())
				s.EqualValues(6, updRequestMsg.GetEventId())

				return s.UpdateRejectMessages(s.Tv(), updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
			return s.Tv().Any().Payloads(), false, nil
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			ActivityTaskHandler: atHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		updateResultCh := sendUpdateNoError(s, s.Tv())

		// Process update in workflow.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)
		updateResult := <-updateResultCh
		s.Equal("rejection-of-"+s.Tv().UpdateID(), updateResult.GetOutcome().GetFailure().GetMessage())
		s.EqualValues(0, res.NewTask.ResetHistoryEventId, "no reset of event ID should happened after update rejection if it was delivered with normal workflow task")

		err = poller.PollAndProcessActivityTask(false)
		s.NoError(err)

		// Complete workflow.
		res, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)

		s.Equal(3, wtHandlerCalls)
		s.Equal(3, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled // Speculative WFT (6-8) presents in the history even though update was rejected.
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 ActivityTaskStarted
 10 ActivityTaskCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted
 14 WorkflowExecutionCompleted`, events)
	})

	t.Run("1stAccept_2ndAccept_2ndComplete_1stComplete", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())
		tv1 := s.Tv().WithUpdateIDNumber(1).WithMessageIDNumber(1).WithActivityIDNumber(1)
		tv2 := s.Tv().WithUpdateIDNumber(2).WithMessageIDNumber(2).WithActivityIDNumber(2)

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, task.History)
				return append(s.UpdateAcceptCommands(tv1), &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             tv1.ActivityID(),
						ActivityType:           tv1.ActivityType(),
						TaskQueue:              tv1.TaskQueue(),
						ScheduleToCloseTimeout: tv1.Any().InfiniteTimeout(),
					}},
				}), nil
			case 2:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted // 1st update is accepted.
  6 ActivityTaskScheduled
  7 WorkflowTaskScheduled // New normal WT is created because of the 2nd update.
  8 WorkflowTaskStarted`, task.History)
				return append(s.UpdateAcceptCommands(tv2), &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             tv2.ActivityID(),
						ActivityType:           tv2.ActivityType(),
						TaskQueue:              tv2.TaskQueue(),
						ScheduleToCloseTimeout: tv2.Any().InfiniteTimeout(),
					}},
				}), nil
			case 3:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 ActivityTaskScheduled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 WorkflowExecutionUpdateAccepted // 2nd update is accepted.
 11 ActivityTaskScheduled
 12 ActivityTaskStarted
 13 ActivityTaskCompleted
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted
`, task.History)
				return s.UpdateCompleteCommands(tv2), nil
			case 4:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 ActivityTaskScheduled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 WorkflowExecutionUpdateAccepted
 11 ActivityTaskScheduled
 12 ActivityTaskStarted
 13 ActivityTaskCompleted
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted
 16 WorkflowTaskCompleted
 17 WorkflowExecutionUpdateCompleted // 2nd update is completed.
 18 ActivityTaskStarted
 19 ActivityTaskCompleted
 20 WorkflowTaskScheduled
 21 WorkflowTaskStarted
`, task.History)
				return s.UpdateCompleteCommands(tv1), nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		var upd1RequestMsg, upd2RequestMsg *protocolpb.Message

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				upd1RequestMsg = task.Messages[0]
				upd1Request := protoutils.UnmarshalAny[*updatepb.Request](s.T(), upd1RequestMsg.GetBody())
				s.Equal("args-value-of-"+tv1.UpdateID(), testcore.DecodeString(s.T(), upd1Request.GetInput().GetArgs()))
				s.EqualValues(2, upd1RequestMsg.GetEventId())
				return s.UpdateAcceptMessages(tv1, upd1RequestMsg), nil
			case 2:
				upd2RequestMsg = task.Messages[0]
				upd2Request := protoutils.UnmarshalAny[*updatepb.Request](s.T(), upd2RequestMsg.GetBody())
				s.Equal("args-value-of-"+tv2.UpdateID(), testcore.DecodeString(s.T(), upd2Request.GetInput().GetArgs()))
				s.EqualValues(7, upd2RequestMsg.GetEventId())
				return s.UpdateAcceptMessages(tv2, upd2RequestMsg), nil
			case 3:
				s.NotNil(upd2RequestMsg)
				return s.UpdateCompleteMessages(tv2, upd2RequestMsg), nil
			case 4:
				s.NotNil(upd1RequestMsg)
				return s.UpdateCompleteMessages(tv1, upd1RequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
			return s.Tv().Any().Payloads(), false, nil
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			ActivityTaskHandler: atHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		updateResultCh1 := sendUpdateNoError(s, tv1)

		// Accept update1 in normal WT1.
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		// Send 2nd update and create speculative WT2.
		updateResultCh2 := sendUpdateNoError(s, tv2)

		// Poll for WT2 which 2nd update. Accept update2.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)
		s.EqualValues(0, res.NewTask.ResetHistoryEventId)

		err = poller.PollAndProcessActivityTask(false)
		s.NoError(err)

		// Complete update2 in WT3.
		res, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)
		updateResult2 := <-updateResultCh2
		s.Equal("success-result-of-"+tv2.UpdateID(), testcore.DecodeString(s.T(), updateResult2.GetOutcome().GetSuccess()))
		s.EqualValues(0, res.NewTask.ResetHistoryEventId)

		err = poller.PollAndProcessActivityTask(false)
		s.NoError(err)

		// Complete update1 in WT4.
		res, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)
		updateResult1 := <-updateResultCh1
		s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, updateResult1.Stage)
		s.Equal("success-result-of-"+tv1.UpdateID(), testcore.DecodeString(s.T(), updateResult1.GetOutcome().GetSuccess()))
		s.EqualValues(0, res.NewTask.ResetHistoryEventId)

		s.Equal(4, wtHandlerCalls)
		s.Equal(4, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 2} // WTScheduled event which delivered update to the worker.
  6 ActivityTaskScheduled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 7} // WTScheduled event which delivered update to the worker.
 11 ActivityTaskScheduled
 12 ActivityTaskStarted
 13 ActivityTaskCompleted
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted
 16 WorkflowTaskCompleted
 17 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 10} // 2nd update is completed.
 18 ActivityTaskStarted
 19 ActivityTaskCompleted
 20 WorkflowTaskScheduled
 21 WorkflowTaskStarted
 22 WorkflowTaskCompleted
 23 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 5} // 1st update is completed.
`, events)
	})

	t.Run("1stAccept_2ndReject_1stComplete", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		tv1 := s.Tv().WithUpdateIDNumber(1).WithMessageIDNumber(1).WithActivityIDNumber(1)
		tv2 := s.Tv().WithUpdateIDNumber(2).WithMessageIDNumber(2).WithActivityIDNumber(2)

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, task.History)
				return append(s.UpdateAcceptCommands(tv1), &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             tv1.ActivityID(),
						ActivityType:           tv1.ActivityType(),
						TaskQueue:              tv1.TaskQueue(),
						ScheduleToCloseTimeout: tv1.Any().InfiniteTimeout(),
					}},
				}), nil
			case 2:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted // 1st update is accepted.
  6 ActivityTaskScheduled
  7 WorkflowTaskScheduled // Speculative WFT with WorkflowExecutionUpdateAccepted(5) event.
  8 WorkflowTaskStarted
`, task.History)
				// Message handler rejects 2nd update.
				return nil, nil
			case 3:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 ActivityTaskScheduled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted // Speculative WFT is written to the history because it shipped event.
 10 ActivityTaskStarted
 11 ActivityTaskCompleted
 12 WorkflowTaskScheduled
 13 WorkflowTaskStarted
`, task.History)
				return s.UpdateCompleteCommands(tv1), nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		var upd1RequestMsg *protocolpb.Message
		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				upd1RequestMsg = task.Messages[0]
				upd1Request := protoutils.UnmarshalAny[*updatepb.Request](s.T(), upd1RequestMsg.GetBody())
				s.Equal("args-value-of-"+tv1.UpdateID(), testcore.DecodeString(s.T(), upd1Request.GetInput().GetArgs()))
				s.EqualValues(2, upd1RequestMsg.GetEventId())
				return s.UpdateAcceptMessages(tv1, upd1RequestMsg), nil
			case 2:
				upd2RequestMsg := task.Messages[0]
				upd2Request := protoutils.UnmarshalAny[*updatepb.Request](s.T(), upd2RequestMsg.GetBody())
				s.Equal("args-value-of-"+tv2.UpdateID(), testcore.DecodeString(s.T(), upd2Request.GetInput().GetArgs()))
				s.EqualValues(7, upd2RequestMsg.GetEventId())
				return s.UpdateRejectMessages(tv2, upd2RequestMsg), nil
			case 3:
				s.NotNil(upd1RequestMsg)
				return s.UpdateCompleteMessages(tv1, upd1RequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
			return s.Tv().Any().Payloads(), false, nil
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			ActivityTaskHandler: atHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		updateResultCh1 := sendUpdateNoError(s, tv1)

		// Accept update1 in WT1.
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		// Send 2nd update and create speculative WT2.
		updateResultCh2 := sendUpdateNoError(s, tv2)

		// Poll for WT2 which 2nd update. Reject update2.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)
		s.EqualValues(0, res.NewTask.ResetHistoryEventId, "no reset of event ID should happened after update rejection if it was delivered with workflow task which had events")

		updateResult2 := <-updateResultCh2
		s.Equal("rejection-of-"+tv2.UpdateID(), updateResult2.GetOutcome().GetFailure().GetMessage())

		err = poller.PollAndProcessActivityTask(false)
		s.NoError(err)

		// Complete update1 in WT3.
		res, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)
		updateResult1 := <-updateResultCh1
		s.Equal("success-result-of-"+tv1.UpdateID(), testcore.DecodeString(s.T(), updateResult1.GetOutcome().GetSuccess()))
		s.EqualValues(0, res.NewTask.ResetHistoryEventId)

		s.Equal(3, wtHandlerCalls)
		s.Equal(3, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 2} // WTScheduled event which delivered update to the worker.
  6 ActivityTaskScheduled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted // WT which had rejected update.
 10 ActivityTaskStarted
 11 ActivityTaskCompleted
 12 WorkflowTaskScheduled
 13 WorkflowTaskStarted
 14 WorkflowTaskCompleted
 15 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 5}
`, events)
	})

	t.Run("SpeculativeWorkflowTask_Fail", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(), taskpoller.DrainWorkflowTask)
		s.NoError(err)

		timeoutCtx, cancel := context.WithTimeout(testcore.NewContext(), 2*time.Second)
		defer cancel()
		updateResultCh := sendUpdate(timeoutCtx, s, s.Tv())

		// Try to accept update in workflow: get malformed response.
		_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.EqualHistory(`
			  1 WorkflowExecutionStarted
			  2 WorkflowTaskScheduled
			  3 WorkflowTaskStarted
			  4 WorkflowTaskCompleted
			  5 WorkflowTaskScheduled // Speculative WT.
			  6 WorkflowTaskStarted
			`, task.History)

				s.Require().NotEmpty(task.Messages, "expected update message in task")
				updRequestMsg := task.Messages[0]
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands: s.UpdateAcceptCommands(s.Tv()),
					// Emulate bug in worker/SDK update handler code. Return malformed acceptance response.
					Messages: []*protocolpb.Message{
						{
							Id:                 s.Tv().MessageID() + "_update-accepted",
							ProtocolInstanceId: s.Tv().Any().String(),
							SequencingId:       nil,
							Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
								AcceptedRequestMessageId:         updRequestMsg.GetId(),
								AcceptedRequestSequencingEventId: updRequestMsg.GetEventId(),
								AcceptedRequest:                  nil, // must not be nil!
							}),
						},
					},
				}, nil
			})
		s.Error(err)
		s.Contains(err.Error(), "wasn't found")

		// Update is aborted, speculative WFT failure is recorded into the history.
		updateResult := <-updateResultCh
		var wfNotReady *serviceerror.WorkflowNotReady
		s.ErrorAs(updateResult.err, &wfNotReady)
		s.Contains(updateResult.err.Error(), "Unable to perform workflow execution update due to unexpected workflow task failure.")

		// New transient WFT is created and is now included in the history.
		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskFailed
  8 WorkflowTaskScheduled`, events)

		// Send Update again. It will be delivered on existing transient WFT.
		updateResultCh = sendUpdate(timeoutCtx, s, s.Tv())

		// Try to accept 2nd update in workflow: get error. Poller will fail WFT, but the registry won't be cleared and Update won't be aborted.
		_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.EqualHistory(`
			  1 WorkflowExecutionStarted
			  2 WorkflowTaskScheduled
			  3 WorkflowTaskStarted
			  4 WorkflowTaskCompleted
			  5 WorkflowTaskScheduled
			  6 WorkflowTaskStarted
			  7 WorkflowTaskFailed
			  8 WorkflowTaskScheduled // Transient WFT
			  9 WorkflowTaskStarted`, task.History)

				s.Require().NotEmpty(task.Messages, "expected update message in task")
				updRequestMsg := task.Messages[0]
				s.EqualValues(8, updRequestMsg.GetEventId())
				// Returning error will cause the poller to fail WFT.
				return nil, errors.New("malformed request")
			})
		// The error is from RespondWorkflowTaskFailed, which should go w/o error.
		s.NoError(err)

		// Update timed out, but stays in the registry and will be delivered again on the new transient WFT.
		updateResult = <-updateResultCh
		s.Error(updateResult.err)
		s.True(common.IsContextDeadlineExceededErr(updateResult.err), "UpdateWorkflowExecution must timeout after 2 seconds")
		s.Nil(updateResult.response)

		// This WFT failure wasn't recorded because WFT was transient, but the scheduled event is included.
		events = s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskFailed
  8 WorkflowTaskScheduled`, events)

		// Try to accept 2nd update in workflow 2nd time: get error. Poller will fail WT. Update is not aborted.
		_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				// 1st attempt UpdateWorkflowExecution call has timed out but the
				// update is still running
				s.Require().NotEmpty(task.Messages, "expected update message in task")
				updRequestMsg := task.Messages[0]
				s.EqualValues(8, updRequestMsg.GetEventId())
				// Fail WT one more time. This is transient WT and shouldn't appear in the history.
				// Returning error will cause the poller to fail WT.
				return nil, errors.New("malformed request")
			})
		// The error is from RespondWorkflowTaskFailed, which should go w/o error.
		s.NoError(err)

		events = s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskFailed
  8 WorkflowTaskScheduled`, events)

		// Complete Update and workflow.
		_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.EqualHistory(`
			  1 WorkflowExecutionStarted
			  2 WorkflowTaskScheduled
			  3 WorkflowTaskStarted
			  4 WorkflowTaskCompleted
			  5 WorkflowTaskScheduled
			  6 WorkflowTaskStarted
			  7 WorkflowTaskFailed
			  8 WorkflowTaskScheduled // Transient WFT
			  9 WorkflowTaskStarted`, task.History)

				s.Require().NotEmpty(task.Messages, "expected update message in task")
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Messages: s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]),
					Commands: append(s.UpdateAcceptCompleteCommands(s.Tv()), &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
					}),
				}, nil
			})
		s.NoError(err)

		events = s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskFailed
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted // Transient WFT was completed successfully and ended up in the history.
 11 WorkflowExecutionUpdateAccepted
 12 WorkflowExecutionUpdateCompleted
 13 WorkflowExecutionCompleted`, events)
	})

	t.Run("StartedSpeculativeWorkflowTask_ConvertToNormalBecauseOfBufferedSignal", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT. Events 5 and 6 are written into the history when signal is received.
  6 WorkflowTaskStarted
`, task.History)
				// Send signal which will be buffered. This will persist MS and speculative WT must be converted to normal.
				err := s.SendSignal(s.Namespace().String(), s.Tv().WorkflowExecution(), s.Tv().Any().String(), s.Tv().Any().Payloads(), s.Tv().Any().String())
				s.NoError(err)
				return nil, nil
			case 3:
				s.EqualHistory(`
  7 WorkflowTaskCompleted
  8 WorkflowExecutionSignaled // It was buffered and got to the history after WT is completed.
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted`, task.History)
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
				}}, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1, 3:
				return nil, nil
			case 2:
				updRequestMsg := task.Messages[0]

				s.EqualValues(5, updRequestMsg.GetEventId())

				// Update is rejected but corresponding speculative WT will be in the history anyway, because it was converted to normal due to buffered signal.
				return s.UpdateRejectMessages(s.Tv(), updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		updateResultCh := sendUpdateNoError(s, s.Tv())

		// Process update in workflow.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		updateResp := res.NewTask
		updateResult := <-updateResultCh
		s.Equal("rejection-of-"+s.Tv().UpdateID(), updateResult.GetOutcome().GetFailure().GetMessage())
		s.EqualValues(0, updateResp.ResetHistoryEventId)

		// Complete workflow.
		completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
		s.NoError(err)
		s.NotNil(completeWorkflowResp)
		s.Nil(completeWorkflowResp.GetWorkflowTask())
		s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

		s.Equal(3, wtHandlerCalls)
		s.Equal(3, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted // Update was rejected on speculative WT, but events 5-7 are in the history because of buffered signal.
  8 WorkflowExecutionSignaled
  9 WorkflowTaskScheduled
 10 WorkflowTaskStarted
 11 WorkflowTaskCompleted
 12 WorkflowExecutionCompleted`, events)
	})

	t.Run("ScheduledSpeculativeWorkflowTask_ConvertToNormalBecauseOfSignal", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // It was initially speculative WT but was already converted to normal when signal was received.
  6 WorkflowExecutionSignaled
  7 WorkflowTaskStarted`, task.History)
				return nil, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				return nil, nil
			case 2:
				s.Require().NotEmpty(task.Messages, "expected update message in task")
				updRequestMsg := task.Messages[0]

				s.EqualValues(6, updRequestMsg.GetEventId())

				// Update is rejected but corresponding speculative WT was already converted to normal,
				// and will be in the history anyway.
				return s.UpdateRejectMessages(s.Tv(), updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		updateResultCh := sendUpdateNoError(s, s.Tv())

		// Send signal which will NOT be buffered because speculative WT is not started yet (only scheduled).
		// This will persist MS and speculative WT must be converted to normal.
		err = s.SendSignal(s.Namespace().String(), s.Tv().WorkflowExecution(), s.Tv().Any().String(), s.Tv().Any().Payloads(), s.Tv().Any().String())
		s.NoError(err)

		// Process update in workflow.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)
		updateResult := <-updateResultCh
		s.Equal("rejection-of-"+s.Tv().UpdateID(), updateResult.GetOutcome().GetFailure().GetMessage())
		s.EqualValues(0, res.NewTask.ResetHistoryEventId)

		s.Equal(2, wtHandlerCalls)
		s.Equal(2, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowExecutionSignaled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted // Update was rejected but WT events 5,7,8 are in the history because of signal.
`, events)
	})

	t.Run("SpeculativeWorkflowTask_StartToCloseTimeout", func(t *testing.T) {
		// Uses CaptureMetricsHandler which requires a dedicated cluster to avoid metric interference.
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           s.Tv().Any().String(),
			Namespace:           s.Namespace().String(),
			WorkflowId:          s.Tv().WorkflowID(),
			WorkflowType:        s.Tv().WorkflowType(),
			TaskQueue:           s.Tv().TaskQueue(),
			WorkflowTaskTimeout: durationpb.New(1 * time.Second), // Important!
		}

		_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
				// Emulate slow worker: sleep little more than WT timeout.
				time.Sleep(request.WorkflowTaskTimeout.AsDuration() + 100*time.Millisecond) //nolint:forbidigo
				// This doesn't matter because WT times out before update is applied.
				return s.UpdateAcceptCompleteCommands(s.Tv()), nil
			case 3:
				// Speculative WT timed out and retried as normal WT.
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskTimedOut
  8 WorkflowTaskScheduled {"Attempt":2 } // Transient WT.
  9 WorkflowTaskStarted`, task.History)
				commands := append(s.UpdateAcceptCompleteCommands(s.Tv()),
					&commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
					})
				return commands, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				return nil, nil
			case 2:
				s.Len(task.Messages, 1)
				updRequestMsg := task.Messages[0]

				// This doesn't matter because WT times out before update is applied.
				return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
			case 3:
				// Update is still in registry and was sent again.
				updRequestMsg := task.Messages[0]

				return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err = poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		updateResultCh := sendUpdateNoError(s, s.Tv())

		// Try to process update in workflow, but it takes more than WT timeout. So, WT times out.
		_, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.Error(err)
		s.Equal("Workflow task not found.", err.Error())

		// ensure correct metrics were recorded
		snap := capture.Snapshot()

		var speculativeWorkflowTaskTimeoutTasks int
		for _, m := range snap[metrics.TaskRequests.Name()] {
			if m.Tags[metrics.OperationTagName] == metrics.TaskTypeTimerActiveTaskSpeculativeWorkflowTaskTimeout {
				speculativeWorkflowTaskTimeoutTasks += 1
			}
		}
		s.Equal(1, speculativeWorkflowTaskTimeoutTasks, "expected 1 speculative workflow task timeout task to be created")

		var speculativeStartToCloseTimeouts int
		for _, m := range snap[metrics.StartToCloseTimeoutCounter.Name()] {
			if m.Tags[metrics.OperationTagName] == metrics.TaskTypeTimerActiveTaskSpeculativeWorkflowTaskTimeout {
				speculativeStartToCloseTimeouts += 1
			}
		}
		s.Equal(1, speculativeStartToCloseTimeouts, "expected 1 timeout of a speculative workflow task timeout task")

		// New normal WT was created on server after speculative WT has timed out.
		// It will accept and complete update first and workflow itself with the same WT.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		updateResp := res.NewTask
		updateResult := <-updateResultCh
		s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
		s.EqualValues(0, updateResp.ResetHistoryEventId)
		s.Nil(updateResp.GetWorkflowTask())

		s.Equal(3, wtHandlerCalls)
		s.Equal(3, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskTimedOut // Timeout of speculative WT writes events 5-7
  8 WorkflowTaskScheduled {"Attempt":2 }
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 8} // WTScheduled event which delivered update to the worker.
 12 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 11}
 13 WorkflowExecutionCompleted`, events)
	})

	t.Run("SpeculativeWorkflowTask_ScheduleToStartTimeout", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		// Drain first WT and respond with sticky enabled response to enable sticky task queue.
		stickyScheduleToStartTimeout := 1 * time.Second
		_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					StickyAttributes: s.Tv().StickyExecutionAttributes(stickyScheduleToStartTimeout),
				}, nil
			})
		s.NoError(err)

		sendUpdateNoError(s, s.Tv())

		s.Logger.Info("Wait for sticky timeout to fire. Sleep poller.StickyScheduleToStartTimeout+ seconds.", tag.Duration("StickyScheduleToStartTimeout", stickyScheduleToStartTimeout))
		time.Sleep(stickyScheduleToStartTimeout + 100*time.Millisecond) //nolint:forbidigo
		s.Logger.Info("Sleep is done.")

		// Try to process update in workflow, poll from normal task queue.
		res, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				// Speculative WFT timed out on sticky task queue. Server sent full history with sticky timeout event.
				s.EqualHistory(`
				1 WorkflowExecutionStarted
				2 WorkflowTaskScheduled
				3 WorkflowTaskStarted
				4 WorkflowTaskCompleted
				5 WorkflowTaskScheduled // Speculative WT.
				6 WorkflowTaskTimedOut
				7 WorkflowTaskScheduled {"Attempt":1} // Normal WT.
				8 WorkflowTaskStarted`, task.History)

				// Reject update, but WFT will still be in the history due to timeout on sticky queue.
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Messages: s.UpdateRejectMessages(s.Tv(), task.Messages[0]),
				}, nil
			})
		s.NoError(err)
		s.NotNil(res)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())
		s.EqualHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted
		5 WorkflowTaskScheduled // Speculative WT was written into the history because of timeout.
		6 WorkflowTaskTimedOut
		7 WorkflowTaskScheduled {"Attempt":1} // Second attempt WT is normal WT (clear stickiness reset attempts count).
		8 WorkflowTaskStarted
		9 WorkflowTaskCompleted // Normal WT is completed and events are in the history even update was rejected.
	`, events)
	})

	t.Run("SpeculativeWorkflowTask_ScheduleToStartTimeoutOnNormalTaskQueue", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled {"TaskQueue": {"Kind": 1}} // Speculative WT timed out on normal(1) task queue.
  6 WorkflowTaskTimedOut
  7 WorkflowTaskScheduled {"Attempt":1} // Normal WT is scheduled.
  8 WorkflowTaskStarted
`, task.History)
				return nil, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				return nil, nil
			case 2:
				updRequestMsg := task.Messages[0]
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

				s.Equal("args-value-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
				s.Equal(s.Tv().HandlerName(), updRequest.GetInput().GetName())
				s.EqualValues(7, updRequestMsg.GetEventId())

				return s.UpdateRejectMessages(s.Tv(), updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain existing WT from normal task queue.
		_, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)

		// Now send an update. It will create a speculative WT on normal task queue,
		// which will time out in 5 seconds and create new normal WT.
		updateResultCh := sendUpdateNoError(s, s.Tv())

		// TODO: it would be nice to shutdown matching before sending an update to emulate case which is actually being tested here.
		//  But test infrastructure doesn't support it. 5 seconds sleep will cause same observable effect.
		s.Logger.Info("Sleep 5+ seconds to make sure tasks.SpeculativeWorkflowTaskScheduleToStartTimeout time has passed.")
		time.Sleep(5*time.Second + 100*time.Millisecond) //nolint:forbidigo
		s.Logger.Info("Sleep 5+ seconds is done.")

		// Process update in workflow.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)
		updateResult := <-updateResultCh
		s.Equal("rejection-of-"+s.Tv().UpdateID(), updateResult.GetOutcome().GetFailure().GetMessage())
		s.EqualValues(0, res.NewTask.ResetHistoryEventId, "no reset of event ID should happened after update rejection if it was delivered with normal workflow task")

		s.Equal(2, wtHandlerCalls)
		s.Equal(2, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled {"TaskQueue": {"Kind": 1}} // Speculative WT timed out on normal(1) task queue.
  6 WorkflowTaskTimedOut
  7 WorkflowTaskScheduled {"Attempt":1} // Normal WT is scheduled. Even update was rejected, WT is in the history.
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
`, events)
	})

	t.Run("StartedSpeculativeWorkflowTask_TerminateWorkflow", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				// Terminate workflow while speculative WT is running.
				_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
					Namespace:         s.Namespace().String(),
					WorkflowExecution: s.Tv().WorkflowExecution(),
					Reason:            s.Tv().Any().String(),
				})
				s.NoError(err)

				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted`, task.History)
				return s.UpdateAcceptCompleteCommands(s.Tv()), nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				return nil, nil
			case 2:
				updRequestMsg := task.Messages[0]
				return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		oneSecondTimeoutCtx, cancel := context.WithTimeout(testcore.NewContext(), 1*time.Second)
		defer cancel()
		updateResultCh := sendUpdate(oneSecondTimeoutCtx, s, s.Tv())

		// Process update in workflow.
		_, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.Error(err)
		s.ErrorAs(err, new(*serviceerror.NotFound))
		s.ErrorContains(err, "Workflow task not found.")

		updateResult := <-updateResultCh
		s.Error(updateResult.err)
		var notFound *serviceerror.NotFound
		s.ErrorAs(updateResult.err, &notFound)
		s.ErrorContains(updateResult.err, update.AbortedByWorkflowClosingErr.Error())
		s.Nil(updateResult.response)

		s.Equal(2, wtHandlerCalls)
		s.Equal(2, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT was converted to normal WT during termination.
  6 WorkflowTaskStarted
  7 WorkflowTaskFailed
  8 WorkflowExecutionTerminated`, events)

		msResp, err := s.AdminClient().DescribeMutableState(testcore.NewContext(), &adminservice.DescribeMutableStateRequest{
			Namespace: s.Namespace().String(),
			Execution: s.Tv().WorkflowExecution(),
			Archetype: chasm.WorkflowArchetype,
		})
		s.NoError(err)
		s.EqualValues(7, msResp.GetDatabaseMutableState().GetExecutionInfo().GetCompletionEventBatchId(), "completion_event_batch_id should point to WTFailed event")
	})

	t.Run("ScheduledSpeculativeWorkflowTask_TerminateWorkflow", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				return nil, nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		oneSecondTimeoutCtx, cancel := context.WithTimeout(testcore.NewContext(), 1*time.Second)
		defer cancel()
		updateResultCh := sendUpdate(oneSecondTimeoutCtx, s, s.Tv())

		// Terminate workflow after speculative WT is scheduled but not started.
		_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: s.Tv().WorkflowExecution(),
			Reason:            s.Tv().Any().String(),
		})
		s.NoError(err)

		updateResult := <-updateResultCh
		s.Error(updateResult.err)
		var notFound *serviceerror.NotFound
		s.ErrorAs(updateResult.err, &notFound)
		s.ErrorContains(updateResult.err, update.AbortedByWorkflowClosingErr.Error())
		s.Nil(updateResult.response)

		s.Equal(1, wtHandlerCalls)
		s.Equal(1, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionTerminated // Speculative WTScheduled event is not written to history if WF is terminated.
`, events)

		msResp, err := s.AdminClient().DescribeMutableState(testcore.NewContext(), &adminservice.DescribeMutableStateRequest{
			Namespace: s.Namespace().String(),
			Execution: s.Tv().WorkflowExecution(),
			Archetype: chasm.WorkflowArchetype,
		})
		s.NoError(err)
		s.EqualValues(5, msResp.GetDatabaseMutableState().GetExecutionInfo().GetCompletionEventBatchId(), "completion_event_batch_id should point to WFTerminated event")
	})

	t.Run("CompleteWorkflow_AbortUpdates", func(t *testing.T) {
		type testCase struct {
			name          string
			description   string
			updateErr     map[string]string // Update error by completionCommand.Name.
			updateFailure string
			commands      func(s *testcore.FunctionalTestBase, tv *testvars.TestVars) []*commandpb.Command
			messages      func(s *testcore.FunctionalTestBase, tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message
		}
		type completionCommand struct {
			name        string
			finalStatus enumspb.WorkflowExecutionStatus
			useRunID    bool
			command     func(tv *testvars.TestVars) *commandpb.Command
		}
		testCases := []testCase{
			{
				name:        "update admitted",
				description: "update in stateAdmitted must get an error",
				updateErr: map[string]string{
					"workflow completed":                      update.AbortedByWorkflowClosingErr.Error(),
					"workflow continued as new without runID": "workflow operation can not be applied because workflow is closing",
					"workflow continued as new with runID":    "workflow operation can not be applied because workflow is closing",
					"workflow failed":                         update.AbortedByWorkflowClosingErr.Error(),
				},
				updateFailure: "",
				commands:      func(s *testcore.FunctionalTestBase, _ *testvars.TestVars) []*commandpb.Command { return nil },
				messages: func(s *testcore.FunctionalTestBase, _ *testvars.TestVars, _ *protocolpb.Message) []*protocolpb.Message {
					return nil
				},
			},
			{
				name:          "update accepted",
				description:   "update in stateAccepted must get an update failure",
				updateErr:     map[string]string{"*": ""},
				updateFailure: "Workflow Update failed because the Workflow completed before the Update completed.",
				commands: func(s *testcore.FunctionalTestBase, tv *testvars.TestVars) []*commandpb.Command {
					return s.UpdateAcceptCommands(tv)
				},
				messages: func(s *testcore.FunctionalTestBase, tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
					return s.UpdateAcceptMessages(tv, updRequestMsg)
				},
			},
			{
				name:          "update completed",
				description:   "completed update must not be affected by workflow completion",
				updateErr:     map[string]string{"*": ""},
				updateFailure: "",
				commands: func(s *testcore.FunctionalTestBase, tv *testvars.TestVars) []*commandpb.Command {
					return s.UpdateAcceptCompleteCommands(tv)
				},
				messages: func(s *testcore.FunctionalTestBase, tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg)
				},
			},
			{
				name:          "update rejected",
				description:   "rejected update must be rejected with rejection from workflow",
				updateErr:     map[string]string{"*": ""},
				updateFailure: "rejection-of-", // Rejection from workflow.
				commands:      func(s *testcore.FunctionalTestBase, tv *testvars.TestVars) []*commandpb.Command { return nil },
				messages: func(s *testcore.FunctionalTestBase, tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
					return s.UpdateRejectMessages(tv, updRequestMsg)
				},
			},
		}

		workflowCompletionCommands := []completionCommand{
			{
				name:        "workflow completed",
				finalStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				useRunID:    false,
				command: func(_ *testvars.TestVars) *commandpb.Command {
					return &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
					}
				},
			},
			{
				name:        "workflow continued as new with runID",
				finalStatus: enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
				useRunID:    true,
				command: func(tv *testvars.TestVars) *commandpb.Command {
					return &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
							WorkflowType: tv.WorkflowType(),
							TaskQueue:    tv.TaskQueue(),
						}},
					}
				},
			},
			{
				name:        "workflow continued as new without runID",
				finalStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, // This is the status of new run because update doesn't go to particular runID.
				useRunID:    false,
				command: func(tv *testvars.TestVars) *commandpb.Command {
					return &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
							WorkflowType: tv.WorkflowType(),
							TaskQueue:    tv.TaskQueue(),
						}},
					}
				},
			},
			{
				name:        "workflow failed",
				finalStatus: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
				useRunID:    true,
				command: func(tv *testvars.TestVars) *commandpb.Command {
					return &commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
							Failure: tv.Any().ApplicationFailure(),
						}},
					}
				},
			},
		}

		for _, tc := range testCases {
			for _, wfCC := range workflowCompletionCommands {
				t.Run(tc.name+" "+wfCC.name, func(t *testing.T) {
					s := testcore.NewEnv(t)
					runID := mustStartWorkflow(s, s.Tv())
					tv := s.Tv()
					if wfCC.useRunID {
						tv = tv.WithRunID(runID)
					}

					wtHandlerCalls := 0
					wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
						wtHandlerCalls++
						switch wtHandlerCalls {
						case 1:
							// Completes first WT with empty command list.
							return nil, nil
						case 2:
							return append(tc.commands(s.FunctionalTestBase, s.Tv()), wfCC.command(s.Tv())), nil
						default:
							s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
							return nil, nil
						}
					}

					msgHandlerCalls := 0
					msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
						msgHandlerCalls++
						switch msgHandlerCalls {
						case 1:
							return nil, nil
						case 2:
							updRequestMsg := task.Messages[0]
							return tc.messages(s.FunctionalTestBase, s.Tv(), updRequestMsg), nil
						default:
							s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
							return nil, nil
						}
					}

					//nolint:staticcheck // SA1019 TaskPoller replacement needed
					poller := &testcore.TaskPoller{
						Client:              s.FrontendClient(),
						Namespace:           s.Namespace().String(),
						TaskQueue:           s.Tv().TaskQueue(),
						Identity:            s.Tv().WorkerIdentity(),
						WorkflowTaskHandler: wtHandler,
						MessageHandler:      msgHandler,
						Logger:              s.Logger,
						T:                   s.T(),
					}

					// Drain first WT.
					_, err := poller.PollAndProcessWorkflowTask()
					s.NoError(err)

					updateResultCh := sendUpdate(testcore.NewContext(), s, tv)

					// Complete workflow.
					_, err = poller.PollAndProcessWorkflowTask()
					s.NoError(err)

					updateResult := <-updateResultCh
					expectedUpdateErr := tc.updateErr[wfCC.name]
					if expectedUpdateErr == "" {
						expectedUpdateErr = tc.updateErr["*"]
					}
					if expectedUpdateErr != "" {
						s.Error(updateResult.err, tc.description)
						s.Equal(expectedUpdateErr, updateResult.err.Error())
					} else {
						s.NoError(updateResult.err, tc.description)
					}

					if tc.updateFailure != "" {
						s.NotNil(updateResult.response.GetOutcome().GetFailure(), tc.description)
						s.Contains(updateResult.response.GetOutcome().GetFailure().GetMessage(), tc.updateFailure, tc.description)
					} else {
						s.Nil(updateResult.response.GetOutcome().GetFailure(), tc.description)
					}

					if expectedUpdateErr == "" && tc.updateFailure == "" {
						s.Equal(runID, updateResult.response.GetUpdateRef().GetWorkflowExecution().GetRunId(), "update wasn't applied to the same run as was started")
					}

					// Check that update didn't block workflow completion.
					descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
						Namespace: s.Namespace().String(),
						Execution: tv.WorkflowExecution(),
					})
					s.NoError(err)
					s.Equal(wfCC.finalStatus, descResp.WorkflowExecutionInfo.Status)

					s.Equal(2, wtHandlerCalls)
					s.Equal(2, msgHandlerCalls)
				})
			}
		}
	})

	t.Run("SpeculativeWorkflowTask_Heartbeat", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		// Drain first WT.
		_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(), taskpoller.DrainWorkflowTask)
		s.NoError(err)

		updateResultCh := sendUpdateNoError(s, s.Tv())

		// Heartbeat from speculative WT (no messages, no commands).
		var updRequestMsg *protocolpb.Message
		res, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.EqualHistory(`
			  1 WorkflowExecutionStarted
			  2 WorkflowTaskScheduled
			  3 WorkflowTaskStarted
			  4 WorkflowTaskCompleted
			  5 WorkflowTaskScheduled // Events (5 and 6) are for speculative WT, but they won't disappear after reject because speculative WT is converted to normal during heartbeat.
			  6 WorkflowTaskStarted
			`, task.History)

				s.Len(task.Messages, 1)
				updRequestMsg = task.Messages[0]
				s.EqualValues(5, updRequestMsg.GetEventId())

				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					ReturnNewWorkflowTask:      true,
					ForceCreateNewWorkflowTask: true,
				}, nil
			})
		s.NoError(err)

		// Reject update from workflow.
		updateResp, err := s.TaskPoller().HandleWorkflowTask(s.Tv(),
			res.GetWorkflowTask(),
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.EqualHistory(`
			  7 WorkflowTaskCompleted
			  8 WorkflowTaskScheduled // New WT (after heartbeat) is normal and won't disappear from the history after reject.
			  9 WorkflowTaskStarted
			`, task.History)

				s.Empty(task.Messages)

				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Messages: s.UpdateRejectMessages(s.Tv(), updRequestMsg),
				}, nil
			})
		s.NoError(err)
		s.NotNil(updateResp)

		updateResult := <-updateResultCh
		s.Equal("rejection-of-"+s.Tv().UpdateID(), updateResult.GetOutcome().GetFailure().GetMessage())
		s.EqualValues(0, updateResp.ResetHistoryEventId, "no reset of event ID should happened after update rejection because of heartbeat")

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 WorkflowTaskScheduled
	  6 WorkflowTaskStarted
	  7 WorkflowTaskCompleted // Heartbeat response.
	  8 WorkflowTaskScheduled
	  9 WorkflowTaskStarted
	 10 WorkflowTaskCompleted // After heartbeat new normal WT was created and events are written into the history even update is rejected.
	`, events)
	})

	t.Run("ScheduledSpeculativeWorkflowTask_LostUpdate", func(t *testing.T) {
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted`, task.History)
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
				}}, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				return nil, nil
			case 2:
				s.Empty(task.Messages, "update lost due to lost update registry")
				return nil, nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		halfSecondTimeoutCtx, cancel := context.WithTimeout(testcore.NewContext(), 500*time.Millisecond)
		defer cancel()
		updateResult := <-sendUpdate(halfSecondTimeoutCtx, s, s.Tv())
		s.Error(updateResult.err)
		s.True(common.IsContextDeadlineExceededErr(updateResult.err), updateResult.err.Error())
		s.Nil(updateResult.response)

		// Lose update registry. Speculative WFT and update registry disappear.
		loseUpdateRegistryAndAbandonPendingUpdates(s, s.Tv())

		// Ensure, there is no WFT.
		pollCtx, cancel := context.WithTimeout(testcore.NewContext(), common.MinLongPollTimeout*2)
		defer cancel()
		pollResponse, err := s.FrontendClient().PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: s.Tv().TaskQueue(),
			Identity:  s.Tv().WorkerIdentity(),
		})
		s.NoError(err)
		s.Nil(pollResponse.Messages, "there should not be new WFT with messages")

		// Send signal to schedule new WT.
		err = s.SendSignal(s.Namespace().String(), s.Tv().WorkflowExecution(), s.Tv().Any().String(), s.Tv().Any().Payloads(), s.Tv().Any().String())
		s.NoError(err)

		// Complete workflow and check that there is update messages.
		completeWorkflowResp, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)
		s.NotNil(completeWorkflowResp)

		s.Equal(2, wtHandlerCalls)
		s.Equal(2, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionCompleted`, events)
	})

	t.Run("StartedSpeculativeWorkflowTask_LostUpdate", func(t *testing.T) {
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT. Events 5 and 6 will be lost.
  6 WorkflowTaskStarted
`, task.History)

				// Lose update registry. Update is lost and NotFound error will be returned to RespondWorkflowTaskCompleted.
				loseUpdateRegistryAndAbandonPendingUpdates(s, s.Tv())

				return s.UpdateAcceptCompleteCommands(s.Tv()), nil
			case 3:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
`, task.History)
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
				}}, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				return nil, nil
			case 2:
				updRequestMsg := task.Messages[0]
				s.EqualValues(5, updRequestMsg.GetEventId())

				return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
			case 3:
				s.Empty(task.Messages, "no messages since update registry was lost")
				return nil, nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		halfSecondTimeoutCtx, cancel := context.WithTimeout(testcore.NewContext(), 500*time.Millisecond)
		defer cancel()
		updateResultCh := sendUpdate(halfSecondTimeoutCtx, s, s.Tv())

		// Process update in workflow.
		_, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.Error(err)
		s.ErrorAs(err, new(*serviceerror.NotFound))
		s.ErrorContains(err, "Workflow task not found")

		updateResult := <-updateResultCh
		s.Error(updateResult.err)
		s.True(common.IsContextDeadlineExceededErr(updateResult.err), updateResult.err.Error())
		s.Nil(updateResult.response)

		// Send signal to schedule new WFT.
		err = s.SendSignal(s.Namespace().String(), s.Tv().WorkflowExecution(), s.Tv().Any().String(), s.Tv().Any().Payloads(), s.Tv().Any().String())
		s.NoError(err)

		// Complete workflow.
		completeWorkflowResp, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)
		s.NotNil(completeWorkflowResp)

		s.Equal(3, wtHandlerCalls)
		s.Equal(3, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionCompleted`, events)
	})

	t.Run("FirstNormalWorkflowTask_UpdateResurrectedAfterRegistryCleared", func(t *testing.T) {
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
`, task.History)
				// Clear update registry. Update will be resurrected in registry from acceptance message.
				clearUpdateRegistryAndAbortPendingUpdates(s, s.Tv())

				return s.UpdateAcceptCompleteCommands(s.Tv()), nil
			case 2:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted
  7 WorkflowExecutionSignaled
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
`, task.History)
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
				}}, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				updRequestMsg := task.Messages[0]
				s.EqualValues(2, updRequestMsg.GetEventId())

				return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
			case 2:
				s.Empty(task.Messages, "update must be processed and not delivered again")
				return nil, nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		updateResultCh := sendUpdateNoError(s, s.Tv())

		// Process update in workflow. Update won't be found on server but will be resurrected from acceptance message and completed.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)

		// Client receives resurrected Update outcome.
		updateResult := <-updateResultCh
		s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))

		// Signal to create new WFT which shouldn't get any updates.
		err = s.SendSignal(s.Namespace().String(), s.Tv().WorkflowExecution(), s.Tv().Any().String(), s.Tv().Any().Payloads(), s.Tv().Any().String())
		s.NoError(err)

		// Complete workflow.
		completeWorkflowResp, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(completeWorkflowResp)

		s.Equal(2, wtHandlerCalls)
		s.Equal(2, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted
  7 WorkflowExecutionSignaled
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowExecutionCompleted`, events)
	})

	t.Run("ScheduledSpeculativeWorkflowTask_DeduplicateID", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
				return s.UpdateAcceptCompleteCommands(s.Tv()), nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				return nil, nil
			case 2:
				s.Len(task.Messages, 1, "2nd update must be deduplicated by ID")
				updRequestMsg := task.Messages[0]

				return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		updateResultCh := sendUpdateNoError(s, s.Tv())

		// Send second update with the same ID.
		updateResultCh2 := sendUpdateNoError(s, s.Tv())

		// Process update in workflow.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		updateResp := res.NewTask
		updateResult := <-updateResultCh
		updateResult2 := <-updateResultCh2
		s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
		s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult2.GetOutcome().GetSuccess()))
		s.EqualValues(0, updateResp.ResetHistoryEventId)

		s.Equal(2, wtHandlerCalls)
		s.Equal(2, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted  {"AcceptedRequestSequencingEventId": 5} // WTScheduled event which delivered update to the worker.
  9 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 8}
`, events)
	})

	t.Run("StartedSpeculativeWorkflowTask_DeduplicateID", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		var updateResultCh2 <-chan *workflowservice.UpdateWorkflowExecutionResponse

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				// Send second update with the same ID when WT is started but not completed.
				updateResultCh2 = sendUpdateNoError(s, s.Tv())

				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
				return s.UpdateAcceptCompleteCommands(s.Tv()), nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				return nil, nil
			case 2:
				s.Len(task.Messages, 1, "2nd update should not has reached server yet")
				updRequestMsg := task.Messages[0]
				return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		updateResultCh := sendUpdateNoError(s, s.Tv())

		// Process update in workflow.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)
		updateResult := <-updateResultCh
		s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
		s.EqualValues(0, res.NewTask.ResetHistoryEventId)

		updateResult2 := <-updateResultCh2
		s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult2.GetOutcome().GetSuccess()))

		s.Equal(2, wtHandlerCalls)
		s.Equal(2, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 5} // WTScheduled event which delivered update to the worker.
  9 WorkflowExecutionUpdateCompleted  {"AcceptedEventId": 8}
`, events)
	})

	t.Run("CompletedSpeculativeWorkflowTask_DeduplicateID", func(t *testing.T) {
		testCases := []struct {
			Name       string
			CloseShard bool
		}{
			{
				Name:       "no shard reload",
				CloseShard: false,
			},
			{
				Name:       "with shard reload",
				CloseShard: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.Name, func(t *testing.T) {
				// Uses closeShard conditionally which requires a dedicated cluster.
				var opts []testcore.TestOption
				if tc.CloseShard {
					opts = append(opts, testcore.WithDedicatedCluster())
				}
				s := testcore.NewEnv(t, opts...)
				mustStartWorkflow(s, s.Tv())

				wtHandlerCalls := 0
				wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
					wtHandlerCalls++
					switch wtHandlerCalls {
					case 1:
						// Completes first WT with empty command list.
						return nil, nil
					case 2:
						s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
						return s.UpdateAcceptCompleteCommands(s.Tv()), nil
					case 3:
						return []*commandpb.Command{{
							CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
							Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
						}}, nil
					default:
						s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
						return nil, nil
					}
				}

				msgHandlerCalls := 0
				msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
					msgHandlerCalls++
					switch msgHandlerCalls {
					case 1:
						return nil, nil
					case 2:
						updRequestMsg := task.Messages[0]
						return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
					case 3:
						s.Empty(task.Messages, "2nd update must be deduplicated by ID ")
						return nil, nil
					default:
						s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
						return nil, nil
					}
				}

				//nolint:staticcheck // SA1019 TaskPoller replacement needed
				poller := &testcore.TaskPoller{
					Client:              s.FrontendClient(),
					Namespace:           s.Namespace().String(),
					TaskQueue:           s.Tv().TaskQueue(),
					Identity:            s.Tv().WorkerIdentity(),
					WorkflowTaskHandler: wtHandler,
					MessageHandler:      msgHandler,
					Logger:              s.Logger,
					T:                   s.T(),
				}

				// Drain first WT.
				_, err := poller.PollAndProcessWorkflowTask()
				s.NoError(err)

				updateResultCh := sendUpdateNoError(s, s.Tv())

				// Process update in workflow.
				_, err = poller.PollAndProcessWorkflowTask()
				s.NoError(err)
				updateResult := <-updateResultCh
				s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))

				if tc.CloseShard {
					// Close shard to make sure that for completed updates deduplication works even after shard reload.
					closeShard(s, s.Tv().WorkflowID())
				}

				// Send second update with the same ID. It must return immediately.
				updateResult2 := <-sendUpdateNoError(s, s.Tv())

				// Ensure, there is no new WT.
				pollCtx, cancel := context.WithTimeout(testcore.NewContext(), common.MinLongPollTimeout*2)
				defer cancel()
				pollResponse, err := s.FrontendClient().PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
					Namespace: s.Namespace().String(),
					TaskQueue: s.Tv().TaskQueue(),
					Identity:  s.Tv().WorkerIdentity(),
				})
				s.NoError(err)
				s.Nil(pollResponse.Messages, "there must be no new WT")

				s.Equal(
					"success-result-of-"+s.Tv().UpdateID(),
					testcore.DecodeString(s.T(), updateResult2.GetOutcome().GetSuccess()),
					"results of the first update must be available")

				// Send signal to schedule new WT.
				err = s.SendSignal(s.Namespace().String(), s.Tv().WorkflowExecution(), s.Tv().Any().String(), s.Tv().Any().Payloads(), s.Tv().Any().String())
				s.NoError(err)

				// Complete workflow.
				completeWorkflowResp, err := poller.PollAndProcessWorkflowTask()
				s.NoError(err)
				s.NotNil(completeWorkflowResp)

				s.Equal(3, wtHandlerCalls)
				s.Equal(3, msgHandlerCalls)

				events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

				s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 5} // WTScheduled event which delivered update to the worker.
  9 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 8}
 10 WorkflowExecutionSignaled
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted
 14 WorkflowExecutionCompleted
`, events)
			})
		}
	})

	t.Run("StaleSpeculativeWorkflowTask_Fail_BecauseOfDifferentStartedId", func(t *testing.T) {
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		/*
			Test scenario:
			An update created a speculative WT and WT is dispatched to the worker (started).
			Update registry is cleared, speculative WT disappears from server.
			Update is retired and second speculative WT is scheduled but not dispatched yet.
			An activity completes, it converts the 2nd speculative WT into normal one.
			The first speculative WT responds back, server fails request because WorkflowTaskStarted event Id is mismatched.
			The second speculative WT responds back and server completes it.
		*/

		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Schedule activity.
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             s.Tv().ActivityID(),
						ActivityType:           s.Tv().ActivityType(),
						TaskQueue:              s.Tv().TaskQueue(),
						ScheduleToCloseTimeout: s.Tv().Any().InfiniteTimeout(),
					}},
				}}, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
			return s.Tv().Any().Payloads(), false, nil
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			WorkflowTaskHandler: wtHandler,
			ActivityTaskHandler: atHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// First WT will schedule activity.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)

		// Send 1st update. It will create 2nd WT as speculative.
		sendUpdateNoError(s, s.Tv())

		// Poll 2nd speculative WT with 1st update.
		wt2, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: s.Tv().TaskQueue(),
		})
		s.NoError(err)
		s.NotNil(wt2)
		s.NotEmpty(wt2.TaskToken, "2nd workflow task must have valid task token")
		s.Len(wt2.Messages, 1, "2nd workflow task must have a message with 1st update")
		s.EqualValues(7, wt2.StartedEventId)
		s.EqualValues(6, wt2.Messages[0].GetEventId())
		s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 ActivityTaskScheduled
	  6 WorkflowTaskScheduled
	  7 WorkflowTaskStarted`, wt2.History)

		// Clear update registry. Speculative WFT disappears from server.
		clearUpdateRegistryAndAbortPendingUpdates(s, s.Tv())

		// Wait for update request to be retry by frontend and recreated in registry. This will create a 3rd WFT as speculative.
		waitUpdateAdmitted(s, s.Tv())

		// Before polling for the 3rd speculative WT, process activity. This will convert 3rd speculative WT to normal WT.
		err = poller.PollAndProcessActivityTask(false)
		s.NoError(err)

		// Poll the 3rd WFT (not speculative anymore) but must have 2nd update.
		wt3, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: s.Tv().TaskQueue(),
		})
		s.NoError(err)
		s.NotNil(wt3)
		s.NotEmpty(wt3.TaskToken, "3rd workflow task must have valid task token")
		s.Len(wt3.Messages, 1, "3rd workflow task must have a message with 2nd update")
		s.EqualValues(9, wt3.StartedEventId)
		s.EqualValues(8, wt3.Messages[0].GetEventId())
		s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 ActivityTaskScheduled
	  6 WorkflowTaskScheduled
	  7 ActivityTaskStarted
	  8 ActivityTaskCompleted
	  9 WorkflowTaskStarted`, wt3.History)

		// Now try to complete 2nd WT (speculative). It should fail because WorkflowTaskStarted event Id is mismatched.
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: wt2.TaskToken,
			Commands:  s.UpdateAcceptCompleteCommands(s.Tv()),
			Messages:  s.UpdateAcceptCompleteMessages(s.Tv(), wt2.Messages[0]),
		})
		s.Error(err, "Must fail because WorkflowTaskStarted event Id is different.")
		s.ErrorAs(err, new(*serviceerror.NotFound))
		s.Contains(err.Error(), "Workflow task not found")

		// Complete 3rd WT. It should succeed.
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: wt3.TaskToken,
			Commands:  s.UpdateAcceptCompleteCommands(s.Tv()),
			Messages:  s.UpdateAcceptCompleteMessages(s.Tv(), wt3.Messages[0]),
		})
		s.NoError(err)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())
		s.EqualHistoryEvents(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 ActivityTaskScheduled
	  6 WorkflowTaskScheduled
	  7 ActivityTaskStarted {"ScheduledEventId":5}
	  8 ActivityTaskCompleted
	  9 WorkflowTaskStarted
	 10 WorkflowTaskCompleted
	 11 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId":8}
	 12 WorkflowExecutionUpdateCompleted
	`, events)
	})

	t.Run("StaleSpeculativeWorkflowTask_Fail_BecauseOfDifferentStartTime", func(t *testing.T) {
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		/*
			Test scenario:
			An update created a speculative WT and WT is dispatched to the worker (started).
			WF context is cleared, speculative WT is disappeared from server.
			Update is retried and second speculative WT is dispatched to worker with same WT scheduled/started Id and update Id.
			The first speculative WT respond back, server reject it because startTime is different.
			The second speculative WT respond back, server accept it.
		*/
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				return nil, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			WorkflowTaskHandler: wtHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// First WT will schedule activity.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.NotNil(res)

		// Send update. It will create 2nd WT as speculative.
		sendUpdateNoError(s, s.Tv())

		// Poll 2nd speculative WT with 1st update.
		wt2, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: s.Tv().TaskQueue(),
		})
		s.NoError(err)
		s.NotNil(wt2)
		s.NotEmpty(wt2.TaskToken, "2nd workflow task must have valid task token")
		s.Len(wt2.Messages, 1, "2nd workflow task must have a message with 1st update")
		s.EqualValues(6, wt2.StartedEventId)
		s.EqualValues(5, wt2.Messages[0].GetEventId())
		s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 WorkflowTaskScheduled
	  6 WorkflowTaskStarted`, wt2.History)

		// Clear update registry. Speculative WFT disappears from server.
		clearUpdateRegistryAndAbortPendingUpdates(s, s.Tv())

		// Wait for update request to be retry by frontend and recreated in registry. This will create a 3rd WFT as speculative.
		waitUpdateAdmitted(s, s.Tv())

		// Poll for the 3rd speculative WT.
		wt3, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: s.Tv().TaskQueue(),
		})
		s.NoError(err)
		s.NotNil(wt3)
		s.NotEmpty(wt3.TaskToken, "3rd workflow task must have valid task token")
		s.Len(wt3.Messages, 1, "3rd workflow task must have a message with 1st update")
		s.EqualValues(6, wt3.StartedEventId)
		s.EqualValues(5, wt3.Messages[0].GetEventId())
		s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 WorkflowTaskScheduled
	  6 WorkflowTaskStarted`, wt3.History)

		// Now try to complete 2nd (speculative) WT, it should fail.
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: wt2.TaskToken,
			Commands:  s.UpdateAcceptCompleteCommands(s.Tv()),
			Messages:  s.UpdateAcceptCompleteMessages(s.Tv(), wt2.Messages[0]),
		})
		s.Error(err, "Must fail because workflow task start time is different.")
		s.ErrorAs(err, new(*serviceerror.NotFound))
		s.Contains(err.Error(), "Workflow task not found")

		// Try to complete 3rd WT, it should succeed
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: wt3.TaskToken,
			Commands:  s.UpdateAcceptCompleteCommands(s.Tv()),
			Messages:  s.UpdateAcceptCompleteMessages(s.Tv(), wt3.Messages[0]),
		})
		s.NoError(err, "2nd speculative WT should be completed because it has same WT scheduled/started Id and startTime matches the accepted message is valid (same update Id)")

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())
		s.EqualHistoryEvents(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 WorkflowTaskScheduled
	  6 WorkflowTaskStarted
	  7 WorkflowTaskCompleted
	  8 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId":5}
	  9 WorkflowExecutionUpdateCompleted
	`, events)
	})

	t.Run("StaleSpeculativeWorkflowTask_Fail_NewWorkflowTaskWith2Updates", func(t *testing.T) {
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		/*
			Test scenario:
			An update created a speculative WT and WT is dispatched to the worker (started).
			Mutable state cleared, speculative WT and update registry are disappeared from server.
			First update is retried and another update come in.
			Second speculative WT is dispatched to worker with same WT scheduled/started Id but 2 updates.
			The first speculative WT responds back, server rejected it (different start time).
			The second speculative WT responds back, server accepted it.
		*/

		mustStartWorkflow(s, s.Tv())
		tv1 := s.Tv().WithUpdateIDNumber(1).WithMessageIDNumber(1)
		tv2 := s.Tv().WithUpdateIDNumber(2).WithMessageIDNumber(2)

		testCtx := testcore.NewContext()

		// Drain first WFT.
		wt1, err := s.FrontendClient().PollWorkflowTaskQueue(testCtx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: s.Tv().TaskQueue(),
		})
		s.NoError(err)
		s.NotNil(wt1)
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testCtx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: wt1.TaskToken,
		})
		s.NoError(err)

		// Send 1st update. It will create 2nd speculative WFT.
		sendUpdateNoError(s, tv1)

		// Poll 2nd speculative WFT with 1st update.
		wt2, err := s.FrontendClient().PollWorkflowTaskQueue(testCtx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: s.Tv().TaskQueue(),
		})
		s.NoError(err)
		s.NotNil(wt2)
		s.NotEmpty(wt2.TaskToken, "2nd workflow task must have valid task token")
		s.Len(wt2.Messages, 1, "2nd workflow task must have a message with 1st update")
		s.EqualValues(6, wt2.StartedEventId)
		s.EqualValues(5, wt2.Messages[0].GetEventId())
		s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 WorkflowTaskScheduled
	  6 WorkflowTaskStarted`, wt2.History)

		// Clear update registry. Speculative WFT disappears from server.
		clearUpdateRegistryAndAbortPendingUpdates(s, s.Tv())

		// Make sure UpdateWorkflowExecution call for the update "1" is retried and new (3rd) WFT is created as speculative with updateID=1.
		waitUpdateAdmitted(s, tv1)

		// Send 2nd update (with DIFFERENT updateId). It reuses already created 3rd WFT.
		sendUpdateNoError(s, tv2)
		// updateID=1 is still blocked. There must be 2 blocked updates now.

		// Poll the 3rd speculative WFT.
		wt3, err := s.FrontendClient().PollWorkflowTaskQueue(testCtx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: s.Tv().TaskQueue(),
		})
		s.NoError(err)
		s.NotNil(wt3)
		s.NotEmpty(wt3.TaskToken, "3rd workflow task must have valid task token")
		s.Len(wt3.Messages, 2, "3rd workflow task must have a message with 1st and 2nd updates")
		s.EqualValues(6, wt3.StartedEventId)
		s.EqualValues(5, wt3.Messages[0].GetEventId())
		s.EqualValues(5, wt3.Messages[1].GetEventId())
		s.EqualHistory(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 WorkflowTaskScheduled
	  6 WorkflowTaskStarted`, wt3.History)

		// Now try to complete 2nd speculative WT, it should fail because start time does not match.
		_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testCtx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace:             s.Namespace().String(),
			TaskToken:             wt2.TaskToken,
			Commands:              s.UpdateAcceptCompleteCommands(tv1),
			Messages:              s.UpdateAcceptCompleteMessages(tv1, wt2.Messages[0]),
			ReturnNewWorkflowTask: true,
		})
		s.Error(err, "Must fail because start time is different.")
		s.Contains(err.Error(), "Workflow task not found")
		s.ErrorAs(err, new(*serviceerror.NotFound))

		// Complete of the 3rd WT should succeed. It must accept both updates.
		wt4Resp, err := s.FrontendClient().RespondWorkflowTaskCompleted(testCtx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: wt3.TaskToken,
			Commands: append(
				s.UpdateAcceptCompleteCommands(tv1),
				s.UpdateAcceptCompleteCommands(tv2)...),
			Messages: append(
				s.UpdateAcceptCompleteMessages(tv1, wt3.Messages[0]),
				s.UpdateAcceptCompleteMessages(tv2, wt3.Messages[1])...),
			ReturnNewWorkflowTask: true,
		})
		s.NoError(err)
		s.NotNil(wt4Resp)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())
		s.EqualHistoryEvents(`
	  1 WorkflowExecutionStarted
	  2 WorkflowTaskScheduled
	  3 WorkflowTaskStarted
	  4 WorkflowTaskCompleted
	  5 WorkflowTaskScheduled
	  6 WorkflowTaskStarted
	  7 WorkflowTaskCompleted
	  8 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 5}
	  9 WorkflowExecutionUpdateCompleted
	 10 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 5}
	 11 WorkflowExecutionUpdateCompleted
	`, events)
	})

	t.Run("SpeculativeWorkflowTask_WorkerSkippedProcessing_RejectByServer", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())
		tv1 := s.Tv().WithUpdateIDNumber(1).WithMessageIDNumber(1)
		tv2 := s.Tv().WithUpdateIDNumber(2).WithMessageIDNumber(2)

		var update2ResultCh <-chan *workflowservice.UpdateWorkflowExecutionResponse

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
				update2ResultCh = sendUpdateNoError(s, tv2)
				return nil, nil
			case 3:
				s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted`, task.History)
				commands := append(s.UpdateAcceptCompleteCommands(tv2),
					&commandpb.Command{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
					})
				return commands, nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				return nil, nil
			case 2:
				updRequestMsg := task.Messages[0]
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

				s.Equal("args-value-of-"+tv1.UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
				s.EqualValues(5, updRequestMsg.GetEventId())

				// Don't process update in WT.
				return nil, nil
			case 3:
				s.Len(task.Messages, 1)
				updRequestMsg := task.Messages[0]
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

				s.Equal("args-value-of-"+tv2.UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
				s.EqualValues(5, updRequestMsg.GetEventId())
				return s.UpdateAcceptCompleteMessages(tv2, updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Identity:            "old_worker",
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		updateResultCh := sendUpdateNoError(s, tv1)

		// Process 2nd WT which ignores update message.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		updateResp := res.NewTask
		updateResult := <-updateResultCh
		s.Equal("Workflow Update is rejected because it wasn't processed by worker. Probably, Workflow Update is not supported by the worker.", updateResult.GetOutcome().GetFailure().GetMessage())
		s.EqualValues(3, updateResp.ResetHistoryEventId)

		// Process 3rd WT which completes 2nd update and workflow.
		update2Resp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
		s.NoError(err)
		s.NotNil(update2Resp)
		update2Result := <-update2ResultCh
		s.Equal("success-result-of-"+tv2.UpdateID(), testcore.DecodeString(s.T(), update2Result.GetOutcome().GetSuccess()))

		s.Equal(3, wtHandlerCalls)
		s.Equal(3, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // 1st speculative WT is not present in the history. This is 2nd speculative WT.
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted
  9 WorkflowExecutionUpdateCompleted
 10 WorkflowExecutionCompleted`, events)
	})

	t.Run("LastWorkflowTask_HasUpdateMessage", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:    s.FrontendClient(),
			Namespace: s.Namespace().String(),
			TaskQueue: s.Tv().TaskQueue(),
			Identity:  s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				completeWorkflowCommand := &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
						CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
							Result: s.Tv().Any().Payloads(),
						},
					},
				}
				return append(s.UpdateAcceptCommands(s.Tv()), completeWorkflowCommand), nil
			},
			MessageHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
				return s.UpdateAcceptMessages(s.Tv(), task.Messages[0]), nil
			},
			Logger: s.Logger,
			T:      s.T(),
		}

		updateResultCh := sendUpdateNoErrorWaitPolicyAccepted(s, s.Tv())
		_, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		updateResult := <-updateResultCh
		s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, updateResult.GetStage())
		s.Equal("Workflow Update failed because the Workflow completed before the Update completed.", updateResult.GetOutcome().GetFailure().GetMessage())

		s.EqualHistoryEvents(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowTaskStarted
	4 WorkflowTaskCompleted
	5 WorkflowExecutionUpdateAccepted
	6 WorkflowExecutionCompleted
	`, s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution()))
	})

	t.Run("SpeculativeWorkflowTask_QueryFailureClearsWFContext", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
`, task.History)
				return s.UpdateAcceptCompleteCommands(s.Tv()), nil
			default:
				s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
				return nil, nil
			}
		}

		msgHandlerCalls := 0
		msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			msgHandlerCalls++
			switch msgHandlerCalls {
			case 1:
				return nil, nil
			case 2:
				updRequestMsg := task.Messages[0]
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

				s.Equal("args-value-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
				s.Equal(s.Tv().HandlerName(), updRequest.GetInput().GetName())
				s.EqualValues(5, updRequestMsg.GetEventId())

				return s.UpdateAcceptCompleteMessages(s.Tv(), updRequestMsg), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)

		updateResultCh := sendUpdateNoError(s, s.Tv())

		type QueryResult struct {
			Resp *workflowservice.QueryWorkflowResponse
			Err  error
		}
		queryFn := func(resCh chan<- QueryResult) {
			// There is no query handler, and query timeout is ok for this test.
			// But first query must not time out before 2nd query reached server,
			// because 2 queries overflow the query buffer (default size 1),
			// which leads to clearing of WF context.
			shortCtx, cancel := context.WithTimeout(testcore.NewContext(), 100*time.Millisecond)
			defer cancel()
			queryResp, err := s.FrontendClient().QueryWorkflow(shortCtx, &workflowservice.QueryWorkflowRequest{
				Namespace: s.Namespace().String(),
				Execution: s.Tv().WorkflowExecution(),
				Query: &querypb.WorkflowQuery{
					QueryType: s.Tv().Any().String(),
				},
			})
			resCh <- QueryResult{Resp: queryResp, Err: err}
		}

		query1ResultCh := make(chan QueryResult)
		query2ResultCh := make(chan QueryResult)
		go queryFn(query1ResultCh)
		go queryFn(query2ResultCh)
		query1Res := <-query1ResultCh
		query2Res := <-query2ResultCh
		s.Error(query1Res.Err)
		s.Error(query2Res.Err)
		s.Nil(query1Res.Resp)
		s.Nil(query2Res.Resp)

		var queryBufferFullErr *serviceerror.ResourceExhausted
		if common.IsContextDeadlineExceededErr(query1Res.Err) {
			s.True(common.IsContextDeadlineExceededErr(query1Res.Err), "one of query errors must be CDE")
			s.ErrorAs(query2Res.Err, &queryBufferFullErr, "one of query errors must `query buffer is full`")
			s.Contains(query2Res.Err.Error(), "query buffer is full", "one of query errors must `query buffer is full`")
		} else {
			s.ErrorAs(query1Res.Err, &queryBufferFullErr, "one of query errors must `query buffer is full`")
			s.Contains(query1Res.Err.Error(), "query buffer is full", "one of query errors must `query buffer is full`")
			s.True(common.IsContextDeadlineExceededErr(query2Res.Err), "one of query errors must be CDE")
		}

		// "query buffer is full" error clears WF context. If update registry is not cleared together with context (old behaviour),
		// then update stays there but speculative WFT which supposed to deliver it, is cleared.
		// Subsequent retry attempts of "UpdateWorkflowExecution" API wouldn't help, because update is deduped by registry,
		// and new WFT is not created. Update is not delivered to the worker until new WFT is created.
		// If registry is cleared together with WF context (current behaviour), retries of "UpdateWorkflowExecution"
		// will create new update and WFT.

		// Wait to make sure that UpdateWorkflowExecution call is retried, update and speculative WFT are recreated.
		time.Sleep(500 * time.Millisecond) //nolint:forbidigo

		// Process update in workflow.
		res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		updateResp := res.NewTask
		updateResult := <-updateResultCh
		s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
		s.EqualValues(0, updateResp.ResetHistoryEventId)

		s.Equal(2, wtHandlerCalls)
		s.Equal(2, msgHandlerCalls)

		events := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Was speculative WT...
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted // ...and events were written to the history when WT completes.
  8 WorkflowExecutionUpdateAccepted
  9 WorkflowExecutionUpdateCompleted
`, events)
	})

	t.Run("UpdatesAreSentToWorkerInOrderOfAdmission", func(t *testing.T) {
		s := testcore.NewEnv(t)
		// If our implementation is not in fact ordering updates correctly, then it may be ordering them
		// non-deterministically. This number should be high enough that the false-negative rate of the test is low, but
		// must not exceed our limit on number of in-flight updates. If we were picking a random ordering then the
		// false-negative rate would be 1/(nUpdates!).
		nUpdates := 10

		mustStartWorkflow(s, s.Tv())
		for i := range nUpdates {
			// Sequentially send updates one by one.
			sendUpdateNoError(s, s.Tv().WithUpdateIDNumber(i))
		}

		wtHandlerCalls := 0
		msgHandlerCalls := 0
		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:    s.FrontendClient(),
			Namespace: s.Namespace().String(),
			TaskQueue: s.Tv().TaskQueue(),
			Identity:  s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				msgHandlerCalls++
				var commands []*commandpb.Command
				for i := range task.Messages {
					commands = append(commands, s.UpdateAcceptCompleteCommands(s.Tv().WithMessageIDNumber(i))...)
				}
				return commands, nil
			},
			MessageHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
				wtHandlerCalls++
				s.Len(task.Messages, nUpdates)
				var messages []*protocolpb.Message
				// Updates were sent in sequential order of updateId => messages must be ordered in the same way.
				for i, m := range task.Messages {
					s.Equal(s.Tv().WithUpdateIDNumber(i).UpdateID(), m.ProtocolInstanceId)
					messages = append(messages, s.UpdateAcceptCompleteMessages(s.Tv().WithMessageIDNumber(i), m)...)
				}
				return messages, nil
			},
			Logger: s.Logger,
			T:      s.T(),
		}
		_, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		s.Equal(1, wtHandlerCalls)
		s.Equal(1, msgHandlerCalls)

		expectedHistory := `
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
`
		for i := range nUpdates {
			tvi := s.Tv().WithUpdateIDNumber(i)
			expectedHistory += fmt.Sprintf(`
  %d WorkflowExecutionUpdateAccepted {"AcceptedRequest":{"Meta": {"UpdateId": "%s"}}}
  %d WorkflowExecutionUpdateCompleted {"Meta": {"UpdateId": "%s"}}`,
				5+2*i, tvi.UpdateID(),
				6+2*i, tvi.UpdateID())
		}

		history := s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution())
		s.EqualHistoryEvents(expectedHistory, history)
	})

	t.Run("WaitAccepted_GotCompleted", func(t *testing.T) {
		s := testcore.NewEnv(t)
		mustStartWorkflow(s, s.Tv())

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller := &testcore.TaskPoller{
			Client:    s.FrontendClient(),
			Namespace: s.Namespace().String(),
			TaskQueue: s.Tv().TaskQueue(),
			Identity:  s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				return s.UpdateAcceptCompleteCommands(s.Tv()), nil
			},
			MessageHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
				return s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]), nil
			},
			Logger: s.Logger,
			T:      s.T(),
		}

		// Send Update with intent to wait for Accepted stage only,
		updateResultCh := sendUpdateNoErrorWaitPolicyAccepted(s, s.Tv())
		_, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)
		updateResult := <-updateResultCh
		// but Update was accepted and completed on the same WFT, and outcome was returned.
		s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, updateResult.GetStage())
		s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))

		s.EqualHistoryEvents(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowTaskStarted
	4 WorkflowTaskCompleted
	5 WorkflowExecutionUpdateAccepted
	6 WorkflowExecutionUpdateCompleted
	`, s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution()))
	})

	t.Run("ContinueAsNew_UpdateIsNotCarriedOver", func(t *testing.T) {
		s := testcore.NewEnv(t)
		firstRunID := mustStartWorkflow(s, s.Tv())
		tv1 := s.Tv().WithUpdateIDNumber(1).WithMessageIDNumber(1)
		tv2 := s.Tv().WithUpdateIDNumber(2).WithMessageIDNumber(2)

		/*
			1st Update goes to the 1st run and accepted (but not completed) by Workflow.
			While this WFT is running, 2nd Update is sent, and WFT is completing with CAN for the 1st run.
			There are 2 Updates in the registry of the 1st run: 1st is accepted and 2nd is admitted.
			Both of them are aborted but with different errors:
			- Admitted Update is aborted with retryable "workflow is closing" error. SDK should retry this error
			  and new attempt should land on the new run.
			- Accepted Update is aborted with update failure.
		*/

		var update2ResponseCh <-chan updateResponseErr

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller1 := &testcore.TaskPoller{
			Client:    s.FrontendClient(),
			Namespace: s.Namespace().String(),
			TaskQueue: s.Tv().TaskQueue(),
			Identity:  s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				// Send 2nd Update while WFT is running.
				update2ResponseCh = sendUpdate(context.Background(), s, tv2)
				canCommand := &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						WorkflowType: s.Tv().WorkflowType(),
						TaskQueue:    s.Tv().WithTaskQueueNumber(2).TaskQueue(),
					}},
				}
				return append(s.UpdateAcceptCommands(tv1), canCommand), nil
			},
			MessageHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
				return s.UpdateAcceptMessages(tv1, task.Messages[0]), nil
			},
			Logger: s.Logger,
			T:      s.T(),
		}

		//nolint:staticcheck // SA1019 TaskPoller replacement needed
		poller2 := &testcore.TaskPoller{
			Client:    s.FrontendClient(),
			Namespace: s.Namespace().String(),
			TaskQueue: s.Tv().WithTaskQueueNumber(2).TaskQueue(),
			Identity:  s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				return nil, nil
			},
			MessageHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
				s.Empty(task.Messages, "no Updates should be carried over to the 2nd run")
				return nil, nil
			},
			Logger: s.Logger,
			T:      s.T(),
		}

		update1ResponseCh := sendUpdate(context.Background(), s, tv1)
		_, err := poller1.PollAndProcessWorkflowTask()
		s.NoError(err)

		_, err = poller2.PollAndProcessWorkflowTask()
		s.NoError(err)

		update1Response := <-update1ResponseCh
		s.NoError(update1Response.err)
		s.Equal("Workflow Update failed because the Workflow completed before the Update completed.", update1Response.response.GetOutcome().GetFailure().GetMessage())

		update2Response := <-update2ResponseCh
		s.Error(update2Response.err)
		var resourceExhausted *serviceerror.ResourceExhausted
		s.ErrorAs(update2Response.err, &resourceExhausted)
		s.Equal("workflow operation can not be applied because workflow is closing", update2Response.err.Error())

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionContinuedAsNew`, s.GetHistory(s.Namespace().String(), s.Tv().WithRunID(firstRunID).WorkflowExecution()))

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted`, s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution()))
	})

	t.Run("ContinueAsNew_Suggestion", func(t *testing.T) {
		// setup CAN suggestion to be at 2nd Update
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.WorkflowExecutionMaxTotalUpdates, 3),
			testcore.WithDynamicConfig(dynamicconfig.WorkflowExecutionMaxTotalUpdatesSuggestContinueAsNewThreshold, 0.5),
		)

		// start workflow
		mustStartWorkflow(s, s.Tv())
		_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(), taskpoller.DrainWorkflowTask)
		s.NoError(err)

		// send Update #1 - no CAN suggested
		tv1 := s.Tv().WithUpdateIDNumber(1)
		updateResultCh := sendUpdateNoError(s, tv1)
		_, err = s.TaskPoller().PollAndHandleWorkflowTask(tv1,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.EqualHistoryEvents(`
			  1 WorkflowExecutionStarted
			  2 WorkflowTaskScheduled
			  3 WorkflowTaskStarted {"SuggestContinueAsNew": false}
  			  4 WorkflowTaskCompleted
			  5 WorkflowTaskScheduled
			  6 WorkflowTaskStarted {"SuggestContinueAsNew": false}`, task.History.Events)

				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Messages: s.UpdateAcceptCompleteMessages(tv1, task.Messages[0]),
				}, nil
			})
		s.NoError(err)
		<-updateResultCh

		// send Update #2 - CAN suggested
		tv2 := s.Tv().WithUpdateIDNumber(2)
		updateResultCh = sendUpdateNoError(s, tv2)
		_, err = s.TaskPoller().PollAndHandleWorkflowTask(tv2,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.EqualHistoryEventsSuffix(`
			  WorkflowTaskStarted {"SuggestContinueAsNew": true}`, task.History.Events)

				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Messages: s.UpdateAcceptCompleteMessages(tv2, task.Messages[0]),
				}, nil
			})
		s.NoError(err)
		<-updateResultCh
	})

	t.Run("UpdateWithStart", func(t *testing.T) {
		type multiopsResponseErr struct {
			response *workflowservice.ExecuteMultiOperationResponse
			err      error
		}

		sendUpdateWithStart := func(s testcore.Env, ctx context.Context, startReq *workflowservice.StartWorkflowExecutionRequest, updateReq *workflowservice.UpdateWorkflowExecutionRequest) chan multiopsResponseErr {
			capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
			defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

			retCh := make(chan multiopsResponseErr)
			go func() {
				resp, err := s.FrontendClient().ExecuteMultiOperation(
					ctx,
					&workflowservice.ExecuteMultiOperationRequest{
						Namespace: s.Namespace().String(),
						Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
							{
								Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
									StartWorkflow: startReq,
								},
							},
							{
								Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
									UpdateWorkflow: updateReq,
								},
							},
						},
					})

				if err == nil {
					// Use assert (not require) in goroutine - require calls t.FailNow() which panics
					//nolint:testifylint // intentional use of assert in goroutine
					assert.Len(s.T(), resp.Responses, 2)

					startRes := resp.Responses[0].Response.(*workflowservice.ExecuteMultiOperationResponse_Response_StartWorkflow).StartWorkflow
					assert.NotEmpty(s.T(), startRes.RunId)

					updateRes := resp.Responses[1].Response.(*workflowservice.ExecuteMultiOperationResponse_Response_UpdateWorkflow).UpdateWorkflow
					if updateReq.WaitPolicy.LifecycleStage == enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED {
						assert.NotNil(s.T(), updateRes.Outcome)
						assert.NotEmpty(s.T(), updateRes.Outcome.String())
					}
				}

				// make sure there's no lock contention
				//nolint:testifylint // intentional use of assert in goroutine
				assert.Empty(s.T(), capture.Snapshot()[metrics.TaskWorkflowBusyCounter.Name()])

				retCh <- multiopsResponseErr{resp, err}
			}()
			return retCh
		}

		startWorkflowReq := func(s testcore.Env, tv *testvars.TestVars) *workflowservice.StartWorkflowExecutionRequest {
			return &workflowservice.StartWorkflowExecutionRequest{
				Namespace:    s.Namespace().String(),
				WorkflowId:   tv.WorkflowID(),
				WorkflowType: tv.WorkflowType(),
				TaskQueue:    tv.TaskQueue(),
				Identity:     tv.WorkerIdentity(),
			}
		}

		t.Run("workflow is not running", func(t *testing.T) {
			for _, p := range []enumspb.WorkflowIdConflictPolicy{
				enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
				enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
				enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
			} {
				t.Run(fmt.Sprintf("start workflow and send update (with conflict policy %v)", p), func(t *testing.T) {
					t.Run("and accept", func(t *testing.T) {
						s := testcore.NewEnv(t)
						startReq := startWorkflowReq(s, s.Tv())
						startReq.WorkflowIdConflictPolicy = p
						updateReq := updateWorkflowRequest(s, s.Tv(),
							&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
						uwsCh := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

						_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
							func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
								return &workflowservice.RespondWorkflowTaskCompletedRequest{
									Messages: s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]),
								}, nil
							})
						s.NoError(err)

						uwsRes := <-uwsCh
						s.NoError(err)
						startResp := uwsRes.response.Responses[0].GetStartWorkflow()
						updateRep := uwsRes.response.Responses[1].GetUpdateWorkflow()
						requireStartedAndRunning(s.T(), startResp)
						s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateRep.GetOutcome().GetSuccess()))

						// poll update to ensure same outcome is returned
						pollRes, err := pollUpdate(s, s.Tv(),
							&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
						s.NoError(err)
						s.Equal(updateRep.Outcome.String(), pollRes.Outcome.String())

						s.EqualHistoryEvents(`
					  1 WorkflowExecutionStarted
					  2 WorkflowTaskScheduled
					  3 WorkflowTaskStarted
					  4 WorkflowTaskCompleted
					  5 WorkflowExecutionUpdateAccepted
					  6 WorkflowExecutionUpdateCompleted`, s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution()))
					})

					t.Run("and reject", func(t *testing.T) {
						s := testcore.NewEnv(t)
						startReq := startWorkflowReq(s, s.Tv())
						updateReq := updateWorkflowRequest(s, s.Tv(),
							&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
						uwsCh := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

						_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
							func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
								return &workflowservice.RespondWorkflowTaskCompletedRequest{
									Messages: s.UpdateRejectMessages(s.Tv(), task.Messages[0]),
								}, nil
							})
						s.NoError(err)

						uwsRes := <-uwsCh
						s.NoError(uwsRes.err)
						startResp := uwsRes.response.Responses[0].GetStartWorkflow()
						updateRep := uwsRes.response.Responses[1].GetUpdateWorkflow()
						requireStartedAndRunning(s.T(), startResp)
						s.Equal("rejection-of-"+s.Tv().UpdateID(), updateRep.GetOutcome().GetFailure().GetMessage())

						// poll update to ensure same outcome is returned
						_, err = pollUpdate(s, s.Tv(),
							&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
						s.Error(err)
						s.ErrorAs(err, new(*serviceerror.NotFound))

						s.EqualHistoryEvents(`
					  1 WorkflowExecutionStarted
					  2 WorkflowTaskScheduled
					  3 WorkflowTaskStarted
					  4 WorkflowTaskCompleted`, s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution()))
					})
				})
			}
		})

		t.Run("workflow is running", func(t *testing.T) {
			t.Run("workflow id conflict policy use-existing: only send update", func(t *testing.T) {
				t.Run("and accept", func(t *testing.T) {
					s := testcore.NewEnv(t)
					// start workflow
					_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowReq(s, s.Tv()))
					s.NoError(err)

					_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(), taskpoller.DrainWorkflowTask)
					s.NoError(err)

					// update-with-start
					startReq := startWorkflowReq(s, s.Tv())
					startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
					updateReq := updateWorkflowRequest(s, s.Tv(),
						&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
					uwsCh := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

					_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
						func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
							return &workflowservice.RespondWorkflowTaskCompletedRequest{
								Messages: s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]),
							}, nil
						})
					s.NoError(err)

					uwsRes := <-uwsCh
					s.NoError(uwsRes.err)
					startResp := uwsRes.response.Responses[0].GetStartWorkflow()
					updateRep := uwsRes.response.Responses[1].GetUpdateWorkflow()
					requireNotStartedButRunning(s.T(), startResp)
					s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateRep.GetOutcome().GetSuccess()))

					// poll update to ensure same outcome is returned
					pollRes, err := pollUpdate(s, s.Tv(),
						&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
					s.Nil(err)
					s.Equal(updateRep.Outcome.String(), pollRes.Outcome.String())

					s.EqualHistoryEvents(`
				  1 WorkflowExecutionStarted
				  2 WorkflowTaskScheduled
				  3 WorkflowTaskStarted
				  4 WorkflowTaskCompleted
				  5 WorkflowTaskScheduled
				  6 WorkflowTaskStarted
				  7 WorkflowTaskCompleted
				  8 WorkflowExecutionUpdateAccepted
				  9 WorkflowExecutionUpdateCompleted`, s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution()))
				})

				t.Run("and reject", func(t *testing.T) {
					s := testcore.NewEnv(t)
					// start workflow
					_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowReq(s, s.Tv()))
					s.NoError(err)

					_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(), taskpoller.DrainWorkflowTask)
					s.NoError(err)

					// update-with-start
					startReq := startWorkflowReq(s, s.Tv())
					startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
					updateReq := updateWorkflowRequest(s, s.Tv(),
						&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
					uwsCh := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

					_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
						func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
							return &workflowservice.RespondWorkflowTaskCompletedRequest{
								Messages: s.UpdateRejectMessages(s.Tv(), task.Messages[0]),
							}, nil
						})
					s.NoError(err)

					uwsRes := <-uwsCh
					s.NoError(uwsRes.err)
					startResp := uwsRes.response.Responses[0].GetStartWorkflow()
					updateRep := uwsRes.response.Responses[1].GetUpdateWorkflow()
					requireNotStartedButRunning(s.T(), startResp)
					s.Equal("rejection-of-"+s.Tv().UpdateID(), updateRep.GetOutcome().GetFailure().GetMessage())

					// poll update to ensure same outcome is returned
					_, err = pollUpdate(s, s.Tv(),
						&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
					s.Error(err)
					s.ErrorAs(err, new(*serviceerror.NotFound))

					s.EqualHistoryEvents(`
				  1 WorkflowExecutionStarted
				  2 WorkflowTaskScheduled
				  3 WorkflowTaskStarted
				  4 WorkflowTaskCompleted
				`, s.GetHistory(s.Namespace().String(), s.Tv().WorkflowExecution()))
				})
			})

			t.Run("workflow id conflict policy terminate-existing", func(t *testing.T) {
				t.Run("terminate workflow first, then start and update", func(t *testing.T) {
					s := testcore.NewEnv(t)
					// start workflow
					firstWF, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowReq(s, s.Tv()))
					s.NoError(err)

					_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(), taskpoller.DrainWorkflowTask)
					s.NoError(err)

					// update-with-start
					startReq := startWorkflowReq(s, s.Tv())
					startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
					updateReq := updateWorkflowRequest(s, s.Tv(),
						&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
					uwsCh := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

					_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
						func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
							return &workflowservice.RespondWorkflowTaskCompletedRequest{
								Messages: s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]),
							}, nil
						})
					s.NoError(err)

					uwsRes := <-uwsCh
					s.NoError(uwsRes.err)
					startResp := uwsRes.response.Responses[0].GetStartWorkflow()
					updateRep := uwsRes.response.Responses[1].GetUpdateWorkflow()
					requireStartedAndRunning(s.T(), startResp)
					s.Equal(startResp.RunId, updateRep.UpdateRef.GetWorkflowExecution().RunId)
					s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateRep.GetOutcome().GetSuccess()))

					// ensure workflow was terminated
					descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(),
						&workflowservice.DescribeWorkflowExecutionRequest{
							Namespace: s.Namespace().String(),
							Execution: &commonpb.WorkflowExecution{WorkflowId: startReq.WorkflowId, RunId: firstWF.RunId},
						})
					s.NoError(err)
					s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.WorkflowExecutionInfo.Status)

					// poll update to ensure same outcome is returned
					pollRes, err := pollUpdate(s, s.Tv(),
						&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
					s.NoError(err)
					s.Equal(updateRep.Outcome.String(), pollRes.Outcome.String())
				})

				t.Run("given an accepted update, attach to it", func(t *testing.T) {
					s := testcore.NewEnv(t)
					// 1st update-with-start
					startReq := startWorkflowReq(s, s.Tv())
					startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
					updateReq := updateWorkflowRequest(s, s.Tv(),
						&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
					uwsCh1 := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

					// accept the update
					_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
						func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
							return &workflowservice.RespondWorkflowTaskCompletedRequest{
								Messages: s.UpdateAcceptMessages(s.Tv(), task.Messages[0]),
							}, nil
						})
					s.NoError(err)

					uwsRes1 := <-uwsCh1
					s.NoError(uwsRes1.err)
					startResp1 := uwsRes1.response.Responses[0].GetStartWorkflow()
					updateRep1 := uwsRes1.response.Responses[1].GetUpdateWorkflow()
					s.True(startResp1.Started)
					s.Equal(startResp1.RunId, updateRep1.UpdateRef.GetWorkflowExecution().RunId)
					s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, updateRep1.Stage)

					// 2nd update-with-start: attaches to update instead of terminating workflow
					uwsCh2 := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

					uwsRes2 := <-uwsCh2
					s.NoError(uwsRes2.err)
					startResp2 := uwsRes2.response.Responses[0].GetStartWorkflow()
					updateRep2 := uwsRes2.response.Responses[1].GetUpdateWorkflow()
					s.False(startResp2.Started)
					s.Equal(startResp2.RunId, startResp1.RunId) // no termination
					s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, updateRep2.Stage)
				})
			})

			t.Run("workflow id conflict policy fail: abort multi operation", func(t *testing.T) {
				s := testcore.NewEnv(t)
				_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowReq(s, s.Tv()))
				s.NoError(err)

				// start workflow
				startWorkflowReq(s, s.Tv())
				_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(), taskpoller.DrainWorkflowTask)
				s.NoError(err)

				// update-with-start
				startReq := startWorkflowReq(s, s.Tv())
				startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
				updateReq := updateWorkflowRequest(s, s.Tv(), &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
				uwsCh := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)
				uwsRes := <-uwsCh
				s.Error(uwsRes.err)
				s.Equal("Update-with-Start could not be executed.", uwsRes.err.Error())
				errs := uwsRes.err.(*serviceerror.MultiOperationExecution).OperationErrors()
				s.Len(errs, 2)
				var alreadyStartedErr *serviceerror.WorkflowExecutionAlreadyStarted
				s.ErrorAs(errs[0], &alreadyStartedErr)
				s.Equal("Operation was aborted.", errs[1].Error())
			})

			t.Run("receive completed update result", func(t *testing.T) {
				_ = testcore.NewEnv(t) // unused s
				for _, p := range []enumspb.WorkflowIdConflictPolicy{
					enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
					enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
					enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
				} {
					t.Run(fmt.Sprintf("for workflow id conflict policy %v", p), func(t *testing.T) {
						s := testcore.NewEnv(t)
						startReq := startWorkflowReq(s, s.Tv())
						updReq := updateWorkflowRequest(s, s.Tv(),
							&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})

						// 1st update-with-start
						uwsCh1 := sendUpdateWithStart(s, testcore.NewContext(), startReq, updReq)
						_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
							func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
								return &workflowservice.RespondWorkflowTaskCompletedRequest{
									Messages: s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]),
								}, nil
							})
						s.NoError(err)
						uwsRes1 := <-uwsCh1
						s.NoError(uwsRes1.err)

						// 2nd update-with-start: using *same* UpdateID - but *different* RequestID
						uwsRes2 := <-sendUpdateWithStart(s, testcore.NewContext(), startReq, updReq)
						s.NoError(uwsRes2.err)

						s.Equal(uwsRes1.response.Responses[0].GetStartWorkflow().RunId, uwsRes2.response.Responses[0].GetStartWorkflow().RunId)
						s.Equal(uwsRes1.response.Responses[1].GetUpdateWorkflow().Outcome.String(), uwsRes2.response.Responses[1].GetUpdateWorkflow().Outcome.String())

						// poll update to ensure same outcome is returned
						pollRes, err := pollUpdate(s, s.Tv(),
							&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
						s.NoError(err)
						s.Equal(uwsRes1.response.Responses[1].GetUpdateWorkflow().Outcome.String(), pollRes.Outcome.String())
					})
				}
			})

			t.Run("dedupes start", func(t *testing.T) {
				_ = testcore.NewEnv(t) // unused s
				for _, p := range []enumspb.WorkflowIdConflictPolicy{
					enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
					enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
					enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
				} {
					t.Run(fmt.Sprintf("for workflow id conflict policy %v", p), func(t *testing.T) {
						s := testcore.NewEnv(t)
						startReq := startWorkflowReq(s, s.Tv())
						startReq.RequestId = "request_id"
						startReq.WorkflowIdConflictPolicy = p
						updReq1 := updateWorkflowRequest(s, s.Tv().WithUpdateIDNumber(1),
							&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})

						// 1st update-with-start
						uwsCh1 := sendUpdateWithStart(s, testcore.NewContext(), startReq, updReq1)
						_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
							func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
								return &workflowservice.RespondWorkflowTaskCompletedRequest{
									Messages: s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]),
								}, nil
							})
						s.NoError(err)
						uwsRes1 := <-uwsCh1
						s.NoError(uwsRes1.err)

						// 2nd update-with-start: using *same* RequestID - but *different* UpdateID
						updReq2 := updateWorkflowRequest(s, s.Tv().WithUpdateIDNumber(2),
							&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
						uwsCh2 := sendUpdateWithStart(s, testcore.NewContext(), startReq, updReq2)
						_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
							func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
								return &workflowservice.RespondWorkflowTaskCompletedRequest{
									Messages: s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]),
								}, nil
							})
						s.NoError(err)
						uwsRes2 := <-uwsCh2
						s.NoError(uwsRes1.err)

						s.Equal(uwsRes1.response.Responses[0].GetStartWorkflow().RunId, uwsRes2.response.Responses[0].GetStartWorkflow().RunId)
					})
				}
			})
		})

		t.Run("workflow is closed", func(t *testing.T) {
			t.Run("workflow id reuse policy allow-duplicate", func(t *testing.T) {
				s := testcore.NewEnv(t)
				// start and terminate workflow
				initialWorkflow, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowReq(s, s.Tv()))
				s.NoError(err)

				_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(), taskpoller.DrainWorkflowTask)
				s.NoError(err)

				_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(),
					&workflowservice.TerminateWorkflowExecutionRequest{
						Namespace:         s.Namespace().String(),
						WorkflowExecution: s.Tv().WorkflowExecution(),
						Reason:            s.Tv().Any().String(),
					})
				s.NoError(err)

				// update-with-start
				startReq := startWorkflowReq(s, s.Tv())
				startReq.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
				updateReq := updateWorkflowRequest(s, s.Tv(),
					&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
				uwsCh := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

				_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
					func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
						return &workflowservice.RespondWorkflowTaskCompletedRequest{
							Messages: s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]),
						}, nil
					})
				s.NoError(err)

				uwsRes := <-uwsCh
				s.NoError(uwsRes.err)
				startResp := uwsRes.response.Responses[0].GetStartWorkflow()
				updateRep := uwsRes.response.Responses[1].GetUpdateWorkflow()
				requireStartedAndRunning(s.T(), startResp)
				s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateRep.GetOutcome().GetSuccess()))

				// ensure terminated workflow is not locked by update-with-start
				err = s.SendSignal(s.Namespace().String(), &commonpb.WorkflowExecution{
					WorkflowId: s.Tv().WorkflowID(),
					RunId:      initialWorkflow.RunId,
				}, s.Tv().Any().String(), s.Tv().Any().Payloads(), s.Tv().Any().String())
				s.ErrorContains(err, "workflow execution already completed")

				// poll update to ensure same outcome is returned
				pollRes, err := pollUpdate(s, s.Tv(),
					&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
				s.NoError(err)
				s.Equal(updateRep.Outcome.String(), pollRes.Outcome.String())
			})

			t.Run("workflow id reuse policy reject-duplicate", func(t *testing.T) {
				s := testcore.NewEnv(t)
				// start and terminate workflow
				_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowReq(s, s.Tv()))
				s.NoError(err)

				_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(), taskpoller.DrainWorkflowTask)
				s.NoError(err)

				_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(),
					&workflowservice.TerminateWorkflowExecutionRequest{
						Namespace:         s.Namespace().String(),
						WorkflowExecution: s.Tv().WorkflowExecution(),
						Reason:            s.Tv().Any().String(),
					})
				s.NoError(err)

				// update-with-start
				startReq := startWorkflowReq(s, s.Tv())
				startReq.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
				updateReq := updateWorkflowRequest(s, s.Tv(),
					&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
				uwsCh := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

				uwsRes := <-uwsCh
				s.Error(uwsRes.err)
				s.Equal("Update-with-Start could not be executed.", uwsRes.err.Error())
				errs := uwsRes.err.(*serviceerror.MultiOperationExecution).OperationErrors()
				s.Len(errs, 2)
				s.Contains(errs[0].Error(), "Workflow execution already finished")
				var alreadyStartedErr *serviceerror.WorkflowExecutionAlreadyStarted
				s.ErrorAs(errs[0], &alreadyStartedErr)
				s.Equal("Operation was aborted.", errs[1].Error())
			})

			t.Run("receive completed update result", func(t *testing.T) {
				_ = testcore.NewEnv(t) // unused s
				for _, p := range []enumspb.WorkflowIdConflictPolicy{
					enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
					enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
					enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
				} {
					t.Run(fmt.Sprintf("for workflow id conflict policy %v", p), func(t *testing.T) {
						s := testcore.NewEnv(t)
						// 1st update-with-start
						startReq := startWorkflowReq(s, s.Tv())
						startReq.WorkflowIdConflictPolicy = p
						updateReq := updateWorkflowRequest(s, s.Tv(),
							&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
						uwsCh := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

						_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
							func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
								return &workflowservice.RespondWorkflowTaskCompletedRequest{
									Messages: s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]),
									Commands: s.UpdateAcceptCompleteCommands(s.Tv()),
								}, nil
							})
						s.NoError(err)

						uwsRes := <-uwsCh
						s.NoError(uwsRes.err)
						startResp1 := uwsRes.response.Responses[0].GetStartWorkflow()
						_ = uwsRes.response.Responses[1].GetUpdateWorkflow()
						requireStartedAndRunning(s.T(), startResp1)

						// terminate workflow
						_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(),
							&workflowservice.TerminateWorkflowExecutionRequest{
								Namespace:         s.Namespace().String(),
								WorkflowExecution: s.Tv().WorkflowExecution(),
								Reason:            s.Tv().Any().String(),
							})
						s.NoError(err)

						// 2nd update-with-start (using the same Update ID but different Request ID)
						uwsRes = <-sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

						s.NoError(uwsRes.err)
						startResp := uwsRes.response.Responses[0].GetStartWorkflow()
						updateRep := uwsRes.response.Responses[1].GetUpdateWorkflow()
						s.False(startResp.Started)
						s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, startResp.Status)
						// TODO: check startResp.Running
						s.Equal("success-result-of-"+s.Tv().UpdateID(), testcore.DecodeString(s.T(), updateRep.GetOutcome().GetSuccess()))
					})
				}
			})
		})

		t.Run("workflow start conflict", func(t *testing.T) {
			t.Run("workflow id conflict policy fail: use-existing", func(t *testing.T) {
				s := testcore.NewEnv(t)
				startReq := startWorkflowReq(s, s.Tv())
				startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
				updateReq := updateWorkflowRequest(s, s.Tv(),
					&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})

				// simulate a race condition
				s.InjectHook(testhooks.NewHook(testhooks.UpdateWithStartInBetweenLockAndStart, func() {
					_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startReq)
					s.NoError(err)
				}))

				uwsCh := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

				_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
					func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
						return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
					})
				s.NoError(err)

				_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
					func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
						return &workflowservice.RespondWorkflowTaskCompletedRequest{
							Messages: s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]),
						}, nil
					})
				s.NoError(err)

				<-uwsCh
			})
		})

		t.Run("update is aborted by closing workflow", func(t *testing.T) {
			t.Run("retry request once when workflow was not started", func(t *testing.T) {
				s := testcore.NewEnv(t)
				// start workflow
				_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowReq(s, s.Tv()))
				s.NoError(err)
				_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(), taskpoller.DrainWorkflowTask)
				s.NoError(err)

				// update-with-start
				startReq := startWorkflowReq(s, s.Tv())
				startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
				updateReq := updateWorkflowRequest(s, s.Tv(),
					&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
				uwsCh := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

				// wait until the update is admitted - then complete workflow
				waitUpdateAdmitted(s, s.Tv())
				_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
					func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
						return &workflowservice.RespondWorkflowTaskCompletedRequest{
							Commands: []*commandpb.Command{
								{
									CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
									Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
										CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
									},
								},
							},
						}, nil
					})
				s.NoError(err)

				// update-with-start will do a server-side retry

				_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
					func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
						return &workflowservice.RespondWorkflowTaskCompletedRequest{
							Messages: s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]),
						}, nil
					})
				s.NoError(err)

				uwsRes := <-uwsCh
				s.NoError(uwsRes.err)
			})

			t.Run("return retryable error after retry", func(t *testing.T) {
				s := testcore.NewEnv(t)
				// start workflow
				_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowReq(s, s.Tv()))
				s.NoError(err)
				_, err = s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(), taskpoller.DrainWorkflowTask)
				s.NoError(err)

				// update-with-start
				startReq := startWorkflowReq(s, s.Tv())
				startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
				updateReq := updateWorkflowRequest(s, s.Tv(),
					&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
				uwsCh := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

				// wait until the update is admitted
				waitUpdateAdmitted(s, s.Tv())

				s.InjectHook(testhooks.NewHook(testhooks.UpdateWithStartOnClosingWorkflowRetry, func() {
					_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowReq(s, s.Tv()))
					s.NoError(err)
				}))

				// complete workflow (twice including retry)
				for range 2 {
					_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
						func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
							return &workflowservice.RespondWorkflowTaskCompletedRequest{
								Commands: []*commandpb.Command{
									{
										CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
										Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
											CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
										},
									},
								},
							}, nil
						})
					s.NoError(err)
				}

				// ensure update-with-start returns retryable error
				uwsRes := <-uwsCh
				s.Error(uwsRes.err)
				errs := uwsRes.err.(*serviceerror.MultiOperationExecution).OperationErrors()
				s.Len(errs, 2)
				s.Equal("Operation was aborted.", errs[0].Error())
				s.ErrorContains(errs[1], update.AbortedByWorkflowClosingErr.Error())
				s.ErrorAs(errs[1], new(*serviceerror.Aborted))
			})

			t.Run("do not retry when workflow was started", func(t *testing.T) {
				s := testcore.NewEnv(t)
				// update-with-start
				startReq := startWorkflowReq(s, s.Tv())
				startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
				updateReq := updateWorkflowRequest(s, s.Tv(),
					&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
				uwsCh := sendUpdateWithStart(s, testcore.NewContext(), startReq, updateReq)

				// wait until the update is admitted - then complete workflow
				waitUpdateAdmitted(s, s.Tv())
				_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
					func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
						return &workflowservice.RespondWorkflowTaskCompletedRequest{
							Commands: []*commandpb.Command{
								{
									CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
									Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
										CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
									},
								},
							},
						}, nil
					})
				s.NoError(err)

				uwsRes := <-uwsCh
				s.Error(uwsRes.err)
				errs := uwsRes.err.(*serviceerror.MultiOperationExecution).OperationErrors()
				s.Len(errs, 2)
				s.ErrorContains(errs[1], update.AbortedByWorkflowClosingErr.Error())
			})
		})

		t.Run("return update rate limit error", func(t *testing.T) {
			// lower maximum total number of updates for testing purposes
			s := testcore.NewEnv(t,
				testcore.WithDynamicConfig(dynamicconfig.WorkflowExecutionMaxTotalUpdates, 1),
			)

			ctx := testcore.NewContext()
			startReq := startWorkflowReq(s, s.Tv())
			startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING

			// allows 1st
			updateReq := updateWorkflowRequest(s, s.Tv().WithUpdateIDNumber(0),
				&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
			uwsCh := sendUpdateWithStart(s, ctx, startReq, updateReq)
			_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
				func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
					return &workflowservice.RespondWorkflowTaskCompletedRequest{
						Messages: s.UpdateAcceptCompleteMessages(s.Tv(), task.Messages[0]),
					}, nil
				})
			s.NoError(err)
			uwsRes := <-uwsCh
			s.NoError(uwsRes.err)

			// denies 2nd
			updateReq = updateWorkflowRequest(s, s.Tv().WithUpdateIDNumber(1), updateReq.WaitPolicy)
			select {
			case <-sendUpdateWithStart(s, ctx, startReq, updateReq):
				err = (<-sendUpdateWithStart(s, ctx, startReq, updateReq)).err
				s.Error(err)
				errs := err.(*serviceerror.MultiOperationExecution).OperationErrors()
				s.Len(errs, 2)
				s.Equal("Operation was aborted.", errs[0].Error())
				s.Contains(errs[1].Error(), "limit on the total number of distinct updates in this workflow has been reached")
			case <-ctx.Done():
				s.Fail("timed out waiting for update")
			}
		})

		t.Run("return update in-flight limit error", func(t *testing.T) {
			// lower maximum in-flight updates for testing purposes
			maxInFlight := 1
			s := testcore.NewEnv(t,
				testcore.WithDynamicConfig(dynamicconfig.WorkflowExecutionMaxInFlightUpdates, maxInFlight),
			)

			ctx := testcore.NewContext()
			startReq := startWorkflowReq(s, s.Tv())
			startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING

			// Start workflow and admit 1st update (but don't complete it)
			updateReq := updateWorkflowRequest(s, s.Tv().WithUpdateIDNumber(0),
				&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
			uwsCh := sendUpdateWithStart(s, ctx, startReq, updateReq)

			// Poll workflow task but only accept, don't complete the update
			_, err := s.TaskPoller().PollAndHandleWorkflowTask(s.Tv(),
				func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
					return &workflowservice.RespondWorkflowTaskCompletedRequest{
						Messages: s.UpdateAcceptMessages(s.Tv(), task.Messages[0]),
					}, nil
				})
			s.NoError(err)
			uwsRes := <-uwsCh
			s.NoError(uwsRes.err)

			// Try to send 2nd update-with-start while 1st is still in-flight (not completed)
			updateReq = updateWorkflowRequest(s, s.Tv().WithUpdateIDNumber(1), updateReq.WaitPolicy)
			uwsCh = sendUpdateWithStart(s, ctx, startReq, updateReq)
			select {
			case uwsRes := <-uwsCh:
				err = uwsRes.err
				s.Error(err)

				var multiOpsErr *serviceerror.MultiOperationExecution
				s.ErrorAs(err, &multiOpsErr)

				errs := multiOpsErr.OperationErrors()
				s.Len(errs, 2)
				s.Equal("Operation was aborted.", errs[0].Error())
				s.Contains(errs[1].Error(), "limit on number of concurrent in-flight updates has been reached")

				// Verify ResourceExhausted error is accessible with all details preserved
				var resExhausted *serviceerror.ResourceExhausted
				s.ErrorAs(errs[1], &resExhausted)
				s.Equal(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, resExhausted.Cause)
				s.Equal(enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, resExhausted.Scope)
				s.Contains(resExhausted.Message, "limit on number of concurrent in-flight updates")
			case <-ctx.Done():
				s.Fail("timed out waiting for update")
			}
		})
	})
}
