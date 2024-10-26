// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
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
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type UpdateWorkflowSuite struct {
	WorkflowUpdateBaseSuite
}

func TestUpdateWorkflowSuite(t *testing.T) {
	t.Parallel()
	s := new(UpdateWorkflowSuite)
	suite.Run(t, s)
}

// TODO: extract sendUpdate* methods to separate package.

func (s *UpdateWorkflowSuite) sendUpdate(ctx context.Context, tv *testvars.TestVars, updateID string) <-chan updateResponseErr {
	s.T().Helper()
	return s.sendUpdateInternal(ctx, tv, updateID, nil, false)
}

func (s *UpdateWorkflowSuite) sendUpdateNoError(tv *testvars.TestVars, updateID string) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	return s.sendUpdateNoErrorInternal(tv, updateID, nil)
}

func (s *UpdateWorkflowSuite) sendUpdateNoErrorWaitPolicyAccepted(tv *testvars.TestVars, updateID string) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	return s.sendUpdateNoErrorInternal(tv, updateID, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
}

func (s *UpdateWorkflowSuite) pollUpdate(tv *testvars.TestVars, updateID string, waitPolicy *updatepb.WaitPolicy) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
	s.T().Helper()
	return s.FrontendClient().PollWorkflowExecutionUpdate(testcore.NewContext(), &workflowservice.PollWorkflowExecutionUpdateRequest{
		Namespace: s.Namespace(),
		UpdateRef: &updatepb.UpdateRef{
			WorkflowExecution: tv.WorkflowExecution(),
			UpdateId:          tv.UpdateID(updateID),
		},
		WaitPolicy: waitPolicy,
	})
}

// Simulating a graceful shard closure. The shard finalizer will clear the workflow context,
// any update requests are aborted and the frontend retries any in-flight update requests.
func (s *UpdateWorkflowSuite) clearUpdateRegistryAndAbortPendingUpdates(tv *testvars.TestVars) {
	s.closeShard(tv.WorkflowID())
}

// Simulating an unexpected loss of the update registry due to a crash. The shard finalizer won't run,
// therefore the workflow context is NOT cleared, pending update requests are NOT aborted and will time out.
func (s *UpdateWorkflowSuite) loseUpdateRegistryAndAbandonPendingUpdates(tv *testvars.TestVars) {
	cleanup := s.OverrideDynamicConfig(dynamicconfig.ShardFinalizerTimeout, 0)
	defer cleanup()
	s.closeShard(tv.WorkflowID())
}

func (s *UpdateWorkflowSuite) speculativeWorkflowTaskOutcomes(
	snap map[string][]*metricstest.CapturedRecording,
) (commits, rollbacks int) {
	for _ = range snap[metrics.SpeculativeWorkflowTaskCommits.Name()] {
		commits += 1
	}
	for _ = range snap[metrics.SpeculativeWorkflowTaskRollbacks.Name()] {
		rollbacks += 1
	}
	return
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_EmptySpeculativeWorkflowTask_AcceptComplete() {
	testCases := []struct {
		Name     string
		UseRunID bool
	}{
		{
			Name:     "with RunID",
			UseRunID: true,
		},
		{
			Name:     "without RunID",
			UseRunID: false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T())

			tv = s.startWorkflow(tv)
			runID := tv.RunID()
			if !tc.UseRunID {
				// Clear RunID in tv to test code paths when APIs have to fetch current RunID themselves.
				tv = tv.WithRunID("")
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
					return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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

					s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
					s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
					s.EqualValues(5, updRequestMsg.GetEventId())

					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
				default:
					s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
					return nil, nil
				}
			}

			poller := &testcore.TaskPoller{
				Client:              s.FrontendClient(),
				Namespace:           s.Namespace(),
				TaskQueue:           tv.TaskQueue(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			// Drain first WT.
			_, err := poller.PollAndProcessWorkflowTask()
			s.NoError(err)

			updateResultCh := s.sendUpdateNoError(tv, "1")

			// Process update in workflow.
			res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
			s.NoError(err)
			s.NotNil(res.NewTask)
			updateResult := <-updateResultCh
			s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
			s.EqualValues(0, res.NewTask.ResetHistoryEventId)

			// Test non-blocking poll
			for _, waitPolicy := range []*updatepb.WaitPolicy{{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED}, nil} {
				pollUpdateResp, err := s.pollUpdate(tv, "1", waitPolicy)
				s.NoError(err)
				s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, pollUpdateResp.Stage)
				s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), pollUpdateResp.Outcome.GetSuccess()))
				// Even if tv doesn't have RunID, it should be returned as part of UpdateRef.
				s.Equal(runID, pollUpdateResp.UpdateRef.GetWorkflowExecution().RunId)
			}

			s.Equal(2, wtHandlerCalls)
			s.Equal(2, msgHandlerCalls)

			commits, rollbacks := s.speculativeWorkflowTaskOutcomes(capture.Snapshot())
			s.Equal(1, commits)
			s.Equal(0, rollbacks)

			events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_NotEmptySpeculativeWorkflowTask_AcceptComplete() {
	testCases := []struct {
		Name     string
		UseRunID bool
	}{
		{
			Name:     "with RunID",
			UseRunID: true,
		},
		{
			Name:     "without RunID",
			UseRunID: false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T())

			tv = s.startWorkflow(tv)
			if !tc.UseRunID {
				tv = tv.WithRunID("")
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
							ActivityId:             tv.ActivityID(),
							ActivityType:           tv.ActivityType(),
							TaskQueue:              tv.TaskQueue(),
							ScheduleToCloseTimeout: tv.InfiniteTimeout(),
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
					return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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

					s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
					s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
					s.EqualValues(6, updRequestMsg.GetEventId())

					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
				default:
					s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
					return nil, nil
				}
			}

			poller := &testcore.TaskPoller{
				Client:              s.FrontendClient(),
				Namespace:           s.Namespace(),
				TaskQueue:           tv.TaskQueue(),
				Identity:            tv.WorkerIdentity(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			// Drain first WT.
			_, err := poller.PollAndProcessWorkflowTask()
			s.NoError(err)

			updateResultCh := s.sendUpdateNoError(tv, "1")

			// Process update in workflow.
			res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
			s.NoError(err)
			s.NotNil(res)
			updateResult := <-updateResultCh
			s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
			s.EqualValues(0, res.NewTask.ResetHistoryEventId)

			s.Equal(2, wtHandlerCalls)
			s.Equal(2, msgHandlerCalls)

			events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_FirstNormalScheduledWorkflowTask_AcceptComplete() {

	testCases := []struct {
		Name     string
		UseRunID bool
	}{
		{
			Name:     "with RunID",
			UseRunID: true,
		},
		{
			Name:     "without RunID",
			UseRunID: false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T())

			tv = s.startWorkflow(tv)
			if !tc.UseRunID {
				tv = tv.WithRunID("")
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
					return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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

					s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
					s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
					s.EqualValues(2, updRequestMsg.GetEventId())

					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
				default:
					s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
					return nil, nil
				}
			}

			poller := &testcore.TaskPoller{
				Client:              s.FrontendClient(),
				Namespace:           s.Namespace(),
				TaskQueue:           tv.TaskQueue(),
				Identity:            tv.WorkerIdentity(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			updateResultCh := s.sendUpdateNoError(tv, "1")

			// Process update in workflow.
			res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
			s.NoError(err)
			s.NotNil(res)

			updateResult := <-updateResultCh
			s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
			s.EqualValues(0, res.NewTask.ResetHistoryEventId)

			s.Equal(1, wtHandlerCalls)
			s.Equal(1, msgHandlerCalls)

			events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_NormalScheduledWorkflowTask_AcceptComplete() {

	testCases := []struct {
		Name     string
		UseRunID bool
	}{
		{
			Name:     "with RunID",
			UseRunID: true,
		},
		{
			Name:     "without RunID",
			UseRunID: false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T())

			tv = s.startWorkflow(tv)
			if !tc.UseRunID {
				tv = tv.WithRunID("")
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
					return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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
					s.Require().True(len(task.Messages) > 0, "Task has no messages", task)
					updRequestMsg := task.Messages[0]
					updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

					s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
					s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
					s.EqualValues(6, updRequestMsg.GetEventId())

					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
				default:
					s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
					return nil, nil
				}
			}

			poller := &testcore.TaskPoller{
				Client:              s.FrontendClient(),
				Namespace:           s.Namespace(),
				TaskQueue:           tv.TaskQueue(),
				Identity:            tv.WorkerIdentity(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			// Drain first WT.
			_, err := poller.PollAndProcessWorkflowTask()
			s.NoError(err)

			// Send signal to schedule new WT.
			err = s.SendSignal(s.Namespace(), tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
			s.NoError(err)

			updateResultCh := s.sendUpdateNoError(tv, "1")

			// Process update in workflow. It will be attached to existing WT.
			res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
			s.NoError(err)
			s.NotNil(res)

			updateResult := <-updateResultCh
			s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
			s.EqualValues(0, res.NewTask.ResetHistoryEventId)

			s.Equal(2, wtHandlerCalls)
			s.Equal(2, msgHandlerCalls)

			events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_RunningWorkflowTask_NewEmptySpeculativeWorkflowTask_Rejected() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	var updateResultCh <-chan *workflowservice.UpdateWorkflowExecutionResponse

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Send update after 1st WT has started.
			updateResultCh = s.sendUpdateNoError(tv, "1")
			// Completes WT with empty command list to create next WFT w/o events.
			return nil, nil
		case 2:
			s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT2 which was created while completing WT1.
  6 WorkflowTaskStarted`, task.History)
			// Message handler rejects update.
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
		case 1:
			return nil, nil
		case 2:
			s.Len(task.Messages, 1)
			updRequestMsg := task.Messages[0]
			s.EqualValues(5, updRequestMsg.GetEventId())

			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
		case 3:
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(3, wt2Resp.ResetHistoryEventId)

	// Send signal to create WT.
	err = s.SendSignal(s.Namespace(), tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
	s.NoError(err)

	// Complete workflow.
	completeWorkflowResp, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	commits, rollbacks := s.speculativeWorkflowTaskOutcomes(capture.Snapshot())
	s.Equal(0, commits)
	s.Equal(1, rollbacks)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_RunningWorkflowTask_NewNotEmptySpeculativeWorkflowTask_Rejected() {

	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

	var updateResultCh <-chan *workflowservice.UpdateWorkflowExecutionResponse

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Send update after 1st WT has started.
			updateResultCh = s.sendUpdateNoError(tv, "1")
			// Completes WT with update unrelated commands to create events that will be in the next speculative WFT.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}}, nil
		case 2:
			s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 WorkflowTaskScheduled // Speculative WFT2 with event (5) which was created while completing WFT1.
  7 WorkflowTaskStarted`, task.History)
			// Message handler rejects update.
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
		case 1:
			return nil, nil
		case 2:
			s.Len(task.Messages, 1)
			updRequestMsg := task.Messages[0]
			s.EqualValues(6, updRequestMsg.GetEventId())

			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
		case 3:
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		return tv.Any().Payloads(), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
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

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_CompletedWorkflow() {
	s.Run("receive outcome from completed Update", func() {
		tv := testvars.New(s.T())
		tv = s.startWorkflow(tv)

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				res := s.UpdateAcceptCompleteCommands(tv, "1")
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
				return s.UpdateAcceptCompleteMessages(tv, task.Messages[0], "1"), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace(),
			TaskQueue:           tv.TaskQueue(),
			Identity:            tv.WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)

		// Send Update request.
		updateResultCh := s.sendUpdateNoError(tv, "1")

		// Complete Update and Workflow.
		_, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)

		// Receive Update result.
		updateResult1 := <-updateResultCh
		s.NotNil(updateResult1.GetOutcome().GetSuccess())

		// Send same Update request again, receiving the same Update result.
		updateResultCh = s.sendUpdateNoError(tv, "1")
		updateResult2 := <-updateResultCh
		s.EqualValues(updateResult1, updateResult2)
	})

	s.Run("receive update failure from accepted Update", func() {
		tv := testvars.New(s.T())
		tv = s.startWorkflow(tv)

		wtHandlerCalls := 0
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			wtHandlerCalls++
			switch wtHandlerCalls {
			case 1:
				// Completes first WT with empty command list.
				return nil, nil
			case 2:
				res := s.UpdateAcceptCommands(tv, "1")
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
				return s.UpdateAcceptMessages(tv, task.Messages[0], "1"), nil
			default:
				s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
				return nil, nil
			}
		}

		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace(),
			TaskQueue:           tv.TaskQueue(),
			Identity:            tv.WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			MessageHandler:      msgHandler,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// Drain first WT.
		_, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)

		// Send Update request.
		updateResultCh := s.sendUpdate(testcore.NewContext(), tv, "1")

		// Accept Update and complete Workflow.
		_, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
		s.NoError(err)

		// Receive Update result.
		updateResult1 := <-updateResultCh
		s.NoError(updateResult1.err)
		s.Equal("Workflow Update failed because the Workflow completed before the Update completed.", updateResult1.response.GetOutcome().GetFailure().GetMessage())

		// Send same Update request again, receiving the same failure.
		updateResultCh = s.sendUpdate(testcore.NewContext(), tv, "1")
		updateResult2 := <-updateResultCh
		s.NoError(updateResult2.err)
		s.Equal("Workflow Update failed because the Workflow completed before the Update completed.", updateResult2.response.GetOutcome().GetFailure().GetMessage())
	})
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_ValidateWorkerMessages() {
	testCases := []struct {
		Name                     string
		RespondWorkflowTaskError string
		MessageFn                func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message
		CommandFn                func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command
	}{
		{
			Name:                     "message-update-id-not-found-and-accepted-request-not-set",
			RespondWorkflowTaskError: "wasn't found",
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-accepted"),
						ProtocolInstanceId: tv.WithUpdateID("bogus-update-id").UpdateID(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  nil, // Important not to pass original request back.
						}),
					},
				}
			},
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-accepted"),
						}},
					},
				}
			},
		},
		{
			Name:                     "message-update-id-not-found-and-accepted-request-is-set",
			RespondWorkflowTaskError: "",
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-accepted"),
						ProtocolInstanceId: tv.WithUpdateID("lost-update-id").UpdateID(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest, // Update will be resurrected from original request.
						}),
					},
				}
			},
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-accepted"),
						}},
					},
				}
			},
		},
		{
			Name:                     "command-reference-missed-message",
			RespondWorkflowTaskError: "referenced absent message ID",
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.Any().String(),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
				}
			},
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-accepted"),
						}},
					},
				}
			},
		},
		{
			Name:                     "complete-without-accept",
			RespondWorkflowTaskError: "invalid state transition attempted",
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-completed"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Response{
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
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-completed"),
						}},
					},
				}
			},
		},
		{
			Name:                     "accept-twice",
			RespondWorkflowTaskError: "invalid state transition attempted",
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-accepted", "1"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
					{
						Id:                 tv.MessageID("update-accepted", "2"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
				}
			},
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-accepted", "1"),
						}},
					},
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-accepted", "2"),
						}},
					},
				}
			},
		},
		{
			Name:                     "success-case",
			RespondWorkflowTaskError: "",
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-accepted"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
					{
						Id:                 tv.MessageID("update-completed"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Response{
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
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
				return []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-accepted"),
						}},
					},
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-completed"),
						}},
					},
				}
			},
		},
		{
			Name:                     "success-case-no-commands", // PROTOCOL_MESSAGE commands are optional.
			RespondWorkflowTaskError: "",
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.Any().String(),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
					{
						Id:                 tv.Any().String(),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Response{
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
			MessageFn: func(tv *testvars.TestVars, reqMsg *protocolpb.Message) []*protocolpb.Message {
				updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), reqMsg.GetBody())
				return []*protocolpb.Message{
					{
						Id:                 tv.MessageID("update-accepted"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
							AcceptedRequestMessageId:         reqMsg.GetId(),
							AcceptedRequestSequencingEventId: reqMsg.GetEventId(),
							AcceptedRequest:                  updRequest,
						}),
					},
					{
						Id:                 tv.MessageID("update-completed"),
						ProtocolInstanceId: updRequest.GetMeta().GetUpdateId(),
						SequencingId:       nil,
						Body: protoutils.MarshalAny(s.T(), &updatepb.Response{
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
			CommandFn: func(tv *testvars.TestVars, history *historypb.History) []*commandpb.Command {
				return []*commandpb.Command{
					// Complete command goes before Accept command.
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-completed"),
						}},
					},
					{
						CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
						Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
							MessageId: tv.MessageID("update-accepted"),
						}},
					},
				}
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T())

			tv = s.startWorkflow(tv)

			wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				if tc.CommandFn == nil {
					return nil, nil
				}
				return tc.CommandFn(tv, task.History), nil
			}

			msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
				if tc.MessageFn == nil {
					return nil, nil
				}
				updRequestMsg := task.Messages[0]
				return tc.MessageFn(tv, updRequestMsg), nil
			}

			poller := &testcore.TaskPoller{
				Client:              s.FrontendClient(),
				Namespace:           s.Namespace(),
				TaskQueue:           tv.TaskQueue(),
				Identity:            tv.WorkerIdentity(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			halfSecondTimeoutCtx, cancel := context.WithTimeout(testcore.NewContext(), 500*time.Millisecond)
			defer cancel()
			updateResultCh := s.sendUpdate(halfSecondTimeoutCtx, tv, "1")

			// Process update in workflow.
			_, err := poller.PollAndProcessWorkflowTask()
			updateResult := <-updateResultCh
			if tc.RespondWorkflowTaskError != "" {
				require.Error(s.T(), err, "RespondWorkflowTaskCompleted should return an error contains `%v`", tc.RespondWorkflowTaskError)
				require.Contains(s.T(), err.Error(), tc.RespondWorkflowTaskError)

				// When worker returns validation error, API caller got timeout error.
				require.Error(s.T(), updateResult.err)
				require.True(s.T(), common.IsContextDeadlineExceededErr(updateResult.err), updateResult.err.Error())
				require.Nil(s.T(), updateResult.response)
			} else {
				require.NoError(s.T(), err)
				require.NoError(s.T(), updateResult.err)
			}
		})
	}
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_StickySpeculativeWorkflowTask_AcceptComplete() {
	testCases := []struct {
		Name     string
		UseRunID bool
	}{
		{
			Name:     "with RunID",
			UseRunID: true,
		},
		{
			Name:     "without RunID",
			UseRunID: false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T())

			tv = s.startWorkflow(tv)
			if !tc.UseRunID {
				tv = tv.WithRunID("")
			}

			wtHandlerCalls := 0
			wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				wtHandlerCalls++
				switch wtHandlerCalls {
				case 1:
					// Completes first WT with empty command list.
					return nil, nil
				case 2:
					// This WT contains partial history because sticky was enabled.
					s.EqualHistory(`
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted`, task.History)
					return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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

					s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
					s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
					s.EqualValues(5, updRequestMsg.GetEventId())

					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
				default:
					s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
					return nil, nil
				}
			}

			poller := &testcore.TaskPoller{
				Client:                       s.FrontendClient(),
				Namespace:                    s.Namespace(),
				TaskQueue:                    tv.TaskQueue(),
				StickyTaskQueue:              tv.StickyTaskQueue(),
				StickyScheduleToStartTimeout: 3 * time.Second,
				Identity:                     tv.WorkerIdentity(),
				WorkflowTaskHandler:          wtHandler,
				MessageHandler:               msgHandler,
				Logger:                       s.Logger,
				T:                            s.T(),
			}

			// Drain existing first WT from regular task queue, but respond with sticky queue enabled response, next WT will go to sticky queue.
			_, err := poller.PollAndProcessWorkflowTask(testcore.WithRespondSticky)
			s.NoError(err)

			go func() {
				// Process update in workflow task (it is sticky).
				res, err := poller.PollAndProcessWorkflowTask(testcore.WithPollSticky, testcore.WithoutRetries)
				require.NoError(s.T(), err)
				require.NotNil(s.T(), res)
				require.EqualValues(s.T(), 0, res.NewTask.ResetHistoryEventId)
			}()

			// This is to make sure that sticky poller above reached server first.
			// And when update comes, stick poller is already available.
			time.Sleep(500 * time.Millisecond) //nolint:forbidigo
			updateResult := <-s.sendUpdateNoError(tv, "1")

			s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))

			s.Equal(2, wtHandlerCalls)
			s.Equal(2, msgHandlerCalls)

			events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
	}
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_StickySpeculativeWorkflowTask_AcceptComplete_StickyWorkerUnavailable() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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

			s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(5, updRequestMsg.GetEventId())

			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:                       s.FrontendClient(),
		Namespace:                    s.Namespace(),
		TaskQueue:                    tv.TaskQueue(),
		StickyTaskQueue:              tv.StickyTaskQueue(),
		StickyScheduleToStartTimeout: 3 * time.Second,
		Identity:                     tv.WorkerIdentity(),
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
	updateResultCh := s.sendUpdateNoError(tv, "1")

	// Process update in workflow task from non-sticky task queue.
	res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	s.NotNil(res)
	updateResult := <-updateResultCh
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
	s.EqualValues(0, res.NewTask.ResetHistoryEventId)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_FirstNormalScheduledWorkflowTask_Reject() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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

			s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(2, updRequestMsg.GetEventId())

			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	updateResultCh := s.sendUpdateNoError(tv, "1")

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	s.Equal(1, wtHandlerCalls)
	s.Equal(1, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled // First normal WT was scheduled before update and therefore all 3 events have to be written even if update was rejected.
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted // Empty completed WT. No new events were created after it.
`, events)
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_EmptySpeculativeWorkflowTask_Reject() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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
			return nil, nil
		case 3:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled // Speculative WT was dropped and history starts from 5 again.
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
			updRequestMsg := task.Messages[0]
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(5, updRequestMsg.GetEventId())

			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
		case 3:
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	updateResultCh := s.sendUpdateNoError(tv, "1")

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(3, updateResp.ResetHistoryEventId)

	// Send signal to create WT.
	err = s.SendSignal(s.Namespace(), tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
	s.NoError(err)

	// Process signal and complete workflow.
	res, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	s.NotNil(res)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())
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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_NotEmptySpeculativeWorkflowTask_Reject() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
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
		case 1:
			return nil, nil
		case 2:
			updRequestMsg := task.Messages[0]
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(6, updRequestMsg.GetEventId())

			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
		case 3:
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		return tv.Any().Payloads(), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	updateResultCh := s.sendUpdateNoError(tv, "1")

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	s.NotNil(res)
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, res.NewTask.ResetHistoryEventId, "no reset of event ID should happened after update rejection if it was delivered with normal workflow task")

	err = poller.PollAndProcessActivityTask(false)
	s.NoError(err)

	// Complete workflow.
	res, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	s.NotNil(res)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())
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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_1stAccept_2ndAccept_2ndComplete_1stComplete() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, task.History)
			return append(s.UpdateAcceptCommands(tv, "1"), &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID("1"),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
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
			return append(s.UpdateAcceptCommands(tv, "2"), &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID("2"),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
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
			return s.UpdateCompleteCommands(tv, "2"), nil
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
			return s.UpdateCompleteCommands(tv, "1"), nil
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
			s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), upd1Request.GetInput().GetArgs()))
			s.EqualValues(2, upd1RequestMsg.GetEventId())
			return s.UpdateAcceptMessages(tv, upd1RequestMsg, "1"), nil
		case 2:
			upd2RequestMsg = task.Messages[0]
			upd2Request := protoutils.UnmarshalAny[*updatepb.Request](s.T(), upd2RequestMsg.GetBody())
			s.Equal("args-value-of-"+tv.UpdateID("2"), testcore.DecodeString(s.T(), upd2Request.GetInput().GetArgs()))
			s.EqualValues(7, upd2RequestMsg.GetEventId())
			return s.UpdateAcceptMessages(tv, upd2RequestMsg, "2"), nil
		case 3:
			s.NotNil(upd2RequestMsg)
			return s.UpdateCompleteMessages(tv, upd2RequestMsg, "2"), nil
		case 4:
			s.NotNil(upd1RequestMsg)
			return s.UpdateCompleteMessages(tv, upd1RequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		return tv.Any().Payloads(), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	updateResultCh1 := s.sendUpdateNoError(tv, "1")

	// Accept update1 in normal WT1.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Send 2nd update and create speculative WT2.
	updateResultCh2 := s.sendUpdateNoError(tv, "2")

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
	s.EqualValues("success-result-of-"+tv.UpdateID("2"), testcore.DecodeString(s.T(), updateResult2.GetOutcome().GetSuccess()))
	s.EqualValues(0, res.NewTask.ResetHistoryEventId)

	err = poller.PollAndProcessActivityTask(false)
	s.NoError(err)

	// Complete update1 in WT4.
	res, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	s.NotNil(res)
	updateResult1 := <-updateResultCh1
	s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, updateResult1.Stage)
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult1.GetOutcome().GetSuccess()))
	s.EqualValues(0, res.NewTask.ResetHistoryEventId)

	s.Equal(4, wtHandlerCalls)
	s.Equal(4, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_1stAccept_2ndReject_1stComplete() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, task.History)
			return append(s.UpdateAcceptCommands(tv, "1"), &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID("1"),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
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
			return s.UpdateCompleteCommands(tv, "1"), nil
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
			s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), upd1Request.GetInput().GetArgs()))
			s.EqualValues(2, upd1RequestMsg.GetEventId())
			return s.UpdateAcceptMessages(tv, upd1RequestMsg, "1"), nil
		case 2:
			upd2RequestMsg := task.Messages[0]
			upd2Request := protoutils.UnmarshalAny[*updatepb.Request](s.T(), upd2RequestMsg.GetBody())
			s.Equal("args-value-of-"+tv.UpdateID("2"), testcore.DecodeString(s.T(), upd2Request.GetInput().GetArgs()))
			s.EqualValues(7, upd2RequestMsg.GetEventId())
			return s.UpdateRejectMessages(tv, upd2RequestMsg, "2"), nil
		case 3:
			s.NotNil(upd1RequestMsg)
			return s.UpdateCompleteMessages(tv, upd1RequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		return tv.Any().Payloads(), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	updateResultCh1 := s.sendUpdateNoError(tv, "1")

	// Accept update1 in WT1.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Send 2nd update and create speculative WT2.
	updateResultCh2 := s.sendUpdateNoError(tv, "2")

	// Poll for WT2 which 2nd update. Reject update2.
	res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	s.NotNil(res)
	s.EqualValues(0, res.NewTask.ResetHistoryEventId, "no reset of event ID should happened after update rejection if it was delivered with workflow task which had events")

	updateResult2 := <-updateResultCh2
	s.Equal("rejection-of-"+tv.UpdateID("2"), updateResult2.GetOutcome().GetFailure().GetMessage())

	err = poller.PollAndProcessActivityTask(false)
	s.NoError(err)

	// Complete update1 in WT3.
	res, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	s.NotNil(res)
	updateResult1 := <-updateResultCh1
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult1.GetOutcome().GetSuccess()))
	s.EqualValues(0, res.NewTask.ResetHistoryEventId)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_SpeculativeWorkflowTask_Fail() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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
			return s.UpdateAcceptCommands(tv, "1"), nil
		case 3:
			s.Fail("should not be called because messageHandler returns error")
			return nil, nil
		case 4:
			s.Fail("should not be called because messageHandler returns error")
			return nil, nil
		case 5:
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskFailed
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted`, task.History)
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

			// Emulate bug in worker/SDK update handler code. Return malformed acceptance response.
			return []*protocolpb.Message{
				{
					Id:                 tv.MessageID("update-accepted", "1"),
					ProtocolInstanceId: tv.Any().String(),
					SequencingId:       nil,
					Body: protoutils.MarshalAny(s.T(), &updatepb.Acceptance{
						AcceptedRequestMessageId:         updRequestMsg.GetId(),
						AcceptedRequestSequencingEventId: updRequestMsg.GetEventId(),
						AcceptedRequest:                  nil, // must not be nil.
					}),
				},
			}, nil
		case 3:
			// 2nd attempt has same updates attached to it.
			updRequestMsg := task.Messages[0]
			s.EqualValues(8, updRequestMsg.GetEventId())
			wtHandlerCalls++ // because it won't be called for case 3 but counter should be in sync.
			// Fail WT one more time. Although 2nd attempt is normal WT, it is also transient and shouldn't appear in the history.
			// Returning error will cause the poller to fail WT.
			return nil, errors.New("malformed request") //nolint:goerr113
		case 4:
			// 3rd attempt UpdateWorkflowExecution call has timed out but the
			// update is still running
			updRequestMsg := task.Messages[0]
			s.EqualValues(8, updRequestMsg.GetEventId())
			wtHandlerCalls++ // because it won't be called for case 4 but counter should be in sync.
			// Fail WT one more time. This is transient WT and shouldn't appear in the history.
			// Returning error will cause the poller to fail WT.
			return nil, errors.New("malformed request") //nolint:goerr113
		case 5:
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	timeoutCtx, cancel := context.WithTimeout(testcore.NewContext(), 2*time.Second)
	defer cancel()
	updateResultCh := s.sendUpdate(timeoutCtx, tv, "1")

	// Try to accept update in workflow: get malformed response.
	_, err = poller.PollAndProcessWorkflowTask()
	s.Error(err)
	s.Contains(err.Error(), "wasn't found")
	// New normal (but transient) WT will be created but not returned.

	s.waitUpdateAdmitted(tv, "1")
	// Try to accept update in workflow 2nd time: get error. Poller will fail WT.
	_, err = poller.PollAndProcessWorkflowTask()
	// The error is from RespondWorkflowTaskFailed, which should go w/o error.
	s.NoError(err)

	// Wait for UpdateWorkflowExecution to timeout.
	// This does NOT remove update from registry
	updateResult := <-updateResultCh
	s.Error(updateResult.err)
	s.True(common.IsContextDeadlineExceededErr(updateResult.err), "UpdateWorkflowExecution must timeout after 2 seconds")
	s.Nil(updateResult.response)

	// Try to accept update in workflow 3rd time: get error. Poller will fail WT.
	_, err = poller.PollAndProcessWorkflowTask()
	// The error is from RespondWorkflowTaskFailed, which should go w/o error.
	s.NoError(err)

	// Complete workflow.
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	s.Equal(5, wtHandlerCalls)
	s.Equal(5, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())
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
 10 WorkflowTaskCompleted
 11 WorkflowExecutionCompleted`, events)
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_StartedSpeculativeWorkflowTask_ConvertToNormalBecauseOfBufferedSignal() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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
			err := s.SendSignal(s.Namespace(), tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
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
		case 1:
			return nil, nil
		case 2:
			updRequestMsg := task.Messages[0]

			s.EqualValues(5, updRequestMsg.GetEventId())

			// Update is rejected but corresponding speculative WT will be in the history anyway, because it was converted to normal due to buffered signal.
			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
		case 3:
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	updateResultCh := s.sendUpdateNoError(tv, "1")

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	// Complete workflow.
	completeWorkflowResp, err := poller.HandlePartialWorkflowTask(updateResp.GetWorkflowTask(), false)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)
	s.Nil(completeWorkflowResp.GetWorkflowTask())
	s.EqualValues(0, completeWorkflowResp.ResetHistoryEventId)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_ScheduledSpeculativeWorkflowTask_ConvertToNormalBecauseOfSignal() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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
			updRequestMsg := task.Messages[0]

			s.EqualValues(6, updRequestMsg.GetEventId())

			// Update is rejected but corresponding speculative WT was already converted to normal,
			// and will be in the history anyway.
			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	updateResultCh := s.sendUpdateNoError(tv, "1")

	// Send signal which will NOT be buffered because speculative WT is not started yet (only scheduled).
	// This will persist MS and speculative WT must be converted to normal.
	err = s.SendSignal(s.Namespace(), tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
	s.NoError(err)

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	s.NotNil(res)
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, res.NewTask.ResetHistoryEventId)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_SpeculativeWorkflowTask_StartToCloseTimeout() {
	tv := testvars.New(s.T())

	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           tv.Any().String(),
		Namespace:           s.Namespace(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second), // Important!
	}

	startResp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)

	tv = tv.WithRunID(startResp.GetRunId())

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
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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
			commands := append(s.UpdateAcceptCompleteCommands(tv, "1"),
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
			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		case 3:
			// Update is still in registry and was sent again.
			updRequestMsg := task.Messages[0]

			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	updateResultCh := s.sendUpdateNoError(tv, "1")

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
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateResp.ResetHistoryEventId)
	s.Nil(updateResp.GetWorkflowTask())

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_SpeculativeWorkflowTask_ScheduleToStartTimeout() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Completes first WT with empty command list.
			return nil, nil
		case 2:
			// Speculative WT timed out on sticky task queue. Server sent full history with sticky timeout event.
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskTimedOut
  7 WorkflowTaskScheduled {"Attempt":1} // Normal WT.
  8 WorkflowTaskStarted`, task.History)
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
			// Reject update, but WT still will be in the history due to timeout on sticky queue.
			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:                       s.FrontendClient(),
		Namespace:                    s.Namespace(),
		TaskQueue:                    tv.TaskQueue(),
		StickyTaskQueue:              tv.StickyTaskQueue(),
		StickyScheduleToStartTimeout: 1 * time.Second, // Important!
		Identity:                     tv.WorkerIdentity(),
		WorkflowTaskHandler:          wtHandler,
		MessageHandler:               msgHandler,
		Logger:                       s.Logger,
		T:                            s.T(),
	}

	// Drain first WT and respond with sticky enabled response to enable sticky task queue.
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithRespondSticky, testcore.WithoutRetries)
	s.NoError(err)

	s.sendUpdateNoError(tv, "1")

	s.Logger.Info("Wait for sticky timeout to fire. Sleep poller.StickyScheduleToStartTimeout+ seconds.", tag.NewDurationTag("StickyScheduleToStartTimeout", poller.StickyScheduleToStartTimeout))
	time.Sleep(poller.StickyScheduleToStartTimeout + 100*time.Millisecond) //nolint:forbidigo
	s.Logger.Info("Sleep is done.")

	// Try to process update in workflow, poll from normal task queue.
	res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	updateResp := res.NewTask
	s.NotNil(updateResp)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())
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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_SpeculativeWorkflowTask_ScheduleToStartTimeoutOnNormalTaskQueue() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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

			s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(7, updRequestMsg.GetEventId())

			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
	updateResultCh := s.sendUpdateNoError(tv, "1")

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
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, res.NewTask.ResetHistoryEventId, "no reset of event ID should happened after update rejection if it was delivered with normal workflow task")

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_StartedSpeculativeWorkflowTask_TerminateWorkflow() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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
				Namespace:         s.Namespace(),
				WorkflowExecution: tv.WorkflowExecution(),
				Reason:            tv.Any().String(),
			})
			s.NoError(err)

			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted`, task.History)
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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
			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
	updateResultCh := s.sendUpdate(oneSecondTimeoutCtx, tv, "1")

	// Process update in workflow.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.Error(err)
	s.IsType(err, (*serviceerror.NotFound)(nil))
	s.ErrorContains(err, "Workflow task not found.")

	updateResult := <-updateResultCh
	s.Error(updateResult.err)
	var notFound *serviceerror.NotFound
	s.ErrorAs(updateResult.err, &notFound)
	s.Equal("workflow execution already completed", updateResult.err.Error())
	s.Nil(updateResult.response)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
		Namespace: s.Namespace(),
		Execution: tv.WorkflowExecution(),
	})
	s.NoError(err)
	s.EqualValues(7, msResp.GetDatabaseMutableState().GetExecutionInfo().GetCompletionEventBatchId(), "completion_event_batch_id should point to WTFailed event")
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_ScheduledSpeculativeWorkflowTask_TerminateWorkflow() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
	updateResultCh := s.sendUpdate(oneSecondTimeoutCtx, tv, "1")

	// Terminate workflow after speculative WT is scheduled but not started.
	_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.Namespace(),
		WorkflowExecution: tv.WorkflowExecution(),
		Reason:            tv.Any().String(),
	})
	s.NoError(err)

	updateResult := <-updateResultCh
	s.Error(updateResult.err)
	var notFound *serviceerror.NotFound
	s.ErrorAs(updateResult.err, &notFound)
	s.Equal("workflow execution already completed", updateResult.err.Error())
	s.Nil(updateResult.response)

	s.Equal(1, wtHandlerCalls)
	s.Equal(1, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionTerminated // Speculative WTScheduled event is not written to history if WF is terminated.
`, events)

	msResp, err := s.AdminClient().DescribeMutableState(testcore.NewContext(), &adminservice.DescribeMutableStateRequest{
		Namespace: s.Namespace(),
		Execution: tv.WorkflowExecution(),
	})
	s.NoError(err)
	s.EqualValues(5, msResp.GetDatabaseMutableState().GetExecutionInfo().GetCompletionEventBatchId(), "completion_event_batch_id should point to WFTerminated event")
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_CompleteWorkflow_AbortUpdates() {
	type testCase struct {
		name          string
		description   string
		updateErr     map[string]string // Update error by completionCommand.Name.
		updateFailure string
		commands      func(tv *testvars.TestVars) []*commandpb.Command
		messages      func(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message
	}
	type completionCommand struct {
		name        string
		finalStatus enumspb.WorkflowExecutionStatus
		command     func(_ *testvars.TestVars) *commandpb.Command
	}
	testCases := []testCase{
		{
			name:        "update admitted",
			description: "update in stateAdmitted must get an error",
			updateErr: map[string]string{
				"workflow completed":        "workflow execution already completed",
				"workflow continued as new": "workflow operation can not be applied because workflow is closing",
				"workflow failed":           "workflow execution already completed",
			},
			updateFailure: "",
			commands:      func(_ *testvars.TestVars) []*commandpb.Command { return nil },
			messages:      func(_ *testvars.TestVars, _ *protocolpb.Message) []*protocolpb.Message { return nil },
		},
		{
			name:          "update accepted",
			description:   "update in stateAccepted must get an update failure",
			updateErr:     map[string]string{"*": ""},
			updateFailure: "Workflow Update failed because the Workflow completed before the Update completed.",
			commands:      func(tv *testvars.TestVars) []*commandpb.Command { return s.UpdateAcceptCommands(tv, "1") },
			messages: func(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
				return s.UpdateAcceptMessages(tv, updRequestMsg, "1")
			},
		},
		{
			name:          "update completed",
			description:   "completed update must not be affected by workflow completion",
			updateErr:     map[string]string{"*": ""},
			updateFailure: "",
			commands:      func(tv *testvars.TestVars) []*commandpb.Command { return s.UpdateAcceptCompleteCommands(tv, "1") },
			messages: func(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
				return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1")
			},
		},
		{
			name:          "update rejected",
			description:   "rejected update must be rejected with rejection from workflow",
			updateErr:     map[string]string{"*": ""},
			updateFailure: "rejection-of-", // Rejection from workflow.
			commands:      func(tv *testvars.TestVars) []*commandpb.Command { return nil },
			messages: func(tv *testvars.TestVars, updRequestMsg *protocolpb.Message) []*protocolpb.Message {
				return s.UpdateRejectMessages(tv, updRequestMsg, "1")
			},
		},
	}

	workflowCompletionCommands := []completionCommand{
		{
			name:        "workflow completed",
			finalStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			command: func(_ *testvars.TestVars) *commandpb.Command {
				return &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
				}
			},
		},
		{
			name:        "workflow continued as new",
			finalStatus: enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
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
			s.Run(tc.name+" "+wfCC.name, func() {
				tv := testvars.New(s.T())

				tv = s.startWorkflow(tv)

				wtHandlerCalls := 0
				wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
					wtHandlerCalls++
					switch wtHandlerCalls {
					case 1:
						// Completes first WT with empty command list.
						return nil, nil
					case 2:
						return append(tc.commands(tv), wfCC.command(tv)), nil
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
						return tc.messages(tv, updRequestMsg), nil
					default:
						s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
						return nil, nil
					}
				}

				poller := &testcore.TaskPoller{
					Client:              s.FrontendClient(),
					Namespace:           s.Namespace(),
					TaskQueue:           tv.TaskQueue(),
					Identity:            tv.WorkerIdentity(),
					WorkflowTaskHandler: wtHandler,
					MessageHandler:      msgHandler,
					Logger:              s.Logger,
					T:                   s.T(),
				}

				// Drain first WT.
				_, err := poller.PollAndProcessWorkflowTask()
				s.NoError(err)

				updateResultCh := s.sendUpdate(testcore.NewContext(), tv, "1")

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
					s.Equal(updateResult.err.Error(), expectedUpdateErr)
				} else {
					s.NoError(updateResult.err, tc.description)
				}

				if tc.updateFailure != "" {
					s.NotNil(updateResult.response.GetOutcome().GetFailure(), tc.description)
					s.Contains(updateResult.response.GetOutcome().GetFailure().GetMessage(), tc.updateFailure, tc.description)
				} else {
					s.Nil(updateResult.response.GetOutcome().GetFailure(), tc.description)
				}

				// Check that update didn't block workflow completion.
				descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: s.Namespace(),
					Execution: tv.WorkflowExecution(),
				})
				s.NoError(err)
				s.Equal(wfCC.finalStatus, descResp.WorkflowExecutionInfo.Status)

				s.Equal(2, wtHandlerCalls)
				s.Equal(2, msgHandlerCalls)
			})
		}
	}
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_SpeculativeWorkflowTask_Heartbeat() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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
  5 WorkflowTaskScheduled // Events (5 and 6) are for speculative WT, but they won't disappear after reject because speculative WT is converted to normal during heartbeat.
  6 WorkflowTaskStarted
`, task.History)
			// Heartbeat from speculative WT (no messages, no commands).
			return nil, nil
		case 3:
			s.EqualHistory(`
  7 WorkflowTaskCompleted
  8 WorkflowTaskScheduled // New WT (after heartbeat) is normal and won't disappear from the history after reject.
  9 WorkflowTaskStarted
`, task.History)
			// Reject update.
			return nil, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	msgHandlerCalls := 0
	var updRequestMsg *protocolpb.Message
	msgHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
		msgHandlerCalls++
		switch msgHandlerCalls {
		case 1:
			return nil, nil
		case 2:
			s.Len(task.Messages, 1)
			updRequestMsg = task.Messages[0]
			s.EqualValues(5, updRequestMsg.GetEventId())
			return nil, nil
		case 3:
			s.Empty(task.Messages)
			return s.UpdateRejectMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	updateResultCh := s.sendUpdateNoError(tv, "1")

	// Heartbeat from workflow.
	res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries, testcore.WithForceNewWorkflowTask)
	s.NoError(err)
	heartbeatResp := res.NewTask

	// Reject update from workflow.
	updateResp, err := poller.HandlePartialWorkflowTask(heartbeatResp.GetWorkflowTask(), false)
	s.NoError(err)
	s.NotNil(updateResp)
	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv.UpdateID("1"), updateResult.GetOutcome().GetFailure().GetMessage())
	s.EqualValues(0, updateResp.ResetHistoryEventId, "no reset of event ID should happened after update rejection because of heartbeat")

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_ScheduledSpeculativeWorkflowTask_LostUpdate() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
	updateResult := <-s.sendUpdate(halfSecondTimeoutCtx, tv, "1")
	s.Error(updateResult.err)
	s.True(common.IsContextDeadlineExceededErr(updateResult.err), updateResult.err.Error())
	s.Nil(updateResult.response)

	// Lose update registry. Speculative WFT and update registry disappear.
	s.loseUpdateRegistryAndAbandonPendingUpdates(tv)

	// Ensure, there is no WFT.
	pollCtx, cancel := context.WithTimeout(testcore.NewContext(), common.MinLongPollTimeout*2)
	defer cancel()
	pollResponse, err := s.FrontendClient().PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)
	s.Nil(pollResponse.Messages, "there should not be new WFT with messages")

	// Send signal to schedule new WT.
	err = s.SendSignal(s.Namespace(), tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
	s.NoError(err)

	// Complete workflow and check that there is update messages.
	completeWorkflowResp, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.NotNil(completeWorkflowResp)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_StartedSpeculativeWorkflowTask_LostUpdate() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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
			s.loseUpdateRegistryAndAbandonPendingUpdates(tv)

			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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

			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		case 3:
			s.Empty(task.Messages, "no messages since update registry was lost")
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
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
	updateResultCh := s.sendUpdate(halfSecondTimeoutCtx, tv, "1")

	// Process update in workflow.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.Error(err)
	s.IsType(&serviceerror.NotFound{}, err)
	s.ErrorContains(err, "Workflow task not found")

	updateResult := <-updateResultCh
	s.Error(updateResult.err)
	s.True(common.IsContextDeadlineExceededErr(updateResult.err), updateResult.err.Error())
	s.Nil(updateResult.response)

	// Send signal to schedule new WFT.
	err = s.SendSignal(s.Namespace(), tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
	s.NoError(err)

	// Complete workflow.
	completeWorkflowResp, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.NotNil(completeWorkflowResp)

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_FirstNormalWorkflowTask_UpdateResurrectedAfterRegistryCleared() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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
			s.clearUpdateRegistryAndAbortPendingUpdates(tv)

			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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

			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		case 2:
			s.Empty(task.Messages, "update must be processed and not delivered again")
			return nil, nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	updateResultCh := s.sendUpdateNoError(tv, "1")

	// Process update in workflow. Update won't be found on server but will be resurrected from acceptance message and completed.
	res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	s.NotNil(res)

	// Client receives resurrected Update outcome.
	updateResult := <-updateResultCh
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))

	// Signal to create new WFT which shouldn't get any updates.
	err = s.SendSignal(s.Namespace(), tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
	s.NoError(err)

	// Complete workflow.
	completeWorkflowResp, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	s.NotNil(completeWorkflowResp)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_ScheduledSpeculativeWorkflowTask_DeduplicateID() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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

			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	updateResultCh := s.sendUpdateNoError(tv, "1")

	// Send second update with the same ID.
	updateResultCh2 := s.sendUpdateNoError(tv, "1")

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	updateResp := res.NewTask
	updateResult := <-updateResultCh
	updateResult2 := <-updateResultCh2
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult2.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_StartedSpeculativeWorkflowTask_DeduplicateID() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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
			updateResultCh2 = s.sendUpdateNoError(tv, "1")

			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled // Speculative WT.
  6 WorkflowTaskStarted
`, task.History)
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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
			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	updateResultCh := s.sendUpdateNoError(tv, "1")

	// Process update in workflow.
	res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	s.NotNil(res)
	updateResult := <-updateResultCh
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
	s.EqualValues(0, res.NewTask.ResetHistoryEventId)

	updateResult2 := <-updateResultCh2
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult2.GetOutcome().GetSuccess()))

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_CompletedSpeculativeWorkflowTask_DeduplicateID() {
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
		s.Run(tc.Name, func() {
			tv := testvars.New(s.T())

			tv = s.startWorkflow(tv)

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
					return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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
					return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
				case 3:
					s.Empty(task.Messages, "2nd update must be deduplicated by ID ")
					return nil, nil
				default:
					s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
					return nil, nil
				}
			}

			poller := &testcore.TaskPoller{
				Client:              s.FrontendClient(),
				Namespace:           s.Namespace(),
				TaskQueue:           tv.TaskQueue(),
				Identity:            tv.WorkerIdentity(),
				WorkflowTaskHandler: wtHandler,
				MessageHandler:      msgHandler,
				Logger:              s.Logger,
				T:                   s.T(),
			}

			// Drain first WT.
			_, err := poller.PollAndProcessWorkflowTask()
			s.NoError(err)

			updateResultCh := s.sendUpdateNoError(tv, "1")

			// Process update in workflow.
			_, err = poller.PollAndProcessWorkflowTask()
			s.NoError(err)
			updateResult := <-updateResultCh
			s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))

			if tc.CloseShard {
				// Close shard to make sure that for completed updates deduplication works even after shard reload.
				s.closeShard(tv.WorkflowID())
			}

			// Send second update with the same ID. It must return immediately.
			updateResult2 := <-s.sendUpdateNoError(tv, "1")

			// Ensure, there is no new WT.
			pollCtx, cancel := context.WithTimeout(testcore.NewContext(), common.MinLongPollTimeout*2)
			defer cancel()
			pollResponse, err := s.FrontendClient().PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: s.Namespace(),
				TaskQueue: tv.TaskQueue(),
				Identity:  tv.WorkerIdentity(),
			})
			s.NoError(err)
			s.Nil(pollResponse.Messages, "there must be no new WT")

			s.EqualValues(
				"success-result-of-"+tv.UpdateID("1"),
				testcore.DecodeString(s.T(), updateResult2.GetOutcome().GetSuccess()),
				"results of the first update must be available")

			// Send signal to schedule new WT.
			err = s.SendSignal(s.Namespace(), tv.WorkflowExecution(), tv.Any().String(), tv.Any().Payloads(), tv.Any().String())
			s.NoError(err)

			// Complete workflow.
			completeWorkflowResp, err := poller.PollAndProcessWorkflowTask()
			s.NoError(err)
			s.NotNil(completeWorkflowResp)

			s.Equal(3, wtHandlerCalls)
			s.Equal(3, msgHandlerCalls)

			events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_StaleSpeculativeWorkflowTask_Fail_BecauseOfDifferentStartedId() {
	/*
		Test scenario:
		An update created a speculative WT and WT is dispatched to the worker (started).
		Update registry is cleared, speculative WT disappears from server.
		Update is retired and second speculative WT is scheduled but not dispatched yet.
		An activity completes, it converts the 2nd speculative WT into normal one.
		The first speculative WT responds back, server fails request because WorkflowTaskStarted event Id is mismatched.
		The second speculative WT responds back and server completes it.
	*/

	tv := testvars.New(s.T())
	tv = s.startWorkflow(tv)

	wtHandlerCalls := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wtHandlerCalls++
		switch wtHandlerCalls {
		case 1:
			// Schedule activity.
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             tv.ActivityID(),
					ActivityType:           tv.ActivityType(),
					TaskQueue:              tv.TaskQueue(),
					ScheduleToCloseTimeout: tv.InfiniteTimeout(),
				}},
			}}, nil
		default:
			s.Failf("wtHandler called too many times", "wtHandler shouldn't be called %d times", wtHandlerCalls)
			return nil, nil
		}
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		return tv.Any().Payloads(), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
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
	s.sendUpdateNoError(tv, "1")

	// Poll 2nd speculative WT with 1st update.
	wt2, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace(),
		TaskQueue: tv.TaskQueue(),
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
	s.clearUpdateRegistryAndAbortPendingUpdates(tv)

	// Wait for update request to be retry by frontend and recreated in registry. This will create a 3rd WFT as speculative.
	s.waitUpdateAdmitted(tv, "1")

	// Before polling for the 3rd speculative WT, process activity. This will convert 3rd speculative WT to normal WT.
	err = poller.PollAndProcessActivityTask(false)
	s.NoError(err)

	// Poll the 3rd WFT (not speculative anymore) but must have 2nd update.
	wt3, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace(),
		TaskQueue: tv.TaskQueue(),
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
		Namespace: s.Namespace(),
		TaskToken: wt2.TaskToken,
		Commands:  s.UpdateAcceptCompleteCommands(tv, "1"),
		Messages:  s.UpdateAcceptCompleteMessages(tv, wt2.Messages[0], "1"),
	})
	s.Error(err, "Must fail because WorkflowTaskStarted event Id is different.")
	s.IsType(&serviceerror.NotFound{}, err)
	s.Contains(err.Error(), "Workflow task not found")

	// Complete 3rd WT. It should succeed.
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace(),
		TaskToken: wt3.TaskToken,
		Commands:  s.UpdateAcceptCompleteCommands(tv, "1"),
		Messages:  s.UpdateAcceptCompleteMessages(tv, wt3.Messages[0], "1"),
	})
	s.NoError(err)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())
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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_StaleSpeculativeWorkflowTask_Fail_BecauseOfDifferentStartTime() {
	/*
		Test scenario:
		An update created a speculative WT and WT is dispatched to the worker (started).
		WF context is cleared, speculative WT is disappeared from server.
		Update is retried and second speculative WT is dispatched to worker with same WT scheduled/started Id and update Id.
		The first speculative WT respond back, server reject it because startTime is different.
		The second speculative WT respond back, server accept it.
	*/
	tv := testvars.New(s.T())
	tv = s.startWorkflow(tv)

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

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// First WT will schedule activity.
	res, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	s.NotNil(res)

	// Send update. It will create 2nd WT as speculative.
	s.sendUpdateNoError(tv, "1")

	// Poll 2nd speculative WT with 1st update.
	wt2, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace(),
		TaskQueue: tv.TaskQueue(),
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
	s.clearUpdateRegistryAndAbortPendingUpdates(tv)

	// Wait for update request to be retry by frontend and recreated in registry. This will create a 3rd WFT as speculative.
	s.waitUpdateAdmitted(tv, "1")

	// Poll for the 3rd speculative WT.
	wt3, err := s.FrontendClient().PollWorkflowTaskQueue(testcore.NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace(),
		TaskQueue: tv.TaskQueue(),
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
		Namespace: s.Namespace(),
		TaskToken: wt2.TaskToken,
		Commands:  s.UpdateAcceptCompleteCommands(tv, "1"),
		Messages:  s.UpdateAcceptCompleteMessages(tv, wt2.Messages[0], "1"),
	})
	s.Error(err, "Must fail because workflow task start time is different.")
	s.IsType(&serviceerror.NotFound{}, err)
	s.Contains(err.Error(), "Workflow task not found")

	// Try to complete 3rd WT, it should succeed
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testcore.NewContext(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace(),
		TaskToken: wt3.TaskToken,
		Commands:  s.UpdateAcceptCompleteCommands(tv, "1"),
		Messages:  s.UpdateAcceptCompleteMessages(tv, wt3.Messages[0], "1"),
	})
	s.NoError(err, "2nd speculative WT should be completed because it has same WT scheduled/started Id and startTime matches the accepted message is valid (same update Id)")

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())
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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_StaleSpeculativeWorkflowTask_Fail_NewWorkflowTaskWith2Updates() {
	/*
		Test scenario:
		An update created a speculative WT and WT is dispatched to the worker (started).
		Mutable state cleared, speculative WT and update registry are disappeared from server.
		First update is retried and another update come in.
		Second speculative WT is dispatched to worker with same WT scheduled/started Id but 2 updates.
		The first speculative WT responds back, server rejected it (different start time).
		The second speculative WT responds back, server accepted it.
	*/

	tv := testvars.New(s.T())
	tv = s.startWorkflow(tv)
	testCtx := testcore.NewContext()

	// Drain first WFT.
	wt1, err := s.FrontendClient().PollWorkflowTaskQueue(testCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace(),
		TaskQueue: tv.TaskQueue(),
	})
	s.NoError(err)
	s.NotNil(wt1)
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(testCtx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace(),
		TaskToken: wt1.TaskToken,
	})
	s.NoError(err)

	// Send 1st update. It will create 2nd speculative WFT.
	s.sendUpdateNoError(tv, "1")

	// Poll 2nd speculative WFT with 1st update.
	wt2, err := s.FrontendClient().PollWorkflowTaskQueue(testCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace(),
		TaskQueue: tv.TaskQueue(),
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
	s.clearUpdateRegistryAndAbortPendingUpdates(tv)

	// Make sure UpdateWorkflowExecution call for the update "1" is retried and new (3rd) WFT is created as speculative with updateID=1.
	s.waitUpdateAdmitted(tv, "1")

	// Send 2nd update (with DIFFERENT updateId). It reuses already created 3rd WFT.
	s.sendUpdateNoError(tv, "2")
	// updateID=1 is still blocked. There must be 2 blocked updates now.

	// Poll the 3rd speculative WFT.
	wt3, err := s.FrontendClient().PollWorkflowTaskQueue(testCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace(),
		TaskQueue: tv.TaskQueue(),
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
		Namespace:             s.Namespace(),
		TaskToken:             wt2.TaskToken,
		Commands:              s.UpdateAcceptCompleteCommands(tv, "1"),
		Messages:              s.UpdateAcceptCompleteMessages(tv, wt2.Messages[0], "1"),
		ReturnNewWorkflowTask: true,
	})
	s.Error(err, "Must fail because start time is different.")
	s.Contains(err.Error(), "Workflow task not found")
	s.IsType(&serviceerror.NotFound{}, err)

	// Complete of the 3rd WT should succeed. It must accept both updates.
	wt4Resp, err := s.FrontendClient().RespondWorkflowTaskCompleted(testCtx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace(),
		TaskToken: wt3.TaskToken,
		Commands: append(
			s.UpdateAcceptCompleteCommands(tv, "1"),
			s.UpdateAcceptCompleteCommands(tv, "2")...),
		Messages: append(
			s.UpdateAcceptCompleteMessages(tv, wt3.Messages[0], "1"),
			s.UpdateAcceptCompleteMessages(tv, wt3.Messages[1], "2")...),
		ReturnNewWorkflowTask: true,
	})
	s.NoError(err)
	s.NotNil(wt4Resp)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())
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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_SpeculativeWorkflowTask_WorkerSkippedProcessing_RejectByServer() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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
			update2ResultCh = s.sendUpdateNoError(tv, "2")
			return nil, nil
		case 3:
			s.EqualHistory(`
  4 WorkflowTaskCompleted // Speculative WT was dropped and history starts from 4 again.
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted`, task.History)
			commands := append(s.UpdateAcceptCompleteCommands(tv, "2"),
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

			s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
			s.EqualValues(5, updRequestMsg.GetEventId())

			// Don't process update in WT.
			return nil, nil
		case 3:
			s.Len(task.Messages, 1)
			updRequestMsg := task.Messages[0]
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			s.Equal("args-value-of-"+tv.UpdateID("2"), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
			s.EqualValues(5, updRequestMsg.GetEventId())
			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "2"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Identity:            "old_worker",
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	updateResultCh := s.sendUpdateNoError(tv, "1")

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
	s.EqualValues("success-result-of-"+tv.UpdateID("2"), testcore.DecodeString(s.T(), update2Result.GetOutcome().GetSuccess()))

	s.Equal(3, wtHandlerCalls)
	s.Equal(3, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())
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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_LastWorkflowTask_HasUpdateMessage() {
	tv := testvars.New(s.T())
	tv = s.startWorkflow(tv)

	poller := &testcore.TaskPoller{
		Client:    s.FrontendClient(),
		Namespace: s.Namespace(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
		WorkflowTaskHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			completeWorkflowCommand := &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: tv.Any().Payloads(),
					},
				},
			}
			return append(s.UpdateAcceptCommands(tv, "1"), completeWorkflowCommand), nil
		},
		MessageHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			return s.UpdateAcceptMessages(tv, task.Messages[0], "1"), nil
		},
		Logger: s.Logger,
		T:      s.T(),
	}

	updateResultCh := s.sendUpdateNoErrorWaitPolicyAccepted(tv, "1")
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
	`, s.GetHistory(s.Namespace(), tv.WorkflowExecution()))
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_SpeculativeWorkflowTask_QueryFailureClearsWFContext() {
	tv := testvars.New(s.T())

	tv = s.startWorkflow(tv)

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
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
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

			s.Equal("args-value-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
			s.Equal(tv.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(5, updRequestMsg.GetEventId())

			return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
		default:
			s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
			return nil, nil
		}
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowTaskHandler: wtHandler,
		MessageHandler:      msgHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Drain first WT.
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	updateResultCh := s.sendUpdateNoError(tv, "1")

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
			Namespace: s.Namespace(),
			Execution: tv.WorkflowExecution(),
			Query: &querypb.WorkflowQuery{
				QueryType: tv.Any().String(),
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
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
	s.EqualValues(0, updateResp.ResetHistoryEventId)

	s.Equal(2, wtHandlerCalls)
	s.Equal(2, msgHandlerCalls)

	events := s.GetHistory(s.Namespace(), tv.WorkflowExecution())

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
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_UpdatesAreSentToWorkerInOrderOfAdmission() {
	// If our implementation is not in fact ordering updates correctly, then it may be ordering them
	// non-deterministically. This number should be high enough that the false-negative rate of the test is low, but
	// must not exceed our limit on number of in-flight updates. If we were picking a random ordering then the
	// false-negative rate would be 1/(nUpdates!).
	nUpdates := 10

	tv := testvars.New(s.T())
	tv = s.startWorkflow(tv)
	for i := 0; i < nUpdates; i++ {
		// Sequentially send updates one by one.
		updateId := strconv.Itoa(i)
		s.sendUpdateNoError(tv, updateId)
	}

	wtHandlerCalls := 0
	msgHandlerCalls := 0
	poller := &testcore.TaskPoller{
		Client:    s.FrontendClient(),
		Namespace: s.Namespace(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
		WorkflowTaskHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			msgHandlerCalls++
			var commands []*commandpb.Command
			for i := range task.Messages {
				commands = append(commands, s.UpdateAcceptCompleteCommands(tv, strconv.Itoa(i))...)
			}
			return commands, nil
		},
		MessageHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			wtHandlerCalls++
			s.Len(task.Messages, nUpdates)
			var messages []*protocolpb.Message
			// Updates were sent in sequential order of updateId => messages must be ordered in the same way.
			for i, m := range task.Messages {
				s.Equal(tv.UpdateID(strconv.Itoa(i)), m.ProtocolInstanceId)
				messages = append(messages, s.UpdateAcceptCompleteMessages(tv, m, strconv.Itoa(i))...)
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
	for i := 0; i < nUpdates; i++ {
		expectedHistory += fmt.Sprintf(`
  %d WorkflowExecutionUpdateAccepted {"AcceptedRequest":{"Meta": {"UpdateId": "%s"}}}
  %d WorkflowExecutionUpdateCompleted {"Meta": {"UpdateId": "%s"}}`,
			5+2*i, tv.UpdateID(strconv.Itoa(i)),
			6+2*i, tv.UpdateID(strconv.Itoa(i)))
	}

	history := s.GetHistory(s.Namespace(), tv.WorkflowExecution())
	s.EqualHistoryEvents(expectedHistory, history)
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_WaitAccepted_GotCompleted() {
	tv := testvars.New(s.T())
	tv = s.startWorkflow(tv)

	poller := &testcore.TaskPoller{
		Client:    s.FrontendClient(),
		Namespace: s.Namespace(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
		WorkflowTaskHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			return s.UpdateAcceptCompleteCommands(tv, "1"), nil
		},
		MessageHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			return s.UpdateAcceptCompleteMessages(tv, task.Messages[0], "1"), nil
		},
		Logger: s.Logger,
		T:      s.T(),
	}

	// Send Update with intent to wait for Accepted stage only,
	updateResultCh := s.sendUpdateNoErrorWaitPolicyAccepted(tv, "1")
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
	s.NoError(err)
	updateResult := <-updateResultCh
	// but Update was accepted and completed on the same WFT, and outcome was returned.
	s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, updateResult.GetStage())
	s.EqualValues("success-result-of-"+tv.UpdateID("1"), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))

	s.EqualHistoryEvents(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowTaskStarted
	4 WorkflowTaskCompleted
	5 WorkflowExecutionUpdateAccepted
	6 WorkflowExecutionUpdateCompleted
	`, s.GetHistory(s.Namespace(), tv.WorkflowExecution()))
}

func (s *UpdateWorkflowSuite) TestUpdateWorkflow_ContinueAsNew_UpdateIsNotCarriedOver() {
	tv := testvars.New(s.T())
	tv = s.startWorkflow(tv)
	firstRunID := tv.RunID()
	tv = tv.WithRunID("")

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

	poller1 := &testcore.TaskPoller{
		Client:    s.FrontendClient(),
		Namespace: s.Namespace(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
		WorkflowTaskHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			// Send 2nd Update while WFT is running.
			update2ResponseCh = s.sendUpdate(context.Background(), tv, "2")
			canCommand := &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					WorkflowType: tv.WorkflowType(),
					TaskQueue:    tv.TaskQueue("2"),
				}},
			}
			return append(s.UpdateAcceptCommands(tv, "1"), canCommand), nil
		},
		MessageHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
			return s.UpdateAcceptMessages(tv, task.Messages[0], "1"), nil
		},
		Logger: s.Logger,
		T:      s.T(),
	}

	poller2 := &testcore.TaskPoller{
		Client:    s.FrontendClient(),
		Namespace: s.Namespace(),
		TaskQueue: tv.TaskQueue("2"),
		Identity:  tv.WorkerIdentity(),
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

	update1ResponseCh := s.sendUpdate(context.Background(), tv, "1")
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
  6 WorkflowExecutionContinuedAsNew`, s.GetHistory(s.Namespace(), tv.WithRunID(firstRunID).WorkflowExecution()))

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted`, s.GetHistory(s.Namespace(), tv.WorkflowExecution()))
}

func (s *UpdateWorkflowSuite) TestUpdateWithStart() {
	// reset reuse minimal interval to allow workflow termination
	s.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, 0)

	type multiopsResponseErr struct {
		response *workflowservice.ExecuteMultiOperationResponse
		err      error
	}

	runMultiOp := func(
		tv *testvars.TestVars,
		request *workflowservice.ExecuteMultiOperationRequest,
	) (*workflowservice.ExecuteMultiOperationResponse, error) {
		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		msgHandlerCalls := 0
		poller := &testcore.TaskPoller{
			Client:    s.FrontendClient(),
			Namespace: s.Namespace(),
			TaskQueue: tv.TaskQueue(),
			Identity:  tv.WorkerIdentity(),
			WorkflowTaskHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
				return nil, nil
			},
			MessageHandler: func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {
				if len(task.Messages) > 0 {
					updRequestMsg := task.Messages[0]
					msgHandlerCalls += 1
					switch msgHandlerCalls {
					case 1:
						return s.UpdateAcceptCompleteMessages(tv, updRequestMsg, "1"), nil
					default:
						s.Failf("msgHandler called too many times", "msgHandler shouldn't be called %d times", msgHandlerCalls)
					}
				}
				return nil, nil
			},
			Logger: s.Logger,
			T:      s.T(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		retCh := make(chan multiopsResponseErr)
		go func() {
			resp, err := s.FrontendClient().ExecuteMultiOperation(ctx, request)
			retCh <- multiopsResponseErr{resp, err}
		}()

		go func() {
			// TODO: handle error
			_, _ = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
		}()

		ret := <-retCh
		return ret.response, ret.err
	}

	runUpdateWithStart := func(
		tv *testvars.TestVars,
		startReq *workflowservice.StartWorkflowExecutionRequest,
		updateReq *workflowservice.UpdateWorkflowExecutionRequest,
	) (*workflowservice.ExecuteMultiOperationResponse, error) {
		resp, err := runMultiOp(tv,
			&workflowservice.ExecuteMultiOperationRequest{
				Namespace: s.Namespace(),
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
			s.Len(resp.Responses, 2)

			startRes := resp.Responses[0].Response.(*workflowservice.ExecuteMultiOperationResponse_Response_StartWorkflow).StartWorkflow
			s.NotZero(startRes.RunId)

			updateRes := resp.Responses[1].Response.(*workflowservice.ExecuteMultiOperationResponse_Response_UpdateWorkflow).UpdateWorkflow
			s.NotNil(updateRes.Outcome)
			s.NotZero(updateRes.Outcome.String())
		}

		return resp, err
	}

	startWorkflowReq := func(tv *testvars.TestVars) *workflowservice.StartWorkflowExecutionRequest {
		return &workflowservice.StartWorkflowExecutionRequest{
			Namespace:    s.Namespace(),
			WorkflowId:   tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			Identity:     tv.WorkerIdentity(),
		}
	}

	updateWorkflowReq := func(tv *testvars.TestVars) *workflowservice.UpdateWorkflowExecutionRequest {
		return &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace: s.Namespace(),
			Request: &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: tv.UpdateID("1")},
				Input: &updatepb.Input{Name: tv.Any().String(), Args: tv.Any().Payloads()},
			},
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
			WaitPolicy:        &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED},
		}
	}

	s.Run("workflow is not running", func() {

		s.Run("start workflow and send update", func() {
			tv := testvars.New(s.T())

			resp, err := runUpdateWithStart(tv, startWorkflowReq(tv), updateWorkflowReq(tv))
			s.NoError(err)
			s.True(resp.Responses[0].GetStartWorkflow().Started)
		})

		s.Run("poll update result after completion", func() {
			tv := testvars.New(s.T())

			_, err := runUpdateWithStart(tv, startWorkflowReq(tv), updateWorkflowReq(tv))
			s.NoError(err)

			_, err = s.pollUpdate(tv, "1",
				&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
			s.Nil(err)
		})

		s.Run("workflow id conflict policy terminate-existing: not supported yet", func() {
			tv := testvars.New(s.T())

			req := startWorkflowReq(tv)
			req.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
			_, err := runUpdateWithStart(tv, req, updateWorkflowReq(tv))
			s.Error(err)
		})
	})

	s.Run("workflow is running", func() {

		s.Run("workflow id conflict policy use-existing: only send update", func() {
			tv := testvars.New(s.T())

			_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowReq(tv))
			s.NoError(err)

			req := startWorkflowReq(tv)
			req.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
			resp, err := runUpdateWithStart(tv, req, updateWorkflowReq(tv))
			s.NoError(err)
			s.False(resp.Responses[0].GetStartWorkflow().Started)
		})

		s.Run("workflow id conflict policy terminate-existing: terminate workflow first, then start and update", func() {
			s.T().Skip("TODO")
			tv := testvars.New(s.T())

			initReq := startWorkflowReq(tv)
			initReq.TaskQueue.Name = initReq.TaskQueue.Name + "-init" // avoid race condition with poller
			initWF, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), initReq)
			s.NoError(err)

			req := startWorkflowReq(tv)
			req.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
			resp, err := runUpdateWithStart(tv, req, updateWorkflowReq(tv))
			s.NoError(err)
			s.True(resp.Responses[0].GetStartWorkflow().Started)

			descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(),
				&workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: s.Namespace(),
					Execution: &commonpb.WorkflowExecution{WorkflowId: req.WorkflowId, RunId: initWF.RunId},
				})
			s.NoError(err)
			s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.WorkflowExecutionInfo.Status)
		})

		s.Run("workflow id conflict policy fail: abort multi operation", func() {
			tv := testvars.New(s.T())

			_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowReq(tv))
			s.NoError(err)

			req := startWorkflowReq(tv)
			req.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
			_, err = runUpdateWithStart(tv, req, updateWorkflowReq(tv))
			s.Error(err)
			s.Equal(err.Error(), "MultiOperation could not be executed.")
			errs := err.(*serviceerror.MultiOperationExecution).OperationErrors()
			s.Len(errs, 2)
			s.Contains(errs[0].Error(), "Workflow execution is already running")
			s.Equal("Operation was aborted.", errs[1].Error())
		})

		s.Run("poll update result after completion", func() {
			tv := testvars.New(s.T())

			_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowReq(tv))
			s.NoError(err)

			req := startWorkflowReq(tv)
			req.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
			_, err = runUpdateWithStart(tv, req, updateWorkflowReq(tv))
			s.NoError(err)

			_, err = s.pollUpdate(tv, "1",
				&updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
			s.Nil(err)
		})
	})

	s.Run("dedupes both operations", func() {

		s.Run("for workflow id conflict policy fail", func() {
			tv := testvars.New(s.T())

			startReq := startWorkflowReq(tv)
			startReq.RequestId = "request_id"
			startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
			updReq := updateWorkflowReq(tv)

			resp1, err := runUpdateWithStart(tv, startReq, updReq)
			s.NoError(err)

			resp2, err := runUpdateWithStart(tv, startReq, updReq)
			s.NoError(err)

			s.Equal(resp1.Responses[0].GetStartWorkflow().RunId, resp2.Responses[0].GetStartWorkflow().RunId)
			s.Equal(resp1.Responses[1].GetUpdateWorkflow().Outcome.GetSuccess(), resp2.Responses[1].GetUpdateWorkflow().Outcome.GetSuccess())
		})

		s.Run("for workflow id conflict policy use existing", func() {
			tv := testvars.New(s.T())

			startReq := startWorkflowReq(tv)
			startReq.RequestId = "request_id"
			startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
			updReq := updateWorkflowReq(tv)

			resp1, err := runUpdateWithStart(tv, startReq, updReq)
			s.NoError(err)

			resp2, err := runUpdateWithStart(tv, startReq, updReq)
			s.NoError(err)

			s.Equal(resp1.Responses[0].GetStartWorkflow().RunId, resp2.Responses[0].GetStartWorkflow().RunId)
			s.Equal(resp1.Responses[1].GetUpdateWorkflow().Outcome.GetSuccess(), resp2.Responses[1].GetUpdateWorkflow().Outcome.GetSuccess())
		})

		s.Run("for workflow id conflict policy terminate", func() {
			s.T().Skip("TODO")
			tv := testvars.New(s.T())

			startReq := startWorkflowReq(tv)
			startReq.RequestId = "request_id"
			startReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
			updReq := updateWorkflowReq(tv)

			resp1, err := runUpdateWithStart(tv, startReq, updReq)
			s.NoError(err)

			resp2, err := runUpdateWithStart(tv, startReq, updReq)
			s.NoError(err)

			s.Equal(resp1.Responses[0].GetStartWorkflow().RunId, resp2.Responses[0].GetStartWorkflow().RunId)
			s.Equal(resp1.Responses[1].GetUpdateWorkflow().Outcome.GetSuccess(), resp2.Responses[1].GetUpdateWorkflow().Outcome.GetSuccess())
		})
	})
}

func (s *UpdateWorkflowSuite) closeShard(wid string) {
	s.T().Helper()

	resp, err := s.FrontendClient().DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{
		Namespace: s.Namespace(),
	})
	s.NoError(err)

	_, err = s.AdminClient().CloseShard(testcore.NewContext(), &adminservice.CloseShardRequest{
		ShardId: common.WorkflowIDToHistoryShard(resp.NamespaceInfo.Id, wid, s.GetTestClusterConfig().HistoryConfig.NumHistoryShards),
	})
	s.NoError(err)
}
