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

package update

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/api/resetworkflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ResetWorkflowTestSuite struct {
	WorkflowUpdateBaseSuite
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow() {
	id := "functional-reset-workflow-test"
	wt := "functional-reset-workflow-test-type"
	tq := "functional-reset-workflow-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.Namespace(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	workflowComplete := false
	activityData := int32(1)
	activityCount := 3
	isFirstTaskProcessed := false
	isSecondTaskProcessed := false
	var firstActivityCompletionEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !isFirstTaskProcessed {
			// Schedule 3 activities on first workflow task
			isFirstTaskProcessed = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			var scheduleActivityCommands []*commandpb.Command
			for i := 1; i <= activityCount; i++ {
				scheduleActivityCommands = append(scheduleActivityCommands, &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             strconv.Itoa(i),
						ActivityType:           &commonpb.ActivityType{Name: "ResetActivity"},
						TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						Input:                  payloads.EncodeBytes(buf.Bytes()),
						ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
						ScheduleToStartTimeout: durationpb.New(100 * time.Second),
						StartToCloseTimeout:    durationpb.New(50 * time.Second),
						HeartbeatTimeout:       durationpb.New(5 * time.Second),
					}},
				})
			}

			return scheduleActivityCommands, nil
		} else if !isSecondTaskProcessed {
			// Confirm one activity completion on second workflow task
			isSecondTaskProcessed = true
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
					firstActivityCompletionEvent = event
					return []*commandpb.Command{}, nil
				}
			}
		}

		// Complete workflow after reset
		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil

	}

	// activity handler
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Process first workflow task to schedule activities
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Process one activity task which also creates second workflow task
	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("Poll and process first activity", tag.Error(err))
	s.NoError(err)

	// Process second workflow task which checks activity completion
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("Poll and process second workflow task", tag.Error(err))
	s.NoError(err)

	// Find reset point (last completed workflow task)
	events := s.GetHistory(s.Namespace(), &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	})
	var lastWorkflowTask *historypb.HistoryEvent
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			lastWorkflowTask = event
		}
	}

	// Reset workflow execution
	resetResp, err := s.FrontendClient().ResetWorkflowExecution(testcore.NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: lastWorkflowTask.GetEventId(),
		RequestId:                 uuid.New(),
	})
	s.NoError(err)

	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("Poll and process second activity", tag.Error(err))
	s.NoError(err)

	err = poller.PollAndProcessActivityTask(false)
	s.Logger.Info("Poll and process third activity", tag.Error(err))
	s.NoError(err)

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("Poll and process final workflow task", tag.Error(err))
	s.NoError(err)

	s.NotNil(firstActivityCompletionEvent)
	s.True(workflowComplete)

	descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resetResp.GetRunId(),
		},
	})
	s.NoError(err)
	s.Equal(we.RunId, descResp.WorkflowExecutionInfo.GetFirstRunId())
}

func (s *ResetWorkflowTestSuite) runWorkflowWithPoller(tv *testvars.TestVars) []*commonpb.WorkflowExecution {
	var executions []*commonpb.WorkflowExecution
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		executions = append(executions, task.WorkflowExecution)
		time.Sleep(200 * time.Millisecond) //nolint:forbidigo
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("simple success"),
				}},
			}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	return executions
}

func (s *ResetWorkflowTestSuite) TestResetWorkflowAfterTimeout() {
	startTime := time.Now().UTC()
	tv := testvars.New(s.T())
	tv.WorkerIdentity()

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                uuid.New(),
		Namespace:                s.Namespace(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		Input:                    nil,
		WorkflowRunTimeout:       durationpb.New(1 * time.Second),
		WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
		Identity:                 tv.WorkerIdentity(),
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)

	s.runWorkflowWithPoller(tv)

	var historyEvents []*historypb.HistoryEvent
	s.Eventually(func() bool {
		historyEvents = s.GetHistory(s.Namespace(), &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      we.RunId,
		})
		lastEvent := historyEvents[len(historyEvents)-1]
		s.NotNil(historyEvents)

		return lastEvent.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED

	}, 2*time.Second, 200*time.Millisecond)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted`, historyEvents)

	// wait till workflow is closed
	closedCount := 0
	s.Eventually(func() bool {
		resp, err := s.FrontendClient().ListClosedWorkflowExecutions(testcore.NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       s.Namespace(),
			MaximumPageSize: 100,
			StartTimeFilter: &filterpb.StartTimeFilter{
				EarliestTime: timestamppb.New(startTime),
				LatestTime:   timestamppb.New(time.Now().UTC()),
			},
			Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: tv.WorkflowID(),
			}},
		})
		s.NoError(err)
		closedCount = len(resp.Executions)
		if closedCount == 0 {
			s.Logger.Info("Closed WorkflowExecution is not yet visible")
		}

		return closedCount > 0

	}, 5*time.Second, 500*time.Millisecond)
	s.Equal(1, closedCount)

	// make sure we are past timeout time
	time.Sleep(time.Second) //nolint:forbidigo

	_, err = s.FrontendClient().ResetWorkflowExecution(testcore.NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		RequestId:                 uuid.New(),
		WorkflowTaskFinishEventId: 3,
	})
	s.NoError(err)

	executions := s.runWorkflowWithPoller(tv)

	events := s.GetHistory(s.Namespace(), executions[0])

	s.EqualHistoryEvents(`
	1 WorkflowExecutionStarted {"Attempt":1}
	2 WorkflowTaskScheduled
	3 WorkflowTaskStarted
	4 WorkflowTaskFailed
	5 WorkflowTaskScheduled
	6 WorkflowTaskStarted
	7 WorkflowTaskCompleted
	8 WorkflowExecutionCompleted`, events)
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ExcludeNoneReapplyDefault() {
	t := resetTest{
		ResetWorkflowTestSuite: s,
		tv:                     testvars.New(s.T()),
	}
	t.run()
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ExcludeNoneReapplyAll() {
	t := resetTest{
		ResetWorkflowTestSuite: s,
		tv:                     testvars.New(s.T()),
		reapplyExcludeTypes:    []enumspb.ResetReapplyExcludeType{},
		reapplyType:            enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
	}
	t.run()
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ExcludeNoneReapplySignal() {
	t := resetTest{
		ResetWorkflowTestSuite: s,
		tv:                     testvars.New(s.T()),
		reapplyExcludeTypes:    []enumspb.ResetReapplyExcludeType{},
		reapplyType:            enumspb.RESET_REAPPLY_TYPE_SIGNAL,
	}
	t.run()
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ExcludeNoneReapplyNone() {
	t := resetTest{
		ResetWorkflowTestSuite: s,
		tv:                     testvars.New(s.T()),
		reapplyExcludeTypes:    []enumspb.ResetReapplyExcludeType{},
		reapplyType:            enumspb.RESET_REAPPLY_TYPE_NONE,
	}
	t.run()
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ExcludeSignalReapplyAll() {
	t := resetTest{
		ResetWorkflowTestSuite: s,
		tv:                     testvars.New(s.T()),
		reapplyExcludeTypes:    []enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
		reapplyType:            enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
	}
	t.run()
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ExcludeSignalReapplySignal() {
	t := resetTest{
		ResetWorkflowTestSuite: s,
		tv:                     testvars.New(s.T()),
		reapplyExcludeTypes:    []enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
		reapplyType:            enumspb.RESET_REAPPLY_TYPE_SIGNAL,
	}
	t.run()
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ExcludeSignalReapplyNone() {
	t := resetTest{
		ResetWorkflowTestSuite: s,
		tv:                     testvars.New(s.T()),
		reapplyExcludeTypes:    []enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
		reapplyType:            enumspb.RESET_REAPPLY_TYPE_NONE,
	}
	t.run()
}

type resetTest struct {
	*ResetWorkflowTestSuite
	tv                  *testvars.TestVars
	reapplyExcludeTypes []enumspb.ResetReapplyExcludeType
	reapplyType         enumspb.ResetReapplyType
	totalSignals        int
	totalUpdates        int
	wftCounter          int
	commandsCompleted   bool
	messagesCompleted   bool
}

func (t resetTest) sendSignalAndProcessWFT(poller *testcore.TaskPoller) {
	signalRequest := &workflowservice.SignalWorkflowExecutionRequest{
		RequestId:         uuid.New(),
		Namespace:         t.Namespace(),
		WorkflowExecution: t.tv.WorkflowExecution(),
		SignalName:        t.tv.HandlerName(),
		Input:             t.tv.Any().Payloads(),
		Identity:          t.tv.WorkerIdentity(),
	}
	_, err := t.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), signalRequest)
	t.NoError(err)
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	t.NoError(err)
}

func (t resetTest) sendUpdateAndProcessWFT(updateId string, poller *testcore.TaskPoller) {
	t.ResetWorkflowTestSuite.sendUpdateNoErrorWaitPolicyAccepted(t.tv, updateId)
	// Blocks until the update request causes a WFT to be dispatched; then sends the update acceptance message
	// required for the update request to return.
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	t.NoError(err)
}

func (s *ResetWorkflowTestSuite) sendUpdateNoErrorWaitPolicyAccepted(tv *testvars.TestVars, updateID string) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	return s.sendUpdateNoErrorInternal(tv, updateID, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
}

func (t *resetTest) messageHandler(_ *workflowservice.PollWorkflowTaskQueueResponse) ([]*protocolpb.Message, error) {

	// Increment WFT counter here; messageHandler is invoked prior to wftHandler
	t.wftCounter++

	// There's an initial empty WFT; then come `totalUpdates` updates, followed by `totalSignals` signals, each in a
	// separate WFT. We must accept the updates, but otherwise respond with empty messages.
	if t.wftCounter == t.totalUpdates+t.totalSignals+1 {
		t.messagesCompleted = true
	}
	if t.wftCounter > t.totalSignals+1 {
		updateId := fmt.Sprint(t.wftCounter - t.totalSignals - 1)
		return []*protocolpb.Message{
			{
				Id:                 "accept-" + updateId,
				ProtocolInstanceId: t.tv.UpdateID(updateId),
				Body: protoutils.MarshalAny(t.T(), &updatepb.Acceptance{
					AcceptedRequestMessageId:         "fake-request-message-id",
					AcceptedRequestSequencingEventId: int64(-1),
				}),
			},
		}, nil
	}
	return []*protocolpb.Message{}, nil

}

func (t *resetTest) wftHandler(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
	commands := []*commandpb.Command{}

	// There's an initial empty WFT; then come `totalSignals` signals, followed by `totalUpdates` updates, each in
	// a separate WFT. We must send COMPLETE_WORKFLOW_EXECUTION in the final WFT.
	if t.wftCounter > t.totalSignals+1 {
		updateId := fmt.Sprint(t.wftCounter - t.totalSignals - 1)
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
			Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
				MessageId: "accept-" + updateId,
			}},
		})
	}
	if t.wftCounter == t.totalSignals+t.totalUpdates+1 {
		t.commandsCompleted = true
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: t.tv.Any().Payloads(),
				},
			},
		})
	}
	return commands, nil
}

func (t resetTest) reset(eventId int64) string {
	resp, err := t.FrontendClient().ResetWorkflowExecution(testcore.NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 t.Namespace(),
		WorkflowExecution:         t.tv.WorkflowExecution(),
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: eventId,
		RequestId:                 uuid.New(),
		ResetReapplyType:          t.reapplyType,
		ResetReapplyExcludeTypes:  t.reapplyExcludeTypes,
	})
	t.NoError(err)
	return resp.RunId
}

func (t *resetTest) run() {
	t.totalSignals = 2
	t.totalUpdates = 2
	t.tv = t.WorkflowUpdateBaseSuite.startWorkflow(t.tv)

	poller := &testcore.TaskPoller{
		Client:              t.FrontendClient(),
		Namespace:           t.Namespace(),
		TaskQueue:           t.tv.TaskQueue(),
		Identity:            t.tv.WorkerIdentity(),
		WorkflowTaskHandler: t.wftHandler,
		MessageHandler:      t.messageHandler,
		Logger:              t.Logger,
		T:                   t.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	t.NoError(err)

	t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
`, t.GetHistory(t.Namespace(), t.tv.WorkflowExecution()))

	for i := 1; i <= t.totalSignals; i++ {
		t.sendSignalAndProcessWFT(poller)
	}
	for i := 1; i <= t.totalUpdates; i++ {
		t.sendUpdateAndProcessWFT(fmt.Sprint(i), poller)
	}
	t.True(t.commandsCompleted)
	t.True(t.messagesCompleted)

	t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionSignaled
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 WorkflowTaskScheduled
 14 WorkflowTaskStarted
 15 WorkflowTaskCompleted
 16 WorkflowExecutionUpdateAccepted
 17 WorkflowTaskScheduled
 18 WorkflowTaskStarted
 19 WorkflowTaskCompleted
 20 WorkflowExecutionUpdateAccepted
 21 WorkflowExecutionCompleted
`, t.GetHistory(t.Namespace(), t.tv.WorkflowExecution()))

	resetToEventId := int64(4)
	newRunId := t.reset(resetToEventId)
	t.tv = t.tv.WithRunID(newRunId)
	events := t.GetHistory(t.Namespace(), t.tv.WorkflowExecution())

	resetReapplyExcludeTypes := resetworkflow.GetResetReapplyExcludeTypes(t.reapplyExcludeTypes, t.reapplyType)
	signals := !resetReapplyExcludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL]
	updates := !resetReapplyExcludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE]

	if !signals && !updates {
		t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowTaskScheduled
`, events)
	} else if !signals && updates {
		t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionUpdateAdmitted
  6 WorkflowExecutionUpdateAdmitted
  7 WorkflowTaskScheduled
`, events)
	} else if signals && !updates {
		t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
`, events)
	} else {
		t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionSignaled
  7 WorkflowExecutionUpdateAdmitted
  8 WorkflowExecutionUpdateAdmitted
  9 WorkflowTaskScheduled
`, events)
		resetToEventId := int64(4)
		newRunId := t.reset(resetToEventId)
		t.tv = t.tv.WithRunID(newRunId)
		events = t.GetHistory(t.Namespace(), t.tv.WorkflowExecution())
		t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionSignaled
  7 WorkflowExecutionUpdateAdmitted
  8 WorkflowExecutionUpdateAdmitted
  9 WorkflowTaskScheduled
`, events)
	}
}

func (s *ResetWorkflowTestSuite) TestBufferedSignalIsReappliedOnReset() {
	tv := testvars.New(s.T())
	s.testResetWorkflowSignalReapplyBuffer(tv, enumspb.RESET_REAPPLY_TYPE_SIGNAL)
}

func (s *ResetWorkflowTestSuite) TestBufferedSignalIsDroppedOnReset() {
	tv := testvars.New(s.T())
	s.testResetWorkflowSignalReapplyBuffer(tv, enumspb.RESET_REAPPLY_TYPE_NONE)
}

func (s *ResetWorkflowTestSuite) testResetWorkflowSignalReapplyBuffer(
	tv *testvars.TestVars,
	reapplyType enumspb.ResetReapplyType,
) {
	/*
		Test scenario:
		- while the worker is processing a WFT, a Signal and a Reset arrive
		- then, the worker responds with a CompleteWorkflowExecution command
		- depending on the reapply type, the buffered signal is applied post-reset or not
	*/

	tv = s.startWorkflow(tv)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(tv.RunID()))

	var resetRunID string
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if resetRunID == "" {
			s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, task.History.Events)

			// (1) send Signal
			_, err := s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(),
				&workflowservice.SignalWorkflowExecutionRequest{
					RequestId: uuid.New(),
					Namespace: s.Namespace(),
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: tv.WorkflowID(),
						RunId:      tv.RunID(),
					},
					SignalName: "random signal name",
					Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
						Data: []byte("random data"),
					}}},
					Identity: tv.WorkerIdentity(),
				})
			s.NoError(err)

			// (2) send Reset
			resp, err := s.FrontendClient().ResetWorkflowExecution(testcore.NewContext(),
				&workflowservice.ResetWorkflowExecutionRequest{
					Namespace: s.Namespace(),
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: tv.WorkflowID(),
						RunId:      tv.RunID(),
					},
					Reason:                    "reset execution from test",
					WorkflowTaskFinishEventId: 3,
					RequestId:                 uuid.New(),
					ResetReapplyType:          reapplyType,
				})
			s.NoError(err)
			resetRunID = resp.RunId

			return []*commandpb.Command{}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.Error(err) // due to workflow termination (reset)

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	events := s.GetHistory(s.Namespace(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: resetRunID})
	switch reapplyType {
	case enumspb.RESET_REAPPLY_TYPE_SIGNAL:
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled  // reapplied Signal
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionCompleted
`, events)
	case enumspb.RESET_REAPPLY_TYPE_NONE:
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowTaskScheduled  // no reapplied Signal
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionCompleted
`, events)
	default:
		panic(fmt.Sprintf("unknown reset reapply type: %v", reapplyType))
	}
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_WorkflowTask_Schedule() {
	workflowID := "functional-reset-workflow-test-schedule"
	workflowTypeName := "functional-reset-workflow-test-schedule-type"
	taskQueueName := "functional-reset-workflow-test-schedule-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(workflowID, workflowTypeName, taskQueueName, 3)
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_WorkflowTask_ScheduleToStart() {
	workflowID := "functional-reset-workflow-test-schedule-to-start"
	workflowTypeName := "functional-reset-workflow-test-schedule-to-start-type"
	taskQueueName := "functional-reset-workflow-test-schedule-to-start-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(workflowID, workflowTypeName, taskQueueName, 4)
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_WorkflowTask_Start() {
	workflowID := "functional-reset-workflow-test-start"
	workflowTypeName := "functional-reset-workflow-test-start-type"
	taskQueueName := "functional-reset-workflow-test-start-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(workflowID, workflowTypeName, taskQueueName, 5)
}

func (s *ResetWorkflowTestSuite) testResetWorkflowRangeScheduleToStart(
	workflowID string,
	workflowTypeName string,
	taskQueueName string,
	resetToEventID int64,
) {
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}
	taskQueue := &taskqueuepb.TaskQueue{Name: taskQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.Namespace(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)

	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.Namespace(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.RunId,
		},
		SignalName: "random signal name",
		Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{
			{Data: []byte("random signal payload")},
		}},
		Identity: identity,
	})
	s.NoError(err)

	// workflow logic
	workflowComplete := false
	isWorkflowTaskProcessed := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !isWorkflowTaskProcessed {
			isWorkflowTaskProcessed = true
			return []*commandpb.Command{}, nil
		}

		// Complete workflow after reset
		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
		}}, nil

	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// events layout
	//  1. WorkflowExecutionStarted
	//  2. WorkflowTaskScheduled
	//  3. WorkflowExecutionSignaled
	//  4. WorkflowTaskStarted
	//  5. WorkflowTaskCompleted

	// Reset workflow execution
	_, err = s.FrontendClient().ResetWorkflowExecution(testcore.NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: resetToEventID,
		RequestId:                 uuid.New(),
	})
	s.NoError(err)

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)
}

func CaNOnceWorkflow(ctx workflow.Context, input string) (string, error) {
	if input != "don't CaN" {
		return input, workflow.NewContinueAsNewError(ctx, CaNOnceWorkflow, "don't CaN")
	}
	return input, nil
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ResetAfterContinueAsNew() {
	id := "functional-reset-workflow-test"
	tq := "functional-reset-workflow-test-taskqueue"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// get sdkClient
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.HostPort(),
		Namespace: s.Namespace(),
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}

	// start workflow that does CaN once
	w := worker.New(sdkClient, tq, worker.Options{Identity: id})
	w.RegisterWorkflow(CaNOnceWorkflow)
	s.NoError(w.Start())
	defer w.Stop()
	run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, CaNOnceWorkflow, "")
	s.NoError(err)

	// wait for your workflow and its CaN to complete
	s.Eventually(func() bool {
		resp, err := s.FrontendClient().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: s.Namespace(),
			Query:     fmt.Sprintf("WorkflowId = \"%s\" AND ExecutionStatus != \"Running\"", run.GetID()),
		})
		s.NoError(err)
		return resp.GetCount() >= 2
	}, 30*time.Second, time.Second)

	wfExec := &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(),
		RunId:      run.GetRunID(),
	}

	// Find reset point (last completed workflow task)
	events := s.GetHistory(s.Namespace(), wfExec)
	var lastWorkflowTask *historypb.HistoryEvent
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			lastWorkflowTask = event
		}
	}

	// reset the original workflow
	_, err = s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace(),
		WorkflowExecution:         wfExec,
		WorkflowTaskFinishEventId: lastWorkflowTask.GetEventId(),
		RequestId:                 uuid.New(),
	})
	s.NoError(err)
}
