package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/api/resetworkflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ResetWorkflowTestSuite struct {
	WorkflowUpdateBaseSuite
}

func TestResetWorkflowTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ResetWorkflowTestSuite))
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
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
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
		Namespace:           s.Namespace().String(),
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
	events := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
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
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: lastWorkflowTask.GetEventId(),
		RequestId:                 uuid.NewString(),
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
		Namespace: s.Namespace().String(),
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
		Namespace:           s.Namespace().String(),
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
		RequestId:                uuid.NewString(),
		Namespace:                s.Namespace().String(),
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
		historyEvents = s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
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
			Namespace:       s.Namespace().String(),
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
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: 3,
	})
	s.NoError(err)

	executions := s.runWorkflowWithPoller(tv)

	events := s.GetHistory(s.Namespace().String(), executions[0])

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

//nolint:staticcheck // SA1019 TaskPoller replacement needs to be done holistically.
func (t *resetTest) sendSignalAndProcessWFT(poller *testcore.TaskPoller) {
	signalRequest := &workflowservice.SignalWorkflowExecutionRequest{
		RequestId:         uuid.NewString(),
		Namespace:         t.Namespace().String(),
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

//nolint:staticcheck // SA1019 TaskPoller replacement needs to be done holistically.
func (t *resetTest) sendUpdateAndProcessWFT(tv *testvars.TestVars, poller *testcore.TaskPoller) {
	t.ResetWorkflowTestSuite.sendUpdateNoErrorWaitPolicyAccepted(tv)
	// Blocks until the update request causes a WFT to be dispatched; then sends the update acceptance message
	// required for the update request to return.
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	t.NoError(err)
}

func (t *resetTest) sendStartWorkflowRequestWithOptions(
	tv *testvars.TestVars,
	optsFn ...func(request *workflowservice.StartWorkflowExecutionRequest),
) *workflowservice.StartWorkflowExecutionResponse {
	request := t.startWorkflowRequest(tv)
	for _, fn := range optsFn {
		fn(request)
	}
	resp, err := t.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	t.NoError(err)
	return resp
}

func (s *ResetWorkflowTestSuite) sendUpdateNoErrorWaitPolicyAccepted(tv *testvars.TestVars) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	return s.sendUpdateNoErrorInternal(tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
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
		updateID := t.wftCounter - t.totalSignals - 1
		tv := t.tv.WithUpdateIDNumber(updateID).WithMessageIDNumber(updateID)
		return []*protocolpb.Message{
			{
				Id:                 tv.MessageID() + "_update-accepted",
				ProtocolInstanceId: tv.UpdateID(),
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
	var commands []*commandpb.Command

	// There's an initial empty WFT; then come `totalSignals` signals, followed by `totalUpdates` updates, each in
	// a separate WFT. We must send COMPLETE_WORKFLOW_EXECUTION in the final WFT.
	if t.wftCounter > t.totalSignals+1 {
		updateID := t.wftCounter - t.totalSignals - 1
		tv := t.tv.WithMessageIDNumber(updateID)
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
			Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
				MessageId: tv.MessageID() + "_update-accepted",
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

func (t *resetTest) reset(eventId int64) string {
	resp, err := t.FrontendClient().ResetWorkflowExecution(testcore.NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 t.Namespace().String(),
		WorkflowExecution:         t.tv.WorkflowExecution(),
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: eventId,
		RequestId:                 uuid.NewString(),
		ResetReapplyType:          t.reapplyType,
		ResetReapplyExcludeTypes:  t.reapplyExcludeTypes,
	})
	t.NoError(err)
	return resp.RunId
}

func (t *resetTest) run() {
	t.totalSignals = 2
	t.totalUpdates = 2
	runID := t.WorkflowUpdateBaseSuite.startWorkflow(t.tv)

	poller := &testcore.TaskPoller{
		Client:              t.FrontendClient(),
		Namespace:           t.Namespace().String(),
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
`, t.GetHistory(t.Namespace().String(), t.tv.WorkflowExecution()))

	// Trying to start workflow with same WorkflowID will attach the RequestID to the existing workflow.
	onConflictOptions := &workflowpb.OnConflictOptions{AttachRequestId: true}
	resp := t.sendStartWorkflowRequestWithOptions(
		t.tv,
		func(request *workflowservice.StartWorkflowExecutionRequest) {
			request.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
			request.OnConflictOptions = onConflictOptions
		},
	)
	t.Equal(runID, resp.RunId)
	requireNotStartedButRunning(t.T(), resp)

	for i := 1; i <= t.totalSignals; i++ {
		t.sendSignalAndProcessWFT(poller)
	}
	for i := 1; i <= t.totalUpdates; i++ {
		t.sendUpdateAndProcessWFT(t.tv.WithUpdateIDNumber(i), poller)
	}
	t.True(t.commandsCompleted)
	t.True(t.messagesCompleted)

	events := t.GetHistory(t.Namespace().String(), t.tv.WorkflowExecution())
	t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionOptionsUpdated
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 WorkflowExecutionSignaled
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted
 16 WorkflowTaskCompleted
 17 WorkflowExecutionUpdateAccepted
 18 WorkflowTaskScheduled
 19 WorkflowTaskStarted
 20 WorkflowTaskCompleted
 21 WorkflowExecutionUpdateAccepted
 22 WorkflowExecutionCompleted
`, events)

	// Find the RequestID from the second start workflow request that's attached to the running workflow.
	attachedRequestID := ""
	for _, ev := range events {
		if ev.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
			attachedRequestID = ev.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetAttachedRequestId()
			break
		}
	}
	t.NotEmpty(attachedRequestID)

	resetToEventId := int64(4)
	newRunId := t.reset(resetToEventId)
	t.tv = t.tv.WithRunID(newRunId)
	events = t.GetHistory(t.Namespace().String(), t.tv.WorkflowExecution())

	resetReapplyExcludeTypes := resetworkflow.GetResetReapplyExcludeTypes(t.reapplyExcludeTypes, t.reapplyType)
	_, noSignals := resetReapplyExcludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL]
	_, noUpdates := resetReapplyExcludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE]

	expectedHistory := ""
	signals := !noSignals
	updates := !noUpdates
	if !signals && !updates {
		expectedHistory = `
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionOptionsUpdated // this event is always reapplied
  6 WorkflowTaskScheduled`
	} else if !signals && updates {
		expectedHistory = `
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionOptionsUpdated // this event is always reapplied
  6 WorkflowExecutionUpdateAdmitted
  7 WorkflowExecutionUpdateAdmitted
  8 WorkflowTaskScheduled`
	} else if signals && !updates {
		expectedHistory = `
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionOptionsUpdated // this event is always reapplied
  6 WorkflowExecutionSignaled
  7 WorkflowExecutionSignaled
  8 WorkflowTaskScheduled`
	} else {
		expectedHistory = `
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionOptionsUpdated // this event is always reapplied
  6 WorkflowExecutionSignaled
  7 WorkflowExecutionSignaled
  8 WorkflowExecutionUpdateAdmitted
  9 WorkflowExecutionUpdateAdmitted
 10 WorkflowTaskScheduled`
	}
	t.EqualHistoryEvents(expectedHistory, events)

	if signals && updates {
		resetToEventId := int64(4)
		newRunId = t.reset(resetToEventId)
		t.tv = t.tv.WithRunID(newRunId)
		events = t.GetHistory(t.Namespace().String(), t.tv.WorkflowExecution())
		expectedHistory = `
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionOptionsUpdated
  6 WorkflowExecutionSignaled
  7 WorkflowExecutionSignaled
  8 WorkflowExecutionUpdateAdmitted
  9 WorkflowExecutionUpdateAdmitted
 10 WorkflowTaskScheduled`
		t.EqualHistoryEvents(expectedHistory, events)
	}

	// Send another start workflow with the same RequestID that got attached to verify it's deduped.
	resp = t.sendStartWorkflowRequestWithOptions(
		t.tv,
		func(request *workflowservice.StartWorkflowExecutionRequest) {
			request.RequestId = attachedRequestID
			request.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
			request.OnConflictOptions = onConflictOptions
		},
	)
	t.Equal(newRunId, resp.RunId)
	requireNotStartedButRunning(t.T(), resp)

	// History events must be the same.
	events = t.GetHistory(t.Namespace().String(), t.tv.WorkflowExecution())
	t.EqualHistoryEvents(expectedHistory, events)
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

	runID := s.startWorkflow(tv)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(runID))

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
					RequestId:         tv.RequestID(),
					Namespace:         s.Namespace().String(),
					WorkflowExecution: tv.WorkflowExecution(),
					SignalName:        tv.Any().String(),
					Input:             tv.Any().Payloads(),
					Identity:          tv.WorkerIdentity(),
				})
			s.NoError(err)

			// (2) send Reset
			resp, err := s.FrontendClient().ResetWorkflowExecution(testcore.NewContext(),
				&workflowservice.ResetWorkflowExecutionRequest{
					Namespace:                 s.Namespace().String(),
					WorkflowExecution:         tv.WorkflowExecution(),
					Reason:                    "reset execution from test",
					WorkflowTaskFinishEventId: 3,
					RequestId:                 tv.Any().String(),
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
		Namespace:           s.Namespace().String(),
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

	events := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: resetRunID})
	switch reapplyType { // nolint:exhaustive
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
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
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
		Namespace: s.Namespace().String(),
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
		Namespace:           s.Namespace().String(),
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
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: resetToEventID,
		RequestId:                 uuid.NewString(),
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
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s.Worker().RegisterWorkflow(CaNOnceWorkflow)
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: s.TaskQueue()}, CaNOnceWorkflow, "")
	s.NoError(err)

	// wait for your workflow and its CaN to complete
	s.Eventually(func() bool {
		resp, err := s.FrontendClient().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
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
	events := s.GetHistory(s.Namespace().String(), wfExec)
	var lastWorkflowTask *historypb.HistoryEvent
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			lastWorkflowTask = event
		}
	}

	// reset the original workflow
	_, err = s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         wfExec,
		WorkflowTaskFinishEventId: lastWorkflowTask.GetEventId(),
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)
}

func (s *ResetWorkflowTestSuite) TestResetWorkflowWithExternalPayloads() {
	s.OverrideDynamicConfig(dynamicconfig.ExternalPayloadsEnabled, true)

	// This test verifies that ExternalPayloadSize and ExternalPayloadCount are correctly
	// tracked when a workflow is reset. It resets to a point before the activity completes,
	// so only the workflow input external payload should be counted.
	workflowID := "functional-reset-workflow-external-payload-test"
	workflowType := "functional-reset-workflow-external-payload-test-type"
	taskQueue := "functional-reset-workflow-external-payload-test-taskqueue"
	identity := "worker1"

	// External payload in workflow input
	workflowExternalPayloadSize := int64(1024)
	workflowInputPayload := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{
				ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
					{SizeBytes: workflowExternalPayloadSize},
				},
			},
		},
	}

	activityExternalPayloadSize := int64(2048)
	activityInputPayload := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{
				ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
					{SizeBytes: activityExternalPayloadSize},
				},
			},
		},
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               workflowInputPayload,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	})
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// Workflow handler - schedules activity on first task, completes on second task
	isFirstTaskProcessed := false
	workflowComplete := false
	wtHandler := func(_ *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !isFirstTaskProcessed {
			isFirstTaskProcessed = true
			// Schedule an activity
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             "activity1",
						ActivityType:           &commonpb.ActivityType{Name: "TestActivity"},
						TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						Input:                  activityInputPayload,
						ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
						ScheduleToStartTimeout: durationpb.New(100 * time.Second),
						StartToCloseTimeout:    durationpb.New(50 * time.Second),
						HeartbeatTimeout:       durationpb.New(5 * time.Second),
					},
				},
			}}, nil
		}
		workflowComplete = true
		// Complete workflow after activity
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	tv := testvars.New(s.T()).WithTaskQueue(taskQueue)
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())

	// Process first workflow task to schedule activities
	_, err := poller.PollAndHandleWorkflowTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		cmds, err := wtHandler(task)
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: cmds}, err
	})
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Process one activity task which also creates second workflow task
	_, err = poller.PollAndHandleActivityTask(tv, func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		return &workflowservice.RespondActivityTaskCompletedRequest{Result: payloads.EncodeString("Activity Result")}, nil
	})
	s.Logger.Info("Poll and process first activity", tag.Error(err))
	s.NoError(err)

	// Process second workflow task which checks activity completion
	_, err = poller.PollAndHandleWorkflowTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		cmds, err := wtHandler(task)
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: cmds}, err
	})
	s.Logger.Info("Poll and process second workflow task", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)

	descResp, descErr := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.GetRunId(),
		},
	})
	s.NoError(descErr)
	s.Equal(int64(2), descResp.WorkflowExecutionInfo.ExternalPayloadCount)
	s.Equal(workflowExternalPayloadSize+activityExternalPayloadSize, descResp.WorkflowExecutionInfo.ExternalPayloadSizeBytes)

	// Get history to find reset point (first completed workflow task)
	events := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      we.GetRunId(),
	})

	var resetToEventID int64
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			resetToEventID = event.GetEventId()
			break
		}
	}
	s.Positive(resetToEventID, "Should have found first completed workflow task")

	resetResp, err := s.FrontendClient().ResetWorkflowExecution(testcore.NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: resetToEventID,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)
	s.Logger.Info("Workflow reset complete", tag.WorkflowRunID(resetResp.GetRunId()), tag.NewInt64("ResetToEventID", resetToEventID))

	descResp, descErr = s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      resetResp.GetRunId(),
		},
	})
	s.NoError(descErr)

	// Verify external payload stats after reset
	s.NotNil(descResp.WorkflowExecutionInfo.ExternalPayloadCount)
	s.Equal(int64(1), descResp.WorkflowExecutionInfo.ExternalPayloadCount)
	s.Equal(workflowExternalPayloadSize, descResp.WorkflowExecutionInfo.ExternalPayloadSizeBytes)
}
