package tests

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/api/resetworkflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ResetWorkflowTestSuite struct {
	parallelsuite.Suite[*ResetWorkflowTestSuite]
}

func TestResetWorkflowTestSuite(t *testing.T) {
	parallelsuite.Run(t, &ResetWorkflowTestSuite{})
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow() {
	env := testcore.NewEnv(s.T())

	id := "functional-reset-workflow-test"
	wt := "functional-reset-workflow-test-type"
	tq := "functional-reset-workflow-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

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
			s.NoError(binary.Write(buf, binary.LittleEndian, activityData))

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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	// Process first workflow task to schedule activities
	_, err := poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Process one activity task which also creates second workflow task
	err = poller.PollAndProcessActivityTask(false)
	env.Logger.Info("Poll and process first activity", tag.Error(err))
	s.NoError(err)

	// Process second workflow task which checks activity completion
	_, err = poller.PollAndProcessWorkflowTask()
	env.Logger.Info("Poll and process second workflow task", tag.Error(err))
	s.NoError(err)

	// Find reset point (last completed workflow task)
	events := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
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
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
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
	env.Logger.Info("Poll and process second activity", tag.Error(err))
	s.NoError(err)

	err = poller.PollAndProcessActivityTask(false)
	env.Logger.Info("Poll and process third activity", tag.Error(err))
	s.NoError(err)

	_, err = poller.PollAndProcessWorkflowTask()
	env.Logger.Info("Poll and process final workflow task", tag.Error(err))
	s.NoError(err)

	s.NotNil(firstActivityCompletionEvent)
	s.True(workflowComplete)

	descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resetResp.GetRunId(),
		},
	})
	s.NoError(err)
	s.Equal(we.RunId, descResp.WorkflowExecutionInfo.GetFirstRunId())
}

func (s *ResetWorkflowTestSuite) runWorkflowWithPoller(env *testcore.TestEnv, tv *testvars.TestVars) []*commonpb.WorkflowExecution {
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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	return executions
}

func (s *ResetWorkflowTestSuite) TestResetWorkflowAfterTimeout() {
	env := testcore.NewEnv(s.T())

	startTime := time.Now().UTC()
	tv := testvars.New(s.T())
	tv.WorkerIdentity()

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                uuid.NewString(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		Input:                    nil,
		WorkflowRunTimeout:       durationpb.New(1 * time.Second),
		WorkflowExecutionTimeout: durationpb.New(1 * time.Second),
		Identity:                 tv.WorkerIdentity(),
	}

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	s.runWorkflowWithPoller(env, tv)

	var historyEvents []*historypb.HistoryEvent
	s.Eventually(func() bool {
		historyEvents = env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
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
		resp, err := env.FrontendClient().ListClosedWorkflowExecutions(s.Context(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       env.Namespace().String(),
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
			env.Logger.Info("Closed WorkflowExecution is not yet visible")
		}

		return closedCount > 0

	}, 5*time.Second, 500*time.Millisecond)
	s.Equal(1, closedCount)

	// make sure we are past timeout time
	time.Sleep(time.Second) //nolint:forbidigo

	_, err = env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: 3,
	})
	s.NoError(err)

	executions := s.runWorkflowWithPoller(env, tv)

	events := env.GetHistory(env.Namespace().String(), executions[0])

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
	env := testcore.NewEnv(s.T())
	t := resetTest{
		ResetWorkflowTestSuite: s,
		env:                    env,
		tv:                     testvars.New(s.T()),
	}
	t.run()
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ExcludeNoneReapplyAll() {
	env := testcore.NewEnv(s.T())
	t := resetTest{
		ResetWorkflowTestSuite: s,
		env:                    env,
		tv:                     testvars.New(s.T()),
		reapplyExcludeTypes:    []enumspb.ResetReapplyExcludeType{},
		reapplyType:            enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
	}
	t.run()
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ExcludeNoneReapplySignal() {
	env := testcore.NewEnv(s.T())
	t := resetTest{
		ResetWorkflowTestSuite: s,
		env:                    env,
		tv:                     testvars.New(s.T()),
		reapplyExcludeTypes:    []enumspb.ResetReapplyExcludeType{},
		reapplyType:            enumspb.RESET_REAPPLY_TYPE_SIGNAL,
	}
	t.run()
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ExcludeNoneReapplyNone() {
	env := testcore.NewEnv(s.T())
	t := resetTest{
		ResetWorkflowTestSuite: s,
		env:                    env,
		tv:                     testvars.New(s.T()),
		reapplyExcludeTypes:    []enumspb.ResetReapplyExcludeType{},
		reapplyType:            enumspb.RESET_REAPPLY_TYPE_NONE,
	}
	t.run()
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ExcludeSignalReapplyAll() {
	env := testcore.NewEnv(s.T())
	t := resetTest{
		ResetWorkflowTestSuite: s,
		env:                    env,
		tv:                     testvars.New(s.T()),
		reapplyExcludeTypes:    []enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
		reapplyType:            enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
	}
	t.run()
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ExcludeSignalReapplySignal() {
	env := testcore.NewEnv(s.T())
	t := resetTest{
		ResetWorkflowTestSuite: s,
		env:                    env,
		tv:                     testvars.New(s.T()),
		reapplyExcludeTypes:    []enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
		reapplyType:            enumspb.RESET_REAPPLY_TYPE_SIGNAL,
	}
	t.run()
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ExcludeSignalReapplyNone() {
	env := testcore.NewEnv(s.T())
	t := resetTest{
		ResetWorkflowTestSuite: s,
		env:                    env,
		tv:                     testvars.New(s.T()),
		reapplyExcludeTypes:    []enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
		reapplyType:            enumspb.RESET_REAPPLY_TYPE_NONE,
	}
	t.run()
}

type resetTest struct {
	*ResetWorkflowTestSuite
	env                 *testcore.TestEnv
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
		Namespace:         t.env.Namespace().String(),
		WorkflowExecution: t.tv.WorkflowExecution(),
		SignalName:        t.tv.HandlerName(),
		Input:             t.tv.Any().Payloads(),
		Identity:          t.tv.WorkerIdentity(),
	}
	_, err := t.env.FrontendClient().SignalWorkflowExecution(t.Context(), signalRequest)
	t.NoError(err)
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	t.NoError(err)
}

//nolint:staticcheck // SA1019 TaskPoller replacement needs to be done holistically.
func (t *resetTest) sendUpdateAndProcessWFT(tv *testvars.TestVars, poller *testcore.TaskPoller) {
	sendUpdateNoErrorWaitPolicyAccepted(t.env, tv)
	// Blocks until the update request causes a WFT to be dispatched; then sends the update acceptance message
	// required for the update request to return.
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	t.NoError(err)
}

func (t *resetTest) sendStartWorkflowRequestWithOptions(
	tv *testvars.TestVars,
	optsFn ...func(request *workflowservice.StartWorkflowExecutionRequest),
) *workflowservice.StartWorkflowExecutionResponse {
	request := startWorkflowRequest(t.env, tv)
	for _, fn := range optsFn {
		fn(request)
	}
	resp, err := t.env.FrontendClient().StartWorkflowExecution(t.Context(), request)
	t.NoError(err)
	return resp
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
	resp, err := t.env.FrontendClient().ResetWorkflowExecution(t.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 t.env.Namespace().String(),
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
	runID := mustStartWorkflow(t.env, t.tv)

	poller := &testcore.TaskPoller{
		Client:              t.env.FrontendClient(),
		Namespace:           t.env.Namespace().String(),
		TaskQueue:           t.tv.TaskQueue(),
		Identity:            t.tv.WorkerIdentity(),
		WorkflowTaskHandler: t.wftHandler,
		MessageHandler:      t.messageHandler,
		Logger:              t.env.Logger,
		T:                   t.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	t.NoError(err)

	t.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
`, t.env.GetHistory(t.env.Namespace().String(), t.tv.WorkflowExecution()))

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

	events := t.env.GetHistory(t.env.Namespace().String(), t.tv.WorkflowExecution())
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
	events = t.env.GetHistory(t.env.Namespace().String(), t.tv.WorkflowExecution())

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
		events = t.env.GetHistory(t.env.Namespace().String(), t.tv.WorkflowExecution())
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
	events = t.env.GetHistory(t.env.Namespace().String(), t.tv.WorkflowExecution())
	t.EqualHistoryEvents(expectedHistory, events)
}

func (s *ResetWorkflowTestSuite) TestBufferedSignalIsReappliedOnReset() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	s.testResetWorkflowSignalReapplyBuffer(env, tv, enumspb.RESET_REAPPLY_TYPE_SIGNAL)
}

func (s *ResetWorkflowTestSuite) TestBufferedSignalIsDroppedOnReset() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	s.testResetWorkflowSignalReapplyBuffer(env, tv, enumspb.RESET_REAPPLY_TYPE_NONE)
}

func (s *ResetWorkflowTestSuite) testResetWorkflowSignalReapplyBuffer(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	reapplyType enumspb.ResetReapplyType,
) {
	/*
		Test scenario:
		- while the worker is processing a WFT, a Signal and a Reset arrive
		- then, the worker responds with a CompleteWorkflowExecution command
		- depending on the reapply type, the buffered signal is applied post-reset or not
	*/

	runID := mustStartWorkflow(env, tv)
	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(runID))

	var resetRunID string
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if resetRunID == "" {
			s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted`, task.History.Events)

			// (1) send Signal
			_, err := env.FrontendClient().SignalWorkflowExecution(s.Context(),
				&workflowservice.SignalWorkflowExecutionRequest{
					RequestId:         tv.RequestID(),
					Namespace:         env.Namespace().String(),
					WorkflowExecution: tv.WorkflowExecution(),
					SignalName:        tv.Any().String(),
					Input:             tv.Any().Payloads(),
					Identity:          tv.WorkerIdentity(),
				})
			s.NoError(err)

			// (2) send Reset
			resp, err := env.FrontendClient().ResetWorkflowExecution(s.Context(),
				&workflowservice.ResetWorkflowExecutionRequest{
					Namespace:                 env.Namespace().String(),
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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.Error(err) // due to workflow termination (reset)

	_, err = poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	events := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: resetRunID})
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
	env := testcore.NewEnv(s.T())
	workflowID := "functional-reset-workflow-test-schedule"
	workflowTypeName := "functional-reset-workflow-test-schedule-type"
	taskQueueName := "functional-reset-workflow-test-schedule-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(env, workflowID, workflowTypeName, taskQueueName, 3)
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_WorkflowTask_ScheduleToStart() {
	env := testcore.NewEnv(s.T())
	workflowID := "functional-reset-workflow-test-schedule-to-start"
	workflowTypeName := "functional-reset-workflow-test-schedule-to-start-type"
	taskQueueName := "functional-reset-workflow-test-schedule-to-start-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(env, workflowID, workflowTypeName, taskQueueName, 4)
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_WorkflowTask_Start() {
	env := testcore.NewEnv(s.T())
	workflowID := "functional-reset-workflow-test-start"
	workflowTypeName := "functional-reset-workflow-test-start-type"
	taskQueueName := "functional-reset-workflow-test-start-taskqueue"
	s.testResetWorkflowRangeScheduleToStart(env, workflowID, workflowTypeName, taskQueueName, 5)
}

func (s *ResetWorkflowTestSuite) testResetWorkflowRangeScheduleToStart(
	env *testcore.TestEnv,
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
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)

	_, err = env.FrontendClient().SignalWorkflowExecution(s.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// events layout
	//  1. WorkflowExecutionStarted
	//  2. WorkflowTaskScheduled
	//  3. WorkflowExecutionSignaled
	//  4. WorkflowTaskStarted
	//  5. WorkflowTaskCompleted

	// Reset workflow execution
	_, err = env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
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
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)
}

func CaNOnceWorkflow(ctx workflow.Context, input string) (string, error) {
	if input != "don't CaN" {
		return input, workflow.NewContinueAsNewError(ctx, CaNOnceWorkflow, "don't CaN")
	}
	return input, nil
}

// CaNChainSignalWorkflow builds a three-run continue-as-new chain: run A (count 0) continues-as-new
// immediately, run B (count 1) waits for a "reapply-me" signal before continuing-as-new, and run C
// (count 2) completes. The signal recorded on the surviving intermediate run B is what a reset of
// run A must reapply once the current run has been deleted.
func CaNChainSignalWorkflow(ctx workflow.Context, count int) (string, error) {
	switch count {
	case 0:
		return "", workflow.NewContinueAsNewError(ctx, CaNChainSignalWorkflow, 1)
	case 1:
		var val string
		workflow.GetSignalChannel(ctx, "reapply-me").Receive(ctx, &val)
		return "", workflow.NewContinueAsNewError(ctx, CaNChainSignalWorkflow, 2)
	default:
		return "done", nil
	}
}

func (s *ResetWorkflowTestSuite) TestResetWorkflow_ResetAfterContinueAsNew() {
	env := testcore.NewEnv(s.T())

	env.SdkWorker().RegisterWorkflow(CaNOnceWorkflow)
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: env.WorkerTaskQueue()}, CaNOnceWorkflow, "")
	s.NoError(err)

	// wait for your workflow and its CaN to complete
	s.Eventually(func() bool {
		resp, err := env.FrontendClient().CountWorkflowExecutions(s.Context(), &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
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
	events := env.GetHistory(env.Namespace().String(), wfExec)
	var lastWorkflowTask *historypb.HistoryEvent
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			lastWorkflowTask = event
		}
	}

	// reset the original workflow
	_, err = env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         wfExec,
		WorkflowTaskFinishEventId: lastWorkflowTask.GetEventId(),
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)
}

// TestResetWorkflowByRunID_CurrentExecutionMissing resets by an explicit runId when the workflow has
// no current execution (the current run was deleted while older runs survive). It uses a three-run
// chain A -> B (waits for a signal) -> C, deletes the current run C, then resets by run A. This covers
// both the base->reset persistence when the current is missing and the reapply walking the surviving
// chain past the base: the signal recorded on the surviving intermediate run B must land on the reset
// run, and the walk must stop cleanly at the deleted run C.
func (s *ResetWorkflowTestSuite) TestResetWorkflowByRunID_CurrentExecutionMissing() {
	// Speed up the transfer/visibility queues so the async DeleteExecutionTask for the closed current
	// run is processed promptly. Mirrors the delete-execution suite (tests/workflow_delete_execution_test.go).
	env := testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.TransferProcessorUpdateAckInterval, 1*time.Second),
		testcore.WithDynamicConfig(dynamicconfig.VisibilityProcessorUpdateAckInterval, 1*time.Second),
	)

	// Run A -> run B (waits for signal) -> run C. Run A is the reset target; run C becomes current.
	env.SdkWorker().RegisterWorkflow(CaNChainSignalWorkflow)
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: env.WorkerTaskQueue()}, CaNChainSignalWorkflow, 0)
	s.NoError(err)
	baseRunID := run.GetRunID() // run A

	// Wait until run B is the running current execution, then signal it so the signal lands in B's history.
	s.AwaitTrue(func() bool {
		desc, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
		})
		if err != nil {
			return false
		}
		info := desc.GetWorkflowExecutionInfo()
		return info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && info.GetExecution().GetRunId() != baseRunID
	}, 30*time.Second, 100*time.Millisecond)
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "reapply-me", "payload"))

	// Wait for the whole chain (A, B, C) to stop running.
	s.AwaitTrue(func() bool {
		resp, err := env.FrontendClient().CountWorkflowExecutions(s.Context(), &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     fmt.Sprintf("WorkflowId = \"%s\" AND ExecutionStatus != \"Running\"", run.GetID()),
		})
		return err == nil && resp.GetCount() >= 3
	}, 30*time.Second, time.Second)

	// Delete the current run (C). This clears the current_executions record while run A and run B
	// survive — the "no current execution, older run exists" condition.
	desc, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
	})
	s.NoError(err)
	currentRunID := desc.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	s.NotEqual(baseRunID, currentRunID)
	_, err = env.FrontendClient().DeleteWorkflowExecution(s.Context(), &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: currentRunID},
	})
	s.NoError(err)

	// Wait until there is no current execution: resolving by workflowId only now returns NotFound.
	s.AwaitTrue(func() bool {
		_, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
		})
		var notFound *serviceerror.NotFound
		return errors.As(err, &notFound)
	}, 30*time.Second, 100*time.Millisecond)

	// Sanity: run A still exists and is addressable by its runId. Find its reset point (last completed
	// workflow task) while we are here.
	runAEvents := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: baseRunID})
	var lastWorkflowTask *historypb.HistoryEvent
	for _, event := range runAEvents {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			lastWorkflowTask = event
		}
	}
	s.NotNil(lastWorkflowTask)

	// Reset by run A's explicit runId. Before the fix this fails with NotFound (current missing);
	// after the fix it must succeed and create a new current execution from run A.
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: baseRunID},
		Reason:                    "reset by runId with current execution missing",
		WorkflowTaskFinishEventId: lastWorkflowTask.GetEventId(),
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)
	s.NotEmpty(resetResp.GetRunId())
	s.NotEqual(baseRunID, resetResp.GetRunId())

	// The reset run exists and is derived from run A.
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: resetResp.GetRunId()},
	})
	s.NoError(err)
	s.Equal(baseRunID, descResp.WorkflowExecutionInfo.GetFirstRunId())

	// The base run (A) records the base->reset link, so lineage navigation and child-completion
	// redirect can follow run A to the reset run. This is persisted even though A is not the
	// current execution (bypass-current write of the base run).
	baseDescResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: baseRunID},
	})
	s.NoError(err)
	s.Equal(resetResp.GetRunId(), baseDescResp.GetWorkflowExtendedInfo().GetResetRunId())

	// The workflow again has a current execution: resolving by workflowId only now succeeds.
	_, err = env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
	})
	s.NoError(err)

	// The signal recorded on the surviving intermediate run B must be reapplied onto the reset run.
	// Reapplying only run A's own events (the previous behavior) would drop it.
	resetEvents := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: resetResp.GetRunId()})
	signalReapplied := false
	for _, event := range resetEvents {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED &&
			event.GetWorkflowExecutionSignaledEventAttributes().GetSignalName() == "reapply-me" {
			signalReapplied = true
		}
	}
	s.True(signalReapplied, "signal from surviving intermediate run should be reapplied to the reset run")
}

func (s *ResetWorkflowTestSuite) TestResetWorkflowWithExternalPayloads() {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.ExternalPayloadsEnabled, true))

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

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               workflowInputPayload,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	})
	s.NoError(err0)
	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

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
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	// Process first workflow task to schedule activities
	_, err := poller.PollAndHandleWorkflowTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		cmds, err := wtHandler(task)
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: cmds}, err
	})
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Process one activity task which also creates second workflow task
	_, err = poller.PollAndHandleActivityTask(tv, func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		return &workflowservice.RespondActivityTaskCompletedRequest{Result: payloads.EncodeString("Activity Result")}, nil
	})
	env.Logger.Info("Poll and process first activity", tag.Error(err))
	s.NoError(err)

	// Process second workflow task which checks activity completion
	_, err = poller.PollAndHandleWorkflowTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		cmds, err := wtHandler(task)
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: cmds}, err
	})
	env.Logger.Info("Poll and process second workflow task", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)

	descResp, descErr := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.GetRunId(),
		},
	})
	s.NoError(descErr)
	s.Equal(int64(2), descResp.WorkflowExecutionInfo.ExternalPayloadCount)
	s.Equal(workflowExternalPayloadSize+activityExternalPayloadSize, descResp.WorkflowExecutionInfo.ExternalPayloadSizeBytes)

	// Get history to find reset point (first completed workflow task)
	events := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
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

	resetResp, err := env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: resetToEventID,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)
	env.Logger.Info("Workflow reset complete", tag.WorkflowRunID(resetResp.GetRunId()), tag.Int64("ResetToEventID", resetToEventID))

	descResp, descErr = env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
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
