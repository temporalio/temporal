package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SignalWorkflowTestSuite struct {
	testcore.FunctionalTestBase
}

func TestSignalWorkflowTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(SignalWorkflowTestSuite))
}

func (s *SignalWorkflowTestSuite) TestSignalWorkflow() {
	id := "functional-signal-workflow-test"
	wt := "functional-signal-workflow-test-type"
	tl := "functional-signal-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	// Send a signal to non-exist workflow
	header := commonpb.Header_builder{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
	}.Build()
	_, err0 := s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      uuid.NewString(),
		}.Build(),
		SignalName: "failed signal",
		Input:      nil,
		Identity:   identity,
		Header:     header,
	}.Build())
	s.NotNil(err0)
	s.IsType(&serviceerror.NotFound{}, err0)

	// Start workflow execution
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	// workflow logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	var signalEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}.Build(),
			}.Build()}, nil
		} else if task.GetPreviousStartedEventId() > 0 {
			for _, event := range task.GetHistory().GetEvents()[task.GetPreviousStartedEventId():] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*commandpb.Command{}, nil
				}
			}
		}

		workflowComplete = true
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
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

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send first signal using RunID
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build(),
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
		Header:     header,
	}.Build())
	s.NoError(err)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetSignalName())
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetInput())
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetIdentity())
	s.ProtoEqual(header, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetHeader())

	// Send another signal without RunID
	signalName = "another signal"
	signalInput = payloads.EncodeString("another signal input")
	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
		}.Build(),
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	}.Build())
	s.NoError(err)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetSignalName())
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetInput())
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetIdentity())

	// Terminate workflow execution
	_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
		}.Build(),
		Reason:   "test signal",
		Details:  nil,
		Identity: identity,
	}.Build())
	s.NoError(err)

	// Send signal to terminated workflow
	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build(),
		SignalName: "failed signal 1",
		Input:      nil,
		Identity:   identity,
	}.Build())
	s.NotNil(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *SignalWorkflowTestSuite) TestSignalWorkflow_DuplicateRequest() {
	id := "functional-signal-workflow-test-duplicate"
	wt := "functional-signal-workflow-test-duplicate-type"
	tl := "functional-signal-workflow-test-duplicate-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	// Start workflow execution
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	// workflow logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	var signalEvent *historypb.HistoryEvent
	numOfSignaledEvent := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}.Build(),
			}.Build()}, nil
		} else if task.GetPreviousStartedEventId() > 0 {
			numOfSignaledEvent = 0
			for _, event := range task.GetHistory().GetEvents()[task.GetPreviousStartedEventId():] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					numOfSignaledEvent++
				}
			}
			return []*commandpb.Command{}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
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

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send first signal
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	requestID := uuid.NewString()
	signalReqest := workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build(),
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
		RequestId:  requestID,
	}.Build()
	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), signalReqest)
	s.NoError(err)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetSignalName())
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetInput())
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetIdentity())
	s.Equal(1, numOfSignaledEvent)

	// Send another signal with same request id
	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), signalReqest)
	s.NoError(err)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(0, numOfSignaledEvent)
}

func (s *SignalWorkflowTestSuite) TestSignalExternalWorkflowCommand() {
	s.OverrideDynamicConfig(dynamicconfig.EnableCrossNamespaceCommands, true) // explicitly enable cross namespace commands for this test
	id := "functional-signal-external-workflow-test"
	wt := "functional-signal-external-workflow-test-type"
	tl := "functional-signal-external-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	externalRequest := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.ExternalNamespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()
	we2, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), externalRequest)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution on external Namespace", tag.WorkflowNamespace(s.ExternalNamespace().String()), tag.WorkflowRunID(we2.GetRunId()))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	signalHeader := commonpb.Header_builder{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
	}.Build()
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}.Build(),
			}.Build()}, nil
		}

		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			SignalExternalWorkflowExecutionCommandAttributes: commandpb.SignalExternalWorkflowExecutionCommandAttributes_builder{
				Namespace: s.ExternalNamespace().String(),
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: id,
					RunId:      we2.GetRunId(),
				}.Build(),
				SignalName: signalName,
				Input:      signalInput,
				Header:     signalHeader,
			}.Build(),
		}.Build()}, nil
	}

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

	workflowComplete := false
	externalActivityCount := int32(1)
	externalActivityCounter := int32(0)
	var signalEvent *historypb.HistoryEvent
	externalWFTHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if externalActivityCounter < externalActivityCount {
			externalActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, externalActivityCounter))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             convert.Int32ToString(externalActivityCounter),
					ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}.Build(),
			}.Build()}, nil
		} else if task.GetPreviousStartedEventId() > 0 {
			for _, event := range task.GetHistory().GetEvents()[task.GetPreviousStartedEventId():] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*commandpb.Command{}, nil
				}
			}
		}

		workflowComplete = true
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	//nolint:staticcheck // SA1019 TaskPoller replacement needs to be done holistically.
	externalPoller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.ExternalNamespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: externalWFTHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Start both current and external workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = externalPoller.PollAndProcessWorkflowTask()
	s.Logger.Info("external PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = externalPoller.PollAndProcessActivityTask(false)
	s.Logger.Info("external PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// Signal the external workflow with this command.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// in source workflow
	var historyEvents []*historypb.HistoryEvent
CheckHistoryLoopForSignalSent:
	for i := 1; i < 10; i++ {
		historyEvents = s.GetHistory(s.Namespace().String(), commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build())

		signalRequestedEvent := historyEvents[len(historyEvents)-2]
		if signalRequestedEvent.GetEventType() != enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED {
			s.Logger.Info("Signal still not sent")
			time.Sleep(100 * time.Millisecond) //nolint:forbidigo
			continue CheckHistoryLoopForSignalSent
		}
		break
	}

	s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskTimedOut
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 SignalExternalWorkflowExecutionInitiated
 11 ExternalWorkflowExecutionSignaled {"InitiatedEventId":10,"WorkflowExecution":{"RunId":"%s","WorkflowId":"%s"}}
 12 WorkflowTaskScheduled`, we2.GetRunId(), id), historyEvents)

	// Process signal in workflow for external workflow
	_, err = externalPoller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetSignalName())
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetInput())
	s.ProtoEqual(signalHeader, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetHeader())
	s.Equal("history-service", signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetIdentity())
}

func (s *SignalWorkflowTestSuite) TestSignalWorkflow_Cron_NoWorkflowTaskCreated() {
	id := "functional-signal-workflow-test-cron"
	wt := "functional-signal-workflow-test-cron-type"
	tl := "functional-signal-workflow-test-cron-taskqueue"
	identity := "worker1"
	cronSpec := "@every 2s"

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	// Start workflow execution
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		CronSchedule:        cronSpec,
	}.Build()
	now := time.Now().UTC()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	// Send first signal using RunID
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err := s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build(),
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	}.Build())
	s.NoError(err)

	// workflow logic
	var workflowTaskDelay time.Duration
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		workflowTaskDelay = time.Now().UTC().Sub(now)

		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Make first command to schedule activity
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowTaskDelay > time.Second*2)
}

func (s *SignalWorkflowTestSuite) TestSignalWorkflow_WorkflowCloseAttempted() {
	id := "functional-signal-workflow-workflow-close-attempted-test"
	wt := "functional-signal-workflow-workflow-close-attempted-test-type"
	tl := "functional-signal-workflow-workflow-close-attempted-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(3 * time.Second),
		Identity:            identity,
	}.Build())
	s.NoError(err)

	attemptCount := 1
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if attemptCount == 1 {
			_, err := s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
				Namespace: s.Namespace().String(),
				WorkflowExecution: commonpb.WorkflowExecution_builder{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				}.Build(),
				SignalName: "buffered-signal",
				Identity:   identity,
				RequestId:  uuid.NewString(),
			}.Build())
			s.NoError(err)
		}

		if attemptCount == 2 {
			ctx, _ := rpc.NewContextWithTimeoutAndVersionHeaders(time.Second)
			_, err := s.FrontendClient().SignalWorkflowExecution(ctx, workflowservice.SignalWorkflowExecutionRequest_builder{
				Namespace: s.Namespace().String(),
				WorkflowExecution: commonpb.WorkflowExecution_builder{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				}.Build(),
				SignalName: "rejected-signal",
				Identity:   identity,
				RequestId:  uuid.NewString(),
			}.Build())
			s.Error(err)
			s.Error(consts.ErrWorkflowClosing, err)
		}

		attemptCount++
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.Error(err)

	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
}

func (s *SignalWorkflowTestSuite) TestSignalExternalWorkflowCommand_WithoutRunID() {
	s.OverrideDynamicConfig(dynamicconfig.EnableCrossNamespaceCommands, true) // explicitly enable cross namespace commands for this test
	id := "functional-signal-external-workflow-test-without-run-id"
	wt := "functional-signal-external-workflow-test-without-run-id-type"
	tl := "functional-signal-external-workflow-test-without-run-id-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	externalRequest := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.ExternalNamespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()
	we2, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), externalRequest)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution on external Namespace", tag.WorkflowNamespace(s.ExternalNamespace().String()), tag.WorkflowRunID(we2.GetRunId()))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}.Build(),
			}.Build()}, nil
		}

		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			SignalExternalWorkflowExecutionCommandAttributes: commandpb.SignalExternalWorkflowExecutionCommandAttributes_builder{
				Namespace: s.ExternalNamespace().String(),
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: id,
					// No RunID in command
				}.Build(),
				SignalName: signalName,
				Input:      signalInput,
			}.Build(),
		}.Build()}, nil
	}

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

	workflowComplete := false
	externalActivityCount := int32(1)
	externalActivityCounter := int32(0)
	var signalEvent *historypb.HistoryEvent
	externalWFTHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if externalActivityCounter < externalActivityCount {
			externalActivityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, externalActivityCounter))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             convert.Int32ToString(externalActivityCounter),
					ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}.Build(),
			}.Build()}, nil
		} else if task.GetPreviousStartedEventId() > 0 {
			for _, event := range task.GetHistory().GetEvents()[task.GetPreviousStartedEventId():] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*commandpb.Command{}, nil
				}
			}
		}

		workflowComplete = true
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	//nolint:staticcheck // SA1019 TaskPoller replacement needs to be done holistically.
	externalPoller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.ExternalNamespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: externalWFTHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Start both current and external workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = externalPoller.PollAndProcessWorkflowTask()
	s.Logger.Info("external PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = externalPoller.PollAndProcessActivityTask(false)
	s.Logger.Info("external PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// Signal the external workflow with this command.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// in source workflow
	var historyEvents []*historypb.HistoryEvent
CheckHistoryLoopForSignalSent:
	for i := 1; i < 10; i++ {
		historyEvents = s.GetHistory(s.Namespace().String(), commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build())

		signalRequestedEvent := historyEvents[len(historyEvents)-2]
		if signalRequestedEvent.GetEventType() != enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED {
			s.Logger.Info("Signal still not sent")
			time.Sleep(100 * time.Millisecond) //nolint:forbidigo
			continue CheckHistoryLoopForSignalSent
		}

		break
	}

	s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskTimedOut
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 SignalExternalWorkflowExecutionInitiated
 11 ExternalWorkflowExecutionSignaled {"InitiatedEventId":10,"WorkflowExecution":{"RunId":"","WorkflowId":"%s"}}
 12 WorkflowTaskScheduled`, id), historyEvents)

	// Process signal in workflow for external workflow
	_, err = externalPoller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetSignalName())
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetInput())
	s.Equal("history-service", signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetIdentity())
}

func (s *SignalWorkflowTestSuite) TestSignalExternalWorkflowCommand_UnKnownTarget() {
	s.OverrideDynamicConfig(dynamicconfig.EnableCrossNamespaceCommands, true) // explicitly enable cross namespace commands for this test
	id := "functional-signal-unknown-workflow-command-test"
	wt := "functional-signal-unknown-workflow-command-test-type"
	tl := "functional-signal-unknown-workflow-command-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()
	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}.Build(),
			}.Build()}, nil
		}

		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			SignalExternalWorkflowExecutionCommandAttributes: commandpb.SignalExternalWorkflowExecutionCommandAttributes_builder{
				Namespace: s.ExternalNamespace().String(),
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: "workflow_not_exist",
					RunId:      we.GetRunId(),
				}.Build(),
				SignalName: signalName,
				Input:      signalInput,
			}.Build(),
		}.Build()}, nil
	}

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

	// Start workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Signal the external workflow with this command.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	var historyEvents []*historypb.HistoryEvent
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyEvents = s.GetHistory(s.Namespace().String(), commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build())

		signalFailedEvent := historyEvents[len(historyEvents)-2]
		if signalFailedEvent.GetEventType() != enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED {
			s.Logger.Info("Cancellaton not cancelled yet")
			time.Sleep(100 * time.Millisecond) //nolint:forbidigo
			continue CheckHistoryLoopForCancelSent
		}
		break
	}

	s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskTimedOut
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 SignalExternalWorkflowExecutionInitiated
 11 SignalExternalWorkflowExecutionFailed {"InitiatedEventId":10,"WorkflowExecution":{"RunId":"%s","WorkflowId":"workflow_not_exist"}}
 12 WorkflowTaskScheduled`, we.GetRunId()), historyEvents)
}

func (s *SignalWorkflowTestSuite) TestSignalExternalWorkflowCommand_SignalSelf() {
	id := "functional-signal-self-workflow-command-test"
	wt := "functional-signal-self-workflow-command-test-type"
	tl := "functional-signal-self-workflow-command-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()
	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}.Build(),
			}.Build()}, nil
		}

		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			SignalExternalWorkflowExecutionCommandAttributes: commandpb.SignalExternalWorkflowExecutionCommandAttributes_builder{
				Namespace: s.Namespace().String(),
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				}.Build(),
				SignalName: signalName,
				Input:      signalInput,
			}.Build(),
		}.Build()}, nil
	}

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

	// Start workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Signal the external workflow with this command.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	var historyEvents []*historypb.HistoryEvent
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyEvents = s.GetHistory(s.Namespace().String(), commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build())

		signalFailedEvent := historyEvents[len(historyEvents)-2]
		if signalFailedEvent.GetEventType() != enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED {
			s.Logger.Info("Cancellaton not cancelled yet")
			time.Sleep(100 * time.Millisecond) //nolint:forbidigo
			continue CheckHistoryLoopForCancelSent
		}

		break
	}
	s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskTimedOut
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 SignalExternalWorkflowExecutionInitiated
 11 SignalExternalWorkflowExecutionFailed {"InitiatedEventId":10,"WorkflowExecution":{"RunId":"%s","WorkflowId":"%s"}}
 12 WorkflowTaskScheduled`, we.GetRunId(), id), historyEvents)
}

func (s *SignalWorkflowTestSuite) TestSignalWithStartWorkflow() {
	id := "functional-signal-with-start-workflow-test"
	wt := "functional-signal-with-start-workflow-test-type"
	tl := "functional-signal-with-start-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	header := commonpb.Header_builder{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("sample data")},
	}.Build()

	// Start a workflow
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	// workflow logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	newWorkflowStarted := false
	var signalEvent, startedEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}.Build(),
			}.Build()}, nil
		} else if task.GetPreviousStartedEventId() > 0 {
			for _, event := range task.GetHistory().GetEvents()[task.GetPreviousStartedEventId():] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*commandpb.Command{}, nil
				}
			}
		} else if newWorkflowStarted {
			newWorkflowStarted = false
			signalEvent = nil
			startedEvent = nil
			for _, event := range task.GetHistory().GetEvents()[task.GetPreviousStartedEventId():] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
				}
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
					startedEvent = event
				}
			}
			if signalEvent != nil && startedEvent != nil {
				return []*commandpb.Command{}, nil
			}
		}

		workflowComplete = true
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
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

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send a signal
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	wfIDReusePolicy := enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	sRequest := workflowservice.SignalWithStartWorkflowExecutionRequest_builder{
		RequestId:             uuid.NewString(),
		Namespace:             s.Namespace().String(),
		WorkflowId:            id,
		WorkflowType:          workflowType,
		TaskQueue:             taskQueue,
		Input:                 nil,
		Header:                header,
		WorkflowRunTimeout:    durationpb.New(100 * time.Second),
		WorkflowTaskTimeout:   durationpb.New(1 * time.Second),
		SignalName:            signalName,
		SignalInput:           signalInput,
		Identity:              identity,
		WorkflowIdReusePolicy: wfIDReusePolicy,
	}.Build()
	resp, err := s.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), sRequest)
	s.NoError(err)
	s.False(resp.GetStarted())
	s.Equal(we.GetRunId(), resp.GetRunId())

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetSignalName())
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetInput())
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetIdentity())

	// Terminate workflow execution
	_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
		}.Build(),
		Reason:   "test signal",
		Details:  nil,
		Identity: identity,
	}.Build())
	s.NoError(err)

	// Send signal to terminated workflow
	signalName = "signal to terminate"
	signalInput = payloads.EncodeString("signal to terminate input")
	sRequest.SetSignalName(signalName)
	sRequest.SetSignalInput(signalInput)
	sRequest.SetWorkflowId(id)

	resp, err = s.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), sRequest)
	s.NoError(err)
	s.True(resp.GetStarted())
	s.NotNil(resp.GetRunId())
	s.NotEqual(we.GetRunId(), resp.GetRunId())
	newWorkflowStarted = true

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetSignalName())
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetInput())
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetIdentity())
	s.True(startedEvent != nil)
	s.ProtoEqual(header, startedEvent.GetWorkflowExecutionStartedEventAttributes().GetHeader())

	// Send signal to not existed workflow
	id = "functional-signal-with-start-workflow-test-non-exist"
	signalName = "signal to non exist"
	signalInput = payloads.EncodeString("signal to non exist input")
	sRequest.SetSignalName(signalName)
	sRequest.SetSignalInput(signalInput)
	sRequest.SetWorkflowId(id)
	resp, err = s.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), sRequest)
	s.NoError(err)
	s.NotNil(resp.GetRunId())
	s.True(resp.GetStarted())
	newWorkflowStarted = true

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.True(signalEvent != nil)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetSignalName())
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetInput())
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetIdentity())

	listOpenRequest := workflowservice.ListOpenWorkflowExecutionsRequest_builder{
		Namespace:       s.Namespace().String(),
		MaximumPageSize: 100,
		StartTimeFilter: filterpb.StartTimeFilter_builder{
			EarliestTime: nil,
			LatestTime:   timestamppb.New(time.Now().UTC()),
		}.Build(),
		ExecutionFilter: filterpb.WorkflowExecutionFilter_builder{
			WorkflowId: id,
		}.Build(),
	}.Build()

	// Assert visibility is correct
	s.Eventually(
		func() bool {
			listResp, err := s.FrontendClient().ListOpenWorkflowExecutions(testcore.NewContext(), listOpenRequest)
			s.NoError(err)
			return len(listResp.GetExecutions()) == 1
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	// Terminate workflow execution and assert visibility is correct
	_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
		}.Build(),
		Reason:   "kill workflow",
		Details:  nil,
		Identity: identity,
	}.Build())
	s.NoError(err)

	s.Eventually(
		func() bool {
			listResp, err := s.FrontendClient().ListOpenWorkflowExecutions(testcore.NewContext(), listOpenRequest)
			s.NoError(err)
			return len(listResp.GetExecutions()) == 0
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	listClosedRequest := workflowservice.ListClosedWorkflowExecutionsRequest_builder{
		Namespace:       s.Namespace().String(),
		MaximumPageSize: 100,
		StartTimeFilter: filterpb.StartTimeFilter_builder{
			EarliestTime: nil,
			LatestTime:   timestamppb.New(time.Now().UTC()),
		}.Build(),
		ExecutionFilter: filterpb.WorkflowExecutionFilter_builder{
			WorkflowId: id,
		}.Build(),
	}.Build()
	listClosedResp, err := s.FrontendClient().ListClosedWorkflowExecutions(testcore.NewContext(), listClosedRequest)
	s.NoError(err)
	s.Equal(1, len(listClosedResp.GetExecutions()))
}

func (s *SignalWorkflowTestSuite) TestSignalWithStartWorkflow_ResolveIDDeduplication() {

	// setting this to 0 to be sure we are terminating the current workflow
	s.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, 0)

	id := "functional-signal-with-start-workflow-id-reuse-test"
	wt := "functional-signal-with-start-workflow-id-reuse-test-type"
	tl := "functional-signal-with-start-workflow-id-reuse-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()

	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()

	// Start a workflow
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}.Build(),
			}.Build()}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

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

	// Start workflows, make some progress and complete workflow
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)

	// test WorkflowIdReusePolicy: RejectDuplicate
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	sRequest := workflowservice.SignalWithStartWorkflowExecutionRequest_builder{
		RequestId:             uuid.NewString(),
		Namespace:             s.Namespace().String(),
		WorkflowId:            id,
		WorkflowType:          workflowType,
		TaskQueue:             taskQueue,
		Input:                 nil,
		WorkflowRunTimeout:    durationpb.New(100 * time.Second),
		WorkflowTaskTimeout:   durationpb.New(1 * time.Second),
		SignalName:            signalName,
		SignalInput:           signalInput,
		Identity:              identity,
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}.Build()
	ctx, _ := rpc.NewContextWithTimeoutAndVersionHeaders(5 * time.Second)
	resp, err := s.FrontendClient().SignalWithStartWorkflowExecution(ctx, sRequest)
	s.Nil(resp)
	s.Error(err)
	s.True(strings.Contains(err.Error(), "reject duplicate workflow Id"))
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)

	// test WorkflowIdReusePolicy: AllowDuplicateFailedOnly
	sRequest.SetWorkflowIdReusePolicy(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY)
	ctx, _ = rpc.NewContextWithTimeoutAndVersionHeaders(5 * time.Second)
	resp, err = s.FrontendClient().SignalWithStartWorkflowExecution(ctx, sRequest)
	s.Nil(resp)
	s.Error(err)
	s.True(strings.Contains(err.Error(), "allow duplicate workflow Id if last run failed"))
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)

	// test WorkflowIdReusePolicy: AllowDuplicate
	sRequest.SetWorkflowIdReusePolicy(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
	ctx, _ = rpc.NewContextWithTimeoutAndVersionHeaders(5 * time.Second)
	resp, err = s.FrontendClient().SignalWithStartWorkflowExecution(ctx, sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
	s.True(resp.GetStarted())

	// Terminate workflow execution
	_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
		}.Build(),
		Reason:   "test WorkflowIdReusePolicyAllowDuplicateFailedOnly",
		Details:  nil,
		Identity: identity,
	}.Build())
	s.NoError(err)

	// test WorkflowIdReusePolicy: AllowDuplicateFailedOnly
	sRequest.SetWorkflowIdReusePolicy(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY)
	resp, err = s.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
	s.True(resp.GetStarted())

	// test WorkflowIdReusePolicy: TerminateIfRunning (for backwards compatibility)
	prevRunID := resp.GetRunId()
	sRequest.SetWorkflowIdReusePolicy(enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING)
	resp, err = s.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
	s.NotEqual(prevRunID, resp.GetRunId())
	s.True(resp.GetStarted())

	descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), workflowservice.DescribeWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		Execution: commonpb.WorkflowExecution_builder{WorkflowId: id, RunId: prevRunID}.Build(),
	}.Build())
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.GetWorkflowExecutionInfo().GetStatus())

	// test WorkflowIdConflictPolicy: TerminateExisting (replaced TerminateIfRunning)
	prevRunID = resp.GetRunId()
	sRequest.SetWorkflowIdReusePolicy(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
	sRequest.SetWorkflowIdConflictPolicy(enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING)
	resp, err = s.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
	s.NotEqual(prevRunID, resp.GetRunId())
	s.True(resp.GetStarted())

	descResp, err = s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), workflowservice.DescribeWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		Execution: commonpb.WorkflowExecution_builder{WorkflowId: id, RunId: prevRunID}.Build(),
	}.Build())
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.GetWorkflowExecutionInfo().GetStatus())

	descResp, err = s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), workflowservice.DescribeWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		Execution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      resp.GetRunId(),
		}.Build(),
	}.Build())
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, descResp.GetWorkflowExecutionInfo().GetStatus())
}

func (s *SignalWorkflowTestSuite) TestSignalWithStartWorkflow_StartDelay() {
	id := "functional-signal-with-start-workflow-start-delay-test"
	wt := "functional-signal-with-start-workflow-start-delay-test-type"
	tl := "functional-signal-with-start-workflow-start-delay-test-taskqueue"
	stickyTq := "functional-signal-with-start-workflow-start-delay-test-sticky-taskqueue"
	identity := "worker1"

	startDelay := 3 * time.Second

	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")

	sRequest := workflowservice.SignalWithStartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        commonpb.WorkflowType_builder{Name: wt}.Build(),
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		SignalName:          signalName,
		SignalInput:         signalInput,
		Identity:            identity,
		WorkflowStartDelay:  durationpb.New(startDelay),
	}.Build()

	reqStartTime := time.Now()
	we0, startErr := s.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), sRequest)
	s.NoError(startErr)

	var signalEvent *historypb.HistoryEvent
	delayEndTime := time.Now()

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		delayEndTime = time.Now()

		for _, event := range task.GetHistory().GetEvents()[task.GetPreviousStartedEventId():] {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signalEvent = event
			}
		}

		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		StickyTaskQueue:     taskqueuepb.TaskQueue_builder{Name: stickyTq, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl}.Build(),
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, pollErr := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(pollErr)
	s.GreaterOrEqual(delayEndTime.Sub(reqStartTime), startDelay)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetSignalName())
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetInput())
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().GetIdentity())

	descResp, descErr := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), workflowservice.DescribeWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		Execution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we0.GetRunId(),
		}.Build(),
	}.Build())
	s.NoError(descErr)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.GetWorkflowExecutionInfo().GetStatus())
}
