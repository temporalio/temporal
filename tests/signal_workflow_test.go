package tests

import (
	"bytes"
	"encoding/binary"
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
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SignalWorkflowTestSuite struct {
	parallelsuite.Suite[*SignalWorkflowTestSuite]
}

func TestSignalWorkflowTestSuite(t *testing.T) {
	parallelsuite.Run(t, &SignalWorkflowTestSuite{})
}

func (s *SignalWorkflowTestSuite) TestSignalWorkflow() {
	env := testcore.NewEnv(s.T())
	id := "functional-signal-workflow-test"
	wt := "functional-signal-workflow-test-type"
	tl := "functional-signal-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Send a signal to non-exist workflow
	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
	}
	_, err0 := env.FrontendClient().SignalWorkflowExecution(env.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      uuid.NewString(),
		},
		SignalName: "failed signal",
		Input:      nil,
		Identity:   identity,
		Header:     header,
	})
	s.Error(err0)
	s.IsType(&serviceerror.NotFound{}, err0)

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

	we, err0 := env.FrontendClient().StartWorkflowExecution(env.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	workflowComplete := false
	activityScheduled := false
	activityData := int32(1)
	var signalEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*commandpb.Command{}, nil
				}
			}
		}

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

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send first signal using RunID
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = env.FrontendClient().SignalWorkflowExecution(env.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
		Header:     header,
	})
	s.NoError(err)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
	s.ProtoEqual(header, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Header)

	// Send another signal without RunID
	signalName = "another signal"
	signalInput = payloads.EncodeString("another signal input")
	_, err = env.FrontendClient().SignalWorkflowExecution(env.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	// Terminate workflow execution
	_, err = env.FrontendClient().TerminateWorkflowExecution(env.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "test signal",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	// Send signal to terminated workflow
	_, err = env.FrontendClient().SignalWorkflowExecution(env.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: "failed signal 1",
		Input:      nil,
		Identity:   identity,
	})
	s.Error(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *SignalWorkflowTestSuite) TestSignalWorkflow_DuplicateRequest() {
	env := testcore.NewEnv(s.T())
	id := "functional-signal-workflow-test-duplicate"
	wt := "functional-signal-workflow-test-duplicate-type"
	tl := "functional-signal-workflow-test-duplicate-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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

	we, err0 := env.FrontendClient().StartWorkflowExecution(env.Context(), request)
	s.NoError(err0)
	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

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
			s.NoError(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			numOfSignaledEvent = 0
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					numOfSignaledEvent++
				}
			}
			return []*commandpb.Command{}, nil
		}

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

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send first signal
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	requestID := uuid.NewString()
	signalReqest := &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
		RequestId:  requestID,
	}
	_, err = env.FrontendClient().SignalWorkflowExecution(env.Context(), signalReqest)
	s.NoError(err)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
	s.Equal(1, numOfSignaledEvent)

	// Send another signal with same request id
	_, err = env.FrontendClient().SignalWorkflowExecution(env.Context(), signalReqest)
	s.NoError(err)

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.NotNil(signalEvent)
	s.Equal(0, numOfSignaledEvent)
}

func (s *SignalWorkflowTestSuite) TestSignalExternalWorkflowCommand() {
	env := testcore.NewEnv(s.T(), testcore.WithDedicatedCluster())
	env.OverrideDynamicConfig(dynamicconfig.EnableCrossNamespaceCommands, true) // explicitly enable cross namespace commands for this test
	id := "functional-signal-external-workflow-test"
	wt := "functional-signal-external-workflow-test-type"
	tl := "functional-signal-external-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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

	we, err0 := env.FrontendClient().StartWorkflowExecution(env.Context(), request)
	s.NoError(err0)
	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	externalRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.ExternalNamespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we2, err0 := env.FrontendClient().StartWorkflowExecution(env.Context(), externalRequest)
	s.NoError(err0)
	env.Logger.Info("StartWorkflowExecution on external Namespace", tag.WorkflowNamespace(env.ExternalNamespace().String()), tag.WorkflowRunID(we2.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	signalHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"signal header key": payload.EncodeString("signal header value")},
	}
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
				Namespace: env.ExternalNamespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we2.GetRunId(),
				},
				SignalName: signalName,
				Input:      signalInput,
				Header:     signalHeader,
			}},
		}}, nil
	}

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

	workflowComplete := false
	externalActivityCount := int32(1)
	externalActivityCounter := int32(0)
	var signalEvent *historypb.HistoryEvent
	externalWFTHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if externalActivityCounter < externalActivityCount {
			externalActivityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, externalActivityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(externalActivityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*commandpb.Command{}, nil
				}
			}
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	//nolint:staticcheck // SA1019 TaskPoller replacement needs to be done holistically.
	externalPoller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.ExternalNamespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: externalWFTHandler,
		ActivityTaskHandler: atHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	// Start both current and external workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = externalPoller.PollAndProcessWorkflowTask()
	env.Logger.Info("external PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = externalPoller.PollAndProcessActivityTask(false)
	env.Logger.Info("external PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// Signal the external workflow with this command.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// in source workflow
	var historyEvents []*historypb.HistoryEvent
CheckHistoryLoopForSignalSent:
	for i := 1; i < 10; i++ {
		historyEvents = env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		})

		signalRequestedEvent := historyEvents[len(historyEvents)-2]
		if signalRequestedEvent.GetEventType() != enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED {
			env.Logger.Info("Signal still not sent")
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
 12 WorkflowTaskScheduled`, we2.RunId, id), historyEvents)

	// Process signal in workflow for external workflow
	_, err = externalPoller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.ProtoEqual(signalHeader, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Header)
	s.Equal("history-service", signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
}

func (s *SignalWorkflowTestSuite) TestSignalWorkflow_Cron_NoWorkflowTaskCreated() {
	env := testcore.NewEnv(s.T())
	id := "functional-signal-workflow-test-cron"
	wt := "functional-signal-workflow-test-cron-type"
	tl := "functional-signal-workflow-test-cron-taskqueue"
	identity := "worker1"
	cronSpec := "@every 2s"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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
		CronSchedule:        cronSpec,
	}
	now := time.Now().UTC()

	we, err0 := env.FrontendClient().StartWorkflowExecution(env.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// Send first signal using RunID
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err := env.FrontendClient().SignalWorkflowExecution(env.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	// workflow logic
	var workflowTaskDelay time.Duration
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		workflowTaskDelay = time.Now().UTC().Sub(now)

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
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
		Logger:              env.Logger,
		T:                   s.T(),
	}

	// Make first command to schedule activity
	_, err = poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.Greater(workflowTaskDelay, time.Second*2)
}

func (s *SignalWorkflowTestSuite) TestSignalWorkflow_WorkflowCloseAttempted() {
	env := testcore.NewEnv(s.T())
	id := "functional-signal-workflow-workflow-close-attempted-test"
	wt := "functional-signal-workflow-workflow-close-attempted-test-type"
	tl := "functional-signal-workflow-workflow-close-attempted-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	we, err := env.FrontendClient().StartWorkflowExecution(env.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(3 * time.Second),
		Identity:            identity,
	})
	s.NoError(err)

	attemptCount := 1
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if attemptCount == 1 {
			_, err := env.FrontendClient().SignalWorkflowExecution(env.Context(), &workflowservice.SignalWorkflowExecutionRequest{
				Namespace: env.Namespace().String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.RunId,
				},
				SignalName: "buffered-signal",
				Identity:   identity,
				RequestId:  uuid.NewString(),
			})
			s.NoError(err)
		}

		if attemptCount == 2 {
			ctx, _ := rpc.NewContextWithTimeoutAndVersionHeaders(time.Second)
			_, err := env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
				Namespace: env.Namespace().String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.RunId,
				},
				SignalName: "rejected-signal",
				Identity:   identity,
				RequestId:  uuid.NewString(),
			})
			var resourceExhausted *serviceerror.ResourceExhausted
			s.ErrorAs(err, &resourceExhausted)
			s.Equal(consts.ErrWorkflowClosing.Cause, resourceExhausted.Cause)
			s.Equal(consts.ErrWorkflowClosing.Message, resourceExhausted.Message)
		}

		attemptCount++
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
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
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.Error(err)

	_, err = poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
}

func (s *SignalWorkflowTestSuite) TestSignalExternalWorkflowCommand_WithoutRunID() {
	env := testcore.NewEnv(s.T(), testcore.WithDedicatedCluster())
	env.OverrideDynamicConfig(dynamicconfig.EnableCrossNamespaceCommands, true) // explicitly enable cross namespace commands for this test
	id := "functional-signal-external-workflow-test-without-run-id"
	wt := "functional-signal-external-workflow-test-without-run-id-type"
	tl := "functional-signal-external-workflow-test-without-run-id-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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

	we, err0 := env.FrontendClient().StartWorkflowExecution(env.Context(), request)
	s.NoError(err0)
	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	externalRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.ExternalNamespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we2, err0 := env.FrontendClient().StartWorkflowExecution(env.Context(), externalRequest)
	s.NoError(err0)
	env.Logger.Info("StartWorkflowExecution on external Namespace", tag.WorkflowNamespace(env.ExternalNamespace().String()), tag.WorkflowRunID(we2.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
				Namespace: env.ExternalNamespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					// No RunID in command
				},
				SignalName: signalName,
				Input:      signalInput,
			}},
		}}, nil
	}

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

	workflowComplete := false
	externalActivityCount := int32(1)
	externalActivityCounter := int32(0)
	var signalEvent *historypb.HistoryEvent
	externalWFTHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if externalActivityCounter < externalActivityCount {
			externalActivityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, externalActivityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(externalActivityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*commandpb.Command{}, nil
				}
			}
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	//nolint:staticcheck // SA1019 TaskPoller replacement needs to be done holistically.
	externalPoller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.ExternalNamespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: externalWFTHandler,
		ActivityTaskHandler: atHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	// Start both current and external workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	_, err = externalPoller.PollAndProcessWorkflowTask()
	env.Logger.Info("external PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = externalPoller.PollAndProcessActivityTask(false)
	env.Logger.Info("external PollAndProcessActivityTask", tag.Error(err))
	s.NoError(err)

	// Signal the external workflow with this command.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// in source workflow
	var historyEvents []*historypb.HistoryEvent
CheckHistoryLoopForSignalSent:
	for i := 1; i < 10; i++ {
		historyEvents = env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		})

		signalRequestedEvent := historyEvents[len(historyEvents)-2]
		if signalRequestedEvent.GetEventType() != enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED {
			env.Logger.Info("Signal still not sent")
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
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal("history-service", signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
}

func (s *SignalWorkflowTestSuite) TestSignalExternalWorkflowCommand_UnKnownTarget() {
	env := testcore.NewEnv(s.T(), testcore.WithDedicatedCluster())
	env.OverrideDynamicConfig(dynamicconfig.EnableCrossNamespaceCommands, true) // explicitly enable cross namespace commands for this test
	id := "functional-signal-unknown-workflow-command-test"
	wt := "functional-signal-unknown-workflow-command-test-type"
	tl := "functional-signal-unknown-workflow-command-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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
	we, err0 := env.FrontendClient().StartWorkflowExecution(env.Context(), request)
	s.NoError(err0)
	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
				Namespace: env.ExternalNamespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow_not_exist",
					RunId:      we.GetRunId(),
				},
				SignalName: signalName,
				Input:      signalInput,
			}},
		}}, nil
	}

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

	// Start workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Signal the external workflow with this command.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	var historyEvents []*historypb.HistoryEvent
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyEvents = env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		})

		signalFailedEvent := historyEvents[len(historyEvents)-2]
		if signalFailedEvent.GetEventType() != enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED {
			env.Logger.Info("Cancellaton not cancelled yet")
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
 12 WorkflowTaskScheduled`, we.RunId), historyEvents)
}

func (s *SignalWorkflowTestSuite) TestSignalExternalWorkflowCommand_SignalSelf() {
	env := testcore.NewEnv(s.T())
	id := "functional-signal-self-workflow-command-test"
	wt := "functional-signal-self-workflow-command-test-type"
	tl := "functional-signal-self-workflow-command-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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
	we, err0 := env.FrontendClient().StartWorkflowExecution(env.Context(), request)
	s.NoError(err0)
	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	activityCount := int32(1)
	activityCounter := int32(0)
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
				Namespace: env.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				},
				SignalName: signalName,
				Input:      signalInput,
			}},
		}}, nil
	}

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

	// Start workflows to make some progress.
	_, err := poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Signal the external workflow with this command.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	var historyEvents []*historypb.HistoryEvent
CheckHistoryLoopForCancelSent:
	for i := 1; i < 10; i++ {
		historyEvents = env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		})

		signalFailedEvent := historyEvents[len(historyEvents)-2]
		if signalFailedEvent.GetEventType() != enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED {
			env.Logger.Info("Cancellaton not cancelled yet")
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
 12 WorkflowTaskScheduled`, we.RunId, id), historyEvents)
}

func (s *SignalWorkflowTestSuite) TestSignalWithStartWorkflow() {
	env := testcore.NewEnv(s.T())
	id := "functional-signal-with-start-workflow-test"
	wt := "functional-signal-with-start-workflow-test-type"
	tl := "functional-signal-with-start-workflow-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("sample data")},
	}

	// Start a workflow
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

	we, err0 := env.FrontendClient().StartWorkflowExecution(env.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

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
			s.NoError(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             strconv.Itoa(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					signalEvent = event
					return []*commandpb.Command{}, nil
				}
			}
		} else if newWorkflowStarted {
			newWorkflowStarted = false
			signalEvent = nil
			startedEvent = nil
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
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

	// Make first command to schedule activity
	_, err := poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Send a signal
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	wfIDReusePolicy := enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	sRequest := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:             uuid.NewString(),
		Namespace:             env.Namespace().String(),
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
	}
	resp, err := env.FrontendClient().SignalWithStartWorkflowExecution(env.Context(), sRequest)
	s.NoError(err)
	s.False(resp.Started)
	s.Equal(we.GetRunId(), resp.GetRunId())

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	// Terminate workflow execution
	_, err = env.FrontendClient().TerminateWorkflowExecution(env.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "test signal",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	// Send signal to terminated workflow
	signalName = "signal to terminate"
	signalInput = payloads.EncodeString("signal to terminate input")
	sRequest.SignalName = signalName
	sRequest.SignalInput = signalInput
	sRequest.WorkflowId = id

	resp, err = env.FrontendClient().SignalWithStartWorkflowExecution(env.Context(), sRequest)
	s.NoError(err)
	s.True(resp.Started)
	s.NotNil(resp.GetRunId())
	s.NotEqual(we.GetRunId(), resp.GetRunId())
	newWorkflowStarted = true

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)
	s.NotNil(startedEvent)
	s.ProtoEqual(header, startedEvent.GetWorkflowExecutionStartedEventAttributes().Header)

	// Send signal to not existed workflow
	id = "functional-signal-with-start-workflow-test-non-exist"
	signalName = "signal to non exist"
	signalInput = payloads.EncodeString("signal to non exist input")
	sRequest.SignalName = signalName
	sRequest.SignalInput = signalInput
	sRequest.WorkflowId = id
	resp, err = env.FrontendClient().SignalWithStartWorkflowExecution(env.Context(), sRequest)
	s.NoError(err)
	s.NotNil(resp.GetRunId())
	s.True(resp.Started)
	newWorkflowStarted = true

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	listOpenRequest := &workflowservice.ListOpenWorkflowExecutionsRequest{
		Namespace:       env.Namespace().String(),
		MaximumPageSize: 100,
		StartTimeFilter: &filterpb.StartTimeFilter{
			EarliestTime: nil,
			LatestTime:   timestamppb.New(time.Now().UTC()),
		},
		Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{
			ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: id,
			},
		},
	}

	// Assert visibility is correct
	s.Eventually(
		func() bool {
			listResp, err := env.FrontendClient().ListOpenWorkflowExecutions(env.Context(), listOpenRequest)
			s.NoError(err)
			return len(listResp.Executions) == 1
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	// Terminate workflow execution and assert visibility is correct
	_, err = env.FrontendClient().TerminateWorkflowExecution(env.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "kill workflow",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	s.Eventually(
		func() bool {
			listResp, err := env.FrontendClient().ListOpenWorkflowExecutions(env.Context(), listOpenRequest)
			s.NoError(err)
			return len(listResp.Executions) == 0
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	listClosedRequest := &workflowservice.ListClosedWorkflowExecutionsRequest{
		Namespace:       env.Namespace().String(),
		MaximumPageSize: 100,
		StartTimeFilter: &filterpb.StartTimeFilter{
			EarliestTime: nil,
			LatestTime:   timestamppb.New(time.Now().UTC()),
		},
		Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
			WorkflowId: id,
		}},
	}
	listClosedResp, err := env.FrontendClient().ListClosedWorkflowExecutions(env.Context(), listClosedRequest)
	s.NoError(err)
	s.Len(listClosedResp.Executions, 1)
}

func (s *SignalWorkflowTestSuite) TestSignalWithStartWorkflow_ResolveIDDeduplication() {
	env := testcore.NewEnv(s.T())

	// setting this to 0 to be sure we are terminating the current workflow
	env.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, 0)

	id := "functional-signal-with-start-workflow-id-reuse-test"
	wt := "functional-signal-with-start-workflow-id-reuse-test-type"
	tl := "functional-signal-with-start-workflow-id-reuse-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start a workflow
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

	we, err0 := env.FrontendClient().StartWorkflowExecution(env.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

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

	// Start workflows, make some progress and complete workflow
	_, err := poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	_, err = poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)

	// test WorkflowIdReusePolicy: RejectDuplicate
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	sRequest := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:             uuid.NewString(),
		Namespace:             env.Namespace().String(),
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
	}
	ctx, _ := rpc.NewContextWithTimeoutAndVersionHeaders(5 * time.Second)
	resp, err := env.FrontendClient().SignalWithStartWorkflowExecution(ctx, sRequest)
	s.Nil(resp)
	s.Error(err)
	s.Contains(err.Error(), "reject duplicate workflow Id")
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)

	// test WorkflowIdReusePolicy: AllowDuplicateFailedOnly
	sRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	ctx, _ = rpc.NewContextWithTimeoutAndVersionHeaders(5 * time.Second)
	resp, err = env.FrontendClient().SignalWithStartWorkflowExecution(ctx, sRequest)
	s.Nil(resp)
	s.Error(err)
	s.Contains(err.Error(), "allow duplicate workflow Id if last run failed")
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)

	// test WorkflowIdReusePolicy: AllowDuplicate
	sRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	ctx, _ = rpc.NewContextWithTimeoutAndVersionHeaders(5 * time.Second)
	resp, err = env.FrontendClient().SignalWithStartWorkflowExecution(ctx, sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
	s.True(resp.Started)

	// Terminate workflow execution
	_, err = env.FrontendClient().TerminateWorkflowExecution(env.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   "test WorkflowIdReusePolicyAllowDuplicateFailedOnly",
		Details:  nil,
		Identity: identity,
	})
	s.NoError(err)

	// test WorkflowIdReusePolicy: AllowDuplicateFailedOnly
	sRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	resp, err = env.FrontendClient().SignalWithStartWorkflowExecution(env.Context(), sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
	s.True(resp.Started)

	// test WorkflowIdReusePolicy: TerminateIfRunning (for backwards compatibility)
	prevRunID := resp.RunId
	sRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING
	resp, err = env.FrontendClient().SignalWithStartWorkflowExecution(env.Context(), sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
	s.NotEqual(prevRunID, resp.GetRunId())
	s.True(resp.Started)

	descResp, err := env.FrontendClient().DescribeWorkflowExecution(env.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: prevRunID},
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.WorkflowExecutionInfo.Status)

	// test WorkflowIdConflictPolicy: TerminateExisting (replaced TerminateIfRunning)
	prevRunID = resp.RunId
	sRequest.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	sRequest.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
	resp, err = env.FrontendClient().SignalWithStartWorkflowExecution(env.Context(), sRequest)
	s.NoError(err)
	s.NotEmpty(resp.GetRunId())
	s.NotEqual(prevRunID, resp.GetRunId())
	s.True(resp.Started)

	descResp, err = env.FrontendClient().DescribeWorkflowExecution(env.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: prevRunID},
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.WorkflowExecutionInfo.Status)

	descResp, err = env.FrontendClient().DescribeWorkflowExecution(env.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resp.GetRunId(),
		},
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, descResp.WorkflowExecutionInfo.Status)
}

func (s *SignalWorkflowTestSuite) TestSignalWithStartWorkflow_StartDelay() {
	env := testcore.NewEnv(s.T())
	id := "functional-signal-with-start-workflow-start-delay-test"
	wt := "functional-signal-with-start-workflow-start-delay-test-type"
	tl := "functional-signal-with-start-workflow-start-delay-test-taskqueue"
	stickyTq := "functional-signal-with-start-workflow-start-delay-test-sticky-taskqueue"
	identity := "worker1"

	startDelay := 3 * time.Second

	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")

	sRequest := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		SignalName:          signalName,
		SignalInput:         signalInput,
		Identity:            identity,
		WorkflowStartDelay:  durationpb.New(startDelay),
	}

	reqStartTime := time.Now()
	we0, startErr := env.FrontendClient().SignalWithStartWorkflowExecution(env.Context(), sRequest)
	s.NoError(startErr)

	var signalEvent *historypb.HistoryEvent
	delayEndTime := time.Now()

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		delayEndTime = time.Now()

		for _, event := range task.History.Events[task.PreviousStartedEventId:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signalEvent = event
			}
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		StickyTaskQueue:     &taskqueuepb.TaskQueue{Name: stickyTq, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, pollErr := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(pollErr)
	s.GreaterOrEqual(delayEndTime.Sub(reqStartTime), startDelay)
	s.NotNil(signalEvent)
	s.Equal(signalName, signalEvent.GetWorkflowExecutionSignaledEventAttributes().SignalName)
	s.ProtoEqual(signalInput, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Input)
	s.Equal(identity, signalEvent.GetWorkflowExecutionSignaledEventAttributes().Identity)

	descResp, descErr := env.FrontendClient().DescribeWorkflowExecution(env.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we0.RunId,
		},
	})
	s.NoError(descErr)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.WorkflowExecutionInfo.Status)
}
