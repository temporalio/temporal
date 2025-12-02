package tests

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type TransientTaskSuite struct {
	testcore.FunctionalTestBase
}

func TestTransientTaskSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(TransientTaskSuite))
}

func (s *TransientTaskSuite) TestTransientWorkflowTaskTimeout() {
	id := "functional-transient-workflow-task-timeout-test"
	wt := "functional-transient-workflow-task-timeout-test-type"
	tl := "functional-transient-workflow-task-timeout-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// workflow logic
	workflowComplete := false
	failWorkflowTask := true
	signalCount := 0
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if failWorkflowTask {
			failWorkflowTask = false
			return nil, errors.New("workflow panic") //nolint:err113
		}

		// Count signals
		for _, event := range task.History.Events[task.PreviousStartedEventId:] {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				signalCount++
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

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// First workflow task immediately fails and schedules a transient workflow task
	_, err := poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Now send a signal when transient workflow task is scheduled
	err = s.SendSignal(s.Namespace().String(), workflowExecution, "signalA", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// Drop workflow task to cause a workflow task timeout
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithDropTask)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// Now process signal and complete workflow execution
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithExpectedAttemptCount(2))
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.Equal(1, signalCount)
	s.True(workflowComplete)
}

func (s *TransientTaskSuite) TestTransientWorkflowTaskHistorySize() {
	id := "functional-transient-workflow-task-history-size-test"
	wt := "functional-transient-workflow-task-history-size-test-type"
	tl := "functional-transient-workflow-task-history-size-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	}

	// start with 2mb limit
	s.OverrideDynamicConfig(dynamicconfig.HistorySizeSuggestContinueAsNew, 2*1024*1024)

	// workflow logic
	stage := 0
	workflowComplete := false
	largeValue := make([]byte, 1024*1024)
	// record the values that we see for completed tasks here
	type fields struct {
		size    int64
		suggest bool
	}
	var sawFields []fields
	// record value for failed wft
	var failedTaskSawSize int64
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		// find workflow task started event
		event := task.History.Events[len(task.History.Events)-1]
		s.Equal(event.GetEventType(), enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED)
		attrs := event.GetWorkflowTaskStartedEventAttributes()
		s.Logger.Info("wtHandler", tag.Counter(stage))

		stage++
		switch stage {
		case 1:
			s.Less(attrs.HistorySizeBytes, int64(1024*1024))
			s.False(attrs.SuggestContinueAsNew)
			// record a large marker
			sawFields = append(sawFields, fields{size: attrs.HistorySizeBytes, suggest: attrs.SuggestContinueAsNew})
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "big marker",
					Details:    map[string]*commonpb.Payloads{"value": payloads.EncodeBytes(largeValue)},
				}},
			}}, nil

		case 2:
			s.Greater(attrs.HistorySizeBytes, int64(1024*1024))
			s.False(attrs.SuggestContinueAsNew)
			// record another large marker
			sawFields = append(sawFields, fields{size: attrs.HistorySizeBytes, suggest: attrs.SuggestContinueAsNew})
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
					MarkerName: "big marker",
					Details:    map[string]*commonpb.Payloads{"value": payloads.EncodeBytes(largeValue)},
				}},
			}}, nil

		case 3:
			s.Greater(attrs.HistorySizeBytes, int64(2048*1024))
			s.True(attrs.SuggestContinueAsNew)
			failedTaskSawSize = attrs.HistorySizeBytes
			// fail workflow task and we'll get a transient one
			return nil, errors.New("oops") //nolint:err113

		case 4:
			// we might not get the same value but it shouldn't be smaller, and not too much larger
			s.GreaterOrEqual(attrs.HistorySizeBytes, failedTaskSawSize)
			s.Less(attrs.HistorySizeBytes, failedTaskSawSize+10000)
			s.False(attrs.SuggestContinueAsNew)
			sawFields = append(sawFields, fields{size: attrs.HistorySizeBytes, suggest: attrs.SuggestContinueAsNew})
			return nil, nil

		case 5:
			// we should get just a little larger
			prevSize := sawFields[len(sawFields)-1].size
			s.Greater(attrs.HistorySizeBytes, prevSize)
			s.Less(attrs.HistorySizeBytes, prevSize+10000)
			s.False(attrs.SuggestContinueAsNew) // now false

			workflowComplete = true
			sawFields = append(sawFields, fields{size: attrs.HistorySizeBytes, suggest: attrs.SuggestContinueAsNew})
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				}},
			}}, nil
		}

		return nil, errors.New("bad stage") //nolint:err113
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// stage 1
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithNoDumpCommands)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = s.SendSignal(s.Namespace().String(), workflowExecution, "signal", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// stage 2
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithNoDumpCommands)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = s.SendSignal(s.Namespace().String(), workflowExecution, "signal", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// stage 3: this one fails with a panic
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithNoDumpCommands)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// change the dynamic config so that SuggestContinueAsNew should now be false. the current
	// workflow task should still see true, but the next one will see false.
	s.OverrideDynamicConfig(dynamicconfig.HistorySizeSuggestContinueAsNew, 8*1024*1024)

	// stage 4
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithNoDumpCommands)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	err = s.SendSignal(s.Namespace().String(), workflowExecution, "signal", nil, identity)
	s.NoError(err, "failed to send signal to execution")

	// drop workflow task to cause a workflow task timeout
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDropTask, testcore.WithNoDumpCommands)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// stage 5
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithNoDumpCommands)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)

	var sawFieldsFlat []any
	for _, f := range sawFields {
		sawFieldsFlat = append(sawFieldsFlat, f.size, f.suggest)
	}

	allEvents := s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(fmt.Sprintf(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted {"HistorySizeBytes":%d, "SuggestContinueAsNew":%t}
  4 WorkflowTaskCompleted // 1 WFTCompleted
  5 MarkerRecorded
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted {"HistorySizeBytes":%d, "SuggestContinueAsNew":%t}
  9 WorkflowTaskCompleted // 2 WFTCompleted
 10 MarkerRecorded
 11 WorkflowExecutionSignaled
 12 WorkflowTaskScheduled
 13 WorkflowTaskStarted
 14 WorkflowTaskFailed
 15 WorkflowTaskScheduled
 16 WorkflowTaskStarted {"HistorySizeBytes":%d, "SuggestContinueAsNew":%t}
 17 WorkflowTaskCompleted // 3 WFTCompleted
 18 WorkflowExecutionSignaled
 19 WorkflowTaskScheduled
 20 WorkflowTaskStarted
 21 WorkflowTaskTimedOut
 22 WorkflowTaskScheduled
 23 WorkflowTaskStarted {"HistorySizeBytes":%d, "SuggestContinueAsNew":%t}
 24 WorkflowTaskCompleted // 4 WFTCompleted
 25 WorkflowExecutionCompleted`, sawFieldsFlat...), allEvents)
}

func (s *TransientTaskSuite) TestNoTransientWorkflowTaskAfterFlushBufferedEvents() {
	id := "functional-no-transient-workflow-task-after-flush-buffered-events-test"
	wt := "functional-no-transient-workflow-task-after-flush-buffered-events-test-type"
	tl := "functional-no-transient-workflow-task-after-flush-buffered-events-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(20 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	workflowComplete := false
	continueAsNewAndSignal := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !continueAsNewAndSignal {
			continueAsNewAndSignal = true
			// this will create new event when there is in-flight workflow task, and the new event will be buffered
			_, err := s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(),
				&workflowservice.SignalWorkflowExecutionRequest{
					Namespace: s.Namespace().String(),
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: id,
					},
					SignalName: "buffered-signal-1",
					Input:      payloads.EncodeString("buffered-signal-input"),
					Identity:   identity,
				})
			s.NoError(err)

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					WorkflowType:        task.WorkflowType,
					TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:               nil,
					WorkflowRunTimeout:  durationpb.New(1000 * time.Second),
					WorkflowTaskTimeout: durationpb.New(100 * time.Second),
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

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// fist workflow task, this try to do a continue as new but there is a buffered event,
	// so it will fail and create a new workflow task
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
	s.Equal("UnhandledCommand", err.Error())

	// second workflow task, which will complete the workflow
	// this expect the workflow task to have attempt == 1
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithExpectedAttemptCount(1))
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.True(workflowComplete)
}
