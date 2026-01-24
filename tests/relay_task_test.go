package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRelayTask(t *testing.T) {
	t.Run("WorkflowTaskTimeout", func(t *testing.T) {
		s := testcore.NewEnv(t)

		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.Namespace().String(),
			WorkflowId:          s.Tv().WorkflowID(),
			WorkflowType:        s.Tv().WorkflowType(),
			TaskQueue:           s.Tv().TaskQueue(),
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(2 * time.Second),
			Identity:            s.Tv().WorkerIdentity(),
		}

		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)

		workflowExecution := &commonpb.WorkflowExecution{
			WorkflowId: s.Tv().WorkflowID(),
			RunId:      we.RunId,
		}

		workflowComplete, isFirst := false, true
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			if isFirst {
				isFirst = false
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
					Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
						MarkerName: "test-marker",
					}},
				}}, nil
			}
			workflowComplete = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}}}}, nil
		}

		poller := &testcore.TaskPoller{
			Client:              s.FrontendClient(),
			Namespace:           s.Namespace().String(),
			TaskQueue:           s.Tv().TaskQueue(),
			Identity:            s.Tv().WorkerIdentity(),
			WorkflowTaskHandler: wtHandler,
			ActivityTaskHandler: nil,
			Logger:              s.Logger,
			T:                   s.T(),
		}

		// First workflow task complete with a marker command, and request to relay workflow task (immediately return a new workflow task)
		res, err := poller.PollAndProcessWorkflowTask(
			testcore.WithExpectedAttemptCount(0),
			testcore.WithRetries(3),
			testcore.WithForceNewWorkflowTask)
		s.NoError(err)
		newTask := res.NewTask
		s.NotNil(newTask)
		s.NotNil(newTask.WorkflowTask)

		//nolint:forbidigo
		time.Sleep(time.Second * 2) // wait 2s for relay workflow task to timeout
		workflowTaskTimeout := false
		for i := 0; i < 3; i++ {
			events := s.GetHistory(s.Namespace().String(), workflowExecution)
			if len(events) == 8 {
				s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskTimedOut {"ScheduledEventId":6,"StartedEventId":7,"TimeoutType":1} // TIMEOUT_TYPE_START_TO_CLOSE`, events)
				workflowTaskTimeout = true
				break
			}
			time.Sleep(time.Second) //nolint:forbidigo
		}
		// verify relay workflow task timeout
		s.True(workflowTaskTimeout)

		// Now complete workflow
		_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory, testcore.WithExpectedAttemptCount(2))
		s.NoError(err)

		s.True(workflowComplete)
	})
}
