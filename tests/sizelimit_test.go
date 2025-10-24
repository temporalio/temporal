package tests

import (
	"bytes"
	"encoding/binary"
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
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SizeLimitFunctionalSuite struct {
	testcore.FunctionalTestBase
}

func TestSizeLimitFunctionalSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(SizeLimitFunctionalSuite))
}

func (s *SizeLimitFunctionalSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.HistoryCountLimitWarn.Key():      10,
		dynamicconfig.HistoryCountLimitError.Key():     20,
		dynamicconfig.HistorySizeLimitWarn.Key():       5000,
		dynamicconfig.HistorySizeLimitError.Key():      9000,
		dynamicconfig.BlobSizeLimitWarn.Key():          1,
		dynamicconfig.BlobSizeLimitError.Key():         1000,
		dynamicconfig.MutableStateSizeLimitWarn.Key():  200,
		dynamicconfig.MutableStateSizeLimitError.Key(): 1100,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *SizeLimitFunctionalSuite) TestTerminateWorkflowCausedByHistoryCountLimit() {
	id := "functional-terminate-workflow-by-history-count-limit-test"
	wt := "functional-terminate-workflow-by-history-count-limit-test-type"
	tq := "functional-terminate-workflow-by-history-count-limit-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	activityCount := int32(4)
	activityCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		}

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
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	for i := int32(0); i < activityCount-1; i++ {
		dwResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)

		// Poll workflow task only if it is running
		if dwResp.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			_, err := poller.PollAndProcessWorkflowTask()
			s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
			s.NoError(err)

			err = poller.PollAndProcessActivityTask(false)
			s.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
			s.NoError(err)
		}
	}

	var signalErr error
	// Send signals until workflow is force terminated
SignalLoop:
	for i := 0; i < 10; i++ {
		// Send another signal without RunID
		signalName := "another signal"
		signalInput := payloads.EncodeString("another signal input")
		_, signalErr = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: id,
			},
			SignalName: signalName,
			Input:      signalInput,
			Identity:   identity,
		})

		if signalErr != nil {
			break SignalLoop
		}
	}
	// Signalling workflow should result in force terminating the workflow execution and returns with ResourceExhausted
	// error. InvalidArgument is returned by the client.
	s.EqualError(signalErr, common.FailureReasonHistoryCountExceedsLimit)
	s.IsType(&serviceerror.InvalidArgument{}, signalErr)

	historyEvents := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	})
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskStarted
  7 ActivityTaskCompleted
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 ActivityTaskScheduled
 12 ActivityTaskStarted
 13 ActivityTaskCompleted
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted
 16 WorkflowTaskCompleted
 17 ActivityTaskScheduled
 18 ActivityTaskStarted
 19 ActivityTaskCompleted
 20 WorkflowTaskScheduled
 21 WorkflowExecutionSignaled
 22 WorkflowExecutionTerminated`, historyEvents)

	// verify visibility is correctly processed from open to close
	s.Eventually(
		func() bool {
			resp, err1 := s.FrontendClient().ListClosedWorkflowExecutions(
				testcore.NewContext(),
				&workflowservice.ListClosedWorkflowExecutionsRequest{
					Namespace:       s.Namespace().String(),
					MaximumPageSize: 100,
					StartTimeFilter: &filterpb.StartTimeFilter{
						EarliestTime: nil,
						LatestTime:   timestamppb.New(time.Now().UTC()),
					},
					Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{
						ExecutionFilter: &filterpb.WorkflowExecutionFilter{
							WorkflowId: id,
						},
					},
				},
			)
			s.NoError(err1)
			if len(resp.Executions) == 1 {
				return true
			}
			s.Logger.Info("Closed WorkflowExecution is not yet visible")
			return false
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
}

func (s *SizeLimitFunctionalSuite) TestWorkflowFailed_PayloadSizeTooLarge() {

	id := "functional-workflow-failed-large-payload"
	wt := "functional-workflow-failed-large-payload-type"
	tl := "functional-workflow-failed-large-payload-taskqueue"
	identity := "worker1"

	largePayload := make([]byte, 1001)
	pl, err := payloads.Encode(largePayload)
	s.NoError(err)
	sigReadyToSendChan := make(chan struct{}, 1)
	sigSendDoneChan := make(chan struct{})
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		select {
		case sigReadyToSendChan <- struct{}{}:
		default:
		}

		select {
		case <-sigSendDoneChan:
		}
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
				Attributes: &commandpb.Command_RecordMarkerCommandAttributes{
					RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{
						MarkerName: "large-payload",
						Details:    map[string]*commonpb.Payloads{"test": pl},
					},
				},
			},
		}, nil
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

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowTaskTimeout: durationpb.New(60 * time.Second),
		Identity:            identity,
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)

	go func() {
		_, err = poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	}()

	select {
	case <-sigReadyToSendChan:
	}

	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: we.GetRunId()},
		SignalName:        "signal-name",
		Identity:          identity,
		RequestId:         uuid.NewString(),
	})
	s.NoError(err)
	close(sigSendDoneChan)

	// Wait for workflow to fail.
	var historyEvents []*historypb.HistoryEvent
	for i := 0; i < 10; i++ {
		historyEvents = s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: id, RunId: we.GetRunId()})
		lastEvent := historyEvents[len(historyEvents)-1]
		if lastEvent.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED {
			break
		}
		time.Sleep(time.Second) //nolint:forbidigo
	}
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionTerminated`, historyEvents)
}

func (s *SizeLimitFunctionalSuite) TestTerminateWorkflowCausedByMsSizeLimit() {
	id := "functional-terminate-workflow-by-ms-size-limit-test"
	wt := "functional-terminate-workflow-by-ms-size-limit-test-type"
	tq := "functional-terminate-workflow-by-ms-size-limit-test-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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

	activityCount := int32(4)
	activitiesScheduled := false
	activityLargePayload := payloads.EncodeBytes(make([]byte, 900))
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !activitiesScheduled {
			cmds := make([]*commandpb.Command, activityCount)
			for i := range cmds {
				cmds[i] = &commandpb.Command{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             convert.Int32ToString(int32(i)),
						ActivityType:           &commonpb.ActivityType{Name: activityName},
						TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						Input:                  activityLargePayload,
						ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
						ScheduleToStartTimeout: durationpb.New(10 * time.Second),
						StartToCloseTimeout:    durationpb.New(50 * time.Second),
						HeartbeatTimeout:       durationpb.New(5 * time.Second),
					}},
				}
			}
			return cmds, nil
		}

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
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	dwResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)

	// Poll workflow task only if it is running
	if dwResp.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		_, err := poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))

		// Workflow should be force terminated at this point
		s.EqualError(err, common.FailureReasonMutableStateSizeExceedsLimit)
	}

	// Send another signal without RunID
	_, signalErr := s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		SignalName: "another signal",
		Input:      payloads.EncodeString("another signal input"),
		Identity:   identity,
	})

	s.EqualError(signalErr, consts.ErrWorkflowCompleted.Error())
	s.IsType(&serviceerror.NotFound{}, signalErr)

	historyEvents := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	})
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionTerminated`, historyEvents)

	// verify visibility is correctly processed from open to close
	s.Eventually(
		func() bool {
			resp, err1 := s.FrontendClient().ListClosedWorkflowExecutions(
				testcore.NewContext(),
				&workflowservice.ListClosedWorkflowExecutionsRequest{
					Namespace:       s.Namespace().String(),
					MaximumPageSize: 100,
					StartTimeFilter: &filterpb.StartTimeFilter{
						EarliestTime: nil,
						LatestTime:   timestamppb.New(time.Now().UTC()),
					},
					Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{
						ExecutionFilter: &filterpb.WorkflowExecutionFilter{
							WorkflowId: id,
						},
					},
				},
			)
			s.NoError(err1)
			if len(resp.Executions) == 1 {
				return true
			}
			s.Logger.Info("Closed WorkflowExecution is not yet visible")
			return false
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
}

func (s *SizeLimitFunctionalSuite) TestTerminateWorkflowCausedByHistorySizeLimit() {
	id := "functional-terminate-workflow-by-history-size-limit-test"
	wt := "functional-terminate-workflow-by-history-size-limit-test-type"
	tq := "functional-terminate-workflow-by-history-size-limit-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var signalErr error
	// Send signals until workflow is force terminated
	largePayload := make([]byte, 900)
SignalLoop:
	for i := 0; i < 10; i++ {
		// Send another signal without RunID
		signalName := "another signal"
		signalInput, err := payloads.Encode(largePayload)
		s.NoError(err)
		_, signalErr = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: id,
			},
			SignalName: signalName,
			Input:      signalInput,
			Identity:   identity,
		})

		if signalErr != nil {
			break SignalLoop
		}
	}
	// Signalling workflow should result in force terminating the workflow execution and returns with ResourceExhausted
	// error. InvalidArgument is returned by the client.
	s.EqualError(signalErr, common.FailureReasonHistorySizeExceedsLimit)
	s.IsType(&serviceerror.InvalidArgument{}, signalErr)

	historyEvents := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	})
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionSignaled
  4 WorkflowExecutionSignaled
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionSignaled
  7 WorkflowExecutionSignaled
  8 WorkflowExecutionSignaled
  9 WorkflowExecutionSignaled
 10 WorkflowExecutionSignaled
 11 WorkflowExecutionSignaled
 12 WorkflowExecutionTerminated`, historyEvents)

	// verify visibility is correctly processed from open to close
	s.Eventually(
		func() bool {
			resp, err1 := s.FrontendClient().ListClosedWorkflowExecutions(
				testcore.NewContext(),
				&workflowservice.ListClosedWorkflowExecutionsRequest{
					Namespace:       s.Namespace().String(),
					MaximumPageSize: 100,
					StartTimeFilter: &filterpb.StartTimeFilter{
						EarliestTime: nil,
						LatestTime:   timestamppb.New(time.Now().UTC()),
					},
					Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{
						ExecutionFilter: &filterpb.WorkflowExecutionFilter{
							WorkflowId: id,
						},
					},
				},
			)
			s.NoError(err1)
			if len(resp.Executions) == 1 {
				return true
			}
			s.Logger.Info("Closed WorkflowExecution is not yet visible")
			return false
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
}
