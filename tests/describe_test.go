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
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type DescribeTestSuite struct {
	testcore.FunctionalTestBase
}

func TestDescribeTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(DescribeTestSuite))
}

func (s *DescribeTestSuite) TestDescribeWorkflowExecution() {
	id := "functional-describe-wfe-test"
	wt := "functional-describe-wfe-test-type"
	tq := "functional-describe-wfe-test-taskqueue"
	identity := "worker1"

	// Start workflow execution
	requestID := uuid.NewString()
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           requestID,
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
	}
	dweResponse, err := describeWorkflowExecution()
	s.NoError(err)
	wfInfo := dweResponse.WorkflowExecutionInfo
	s.Nil(wfInfo.CloseTime)
	s.Nil(wfInfo.ExecutionDuration)
	s.Equal(int64(2), wfInfo.HistoryLength) // WorkflowStarted, WorkflowTaskScheduled
	s.Equal(wfInfo.GetStartTime(), wfInfo.GetExecutionTime())
	s.Equal(tq, wfInfo.TaskQueue)
	s.Greater(wfInfo.GetHistorySizeBytes(), int64(0))
	s.Empty(wfInfo.GetParentNamespaceId())
	s.Nil(wfInfo.GetParentExecution())
	s.NotNil(wfInfo.GetRootExecution())
	s.Equal(id, wfInfo.RootExecution.GetWorkflowId())
	s.Equal(we.RunId, wfInfo.RootExecution.GetRunId())
	s.Equal(we.RunId, wfInfo.GetFirstRunId())
	s.NotNil(dweResponse.WorkflowExtendedInfo)
	s.Nil(dweResponse.WorkflowExtendedInfo.LastResetTime) // workflow was not reset
	s.Nil(dweResponse.WorkflowExtendedInfo.ExecutionExpirationTime)
	s.NotNil(dweResponse.WorkflowExtendedInfo.RunExpirationTime)
	s.NotNil(dweResponse.WorkflowExtendedInfo.OriginalStartTime)
	s.NotNil(dweResponse.WorkflowExtendedInfo.RequestIdInfos)
	s.Contains(dweResponse.WorkflowExtendedInfo.RequestIdInfos, requestID)
	s.NotNil(dweResponse.WorkflowExtendedInfo.RequestIdInfos[requestID])
	s.ProtoEqual(
		&workflowpb.RequestIdInfo{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			EventId:   common.FirstEventID,
			Buffered:  false,
		},
		dweResponse.WorkflowExtendedInfo.RequestIdInfos[requestID],
	)

	// workflow logic
	workflowComplete := false
	signalSent := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !signalSent {
			signalSent = true

			s.NoError(err)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             "1",
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeString("test-input"),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
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
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// first workflow task to schedule new activity
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	wfInfo = dweResponse.WorkflowExecutionInfo
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, wfInfo.GetStatus())
	s.Nil(wfInfo.CloseTime)
	s.Nil(wfInfo.ExecutionDuration)
	s.Equal(int64(5), wfInfo.HistoryLength) // WorkflowTaskStarted, WorkflowTaskCompleted, ActivityScheduled
	s.Equal(1, len(dweResponse.PendingActivities))
	s.Equal("test-activity-type", dweResponse.PendingActivities[0].ActivityType.GetName())
	s.True(timestamp.TimeValue(dweResponse.PendingActivities[0].GetLastHeartbeatTime()).IsZero())

	// process activity task
	err = poller.PollAndProcessActivityTask(false)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	wfInfo = dweResponse.WorkflowExecutionInfo
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, wfInfo.GetStatus())
	s.Equal(int64(8), wfInfo.HistoryLength) // ActivityTaskStarted, ActivityTaskCompleted, WorkflowTaskScheduled
	s.Equal(0, len(dweResponse.PendingActivities))

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	wfInfo = dweResponse.WorkflowExecutionInfo
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, wfInfo.GetStatus())
	s.NotNil(wfInfo.CloseTime)
	s.NotNil(wfInfo.ExecutionDuration)
	s.Equal(
		wfInfo.GetCloseTime().AsTime().Sub(wfInfo.ExecutionTime.AsTime()),
		wfInfo.ExecutionDuration.AsDuration(),
	)
	s.Equal(int64(11), wfInfo.HistoryLength) // WorkflowTaskStarted, WorkflowTaskCompleted, WorkflowCompleted
}

func (s *DescribeTestSuite) TestDescribeTaskQueue() {
	workflowID := "functional-get-poller-history"
	wt := "functional-get-poller-history-type"
	tl := "functional-get-poller-history-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(25 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(25 * time.Second),
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
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// this function poll events from history side
	testDescribeTaskQueue := func(namespace string, taskqueue *taskqueuepb.TaskQueue, taskqueueType enumspb.TaskQueueType) []*taskqueuepb.PollerInfo {
		responseInner, errInner := s.FrontendClient().DescribeTaskQueue(testcore.NewContext(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:     namespace,
			TaskQueue:     taskqueue,
			TaskQueueType: taskqueueType,
		})

		s.NoError(errInner)
		return responseInner.Pollers
	}

	before := time.Now().UTC()

	// when no one polling on the taskqueue (activity or workflow), there shall be no poller information
	tq := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	pollerInfos := testDescribeTaskQueue(s.Namespace().String(), tq, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.Empty(pollerInfos)
	pollerInfos = testDescribeTaskQueue(s.Namespace().String(), tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Empty(pollerInfos)

	_, errWorkflowTask := poller.PollAndProcessWorkflowTask()
	s.NoError(errWorkflowTask)
	pollerInfos = testDescribeTaskQueue(s.Namespace().String(), tq, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.Empty(pollerInfos)
	pollerInfos = testDescribeTaskQueue(s.Namespace().String(), tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(pollerInfos[0].GetLastAccessTime().AsTime().After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())

	errActivity := poller.PollAndProcessActivityTask(false)
	s.NoError(errActivity)
	pollerInfos = testDescribeTaskQueue(s.Namespace().String(), tq, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(pollerInfos[0].GetLastAccessTime().AsTime().After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())
	pollerInfos = testDescribeTaskQueue(s.Namespace().String(), tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.Equal(1, len(pollerInfos))
	s.Equal(identity, pollerInfos[0].GetIdentity())
	s.True(pollerInfos[0].GetLastAccessTime().AsTime().After(before))
	s.NotEmpty(pollerInfos[0].GetLastAccessTime())
}
