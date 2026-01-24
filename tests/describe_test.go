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
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           requestID,
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        commonpb.WorkflowType_builder{Name: wt}.Build(),
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), workflowservice.DescribeWorkflowExecutionRequest_builder{
			Namespace: s.Namespace().String(),
			Execution: commonpb.WorkflowExecution_builder{
				WorkflowId: id,
				RunId:      we.GetRunId(),
			}.Build(),
		}.Build())
	}
	dweResponse, err := describeWorkflowExecution()
	s.NoError(err)
	wfInfo := dweResponse.GetWorkflowExecutionInfo()
	s.Nil(wfInfo.GetCloseTime())
	s.Nil(wfInfo.GetExecutionDuration())
	s.Equal(int64(2), wfInfo.GetHistoryLength()) // WorkflowStarted, WorkflowTaskScheduled
	s.Equal(wfInfo.GetStartTime(), wfInfo.GetExecutionTime())
	s.Equal(tq, wfInfo.GetTaskQueue())
	s.Greater(wfInfo.GetHistorySizeBytes(), int64(0))
	s.Empty(wfInfo.GetParentNamespaceId())
	s.Nil(wfInfo.GetParentExecution())
	s.NotNil(wfInfo.GetRootExecution())
	s.Equal(id, wfInfo.GetRootExecution().GetWorkflowId())
	s.Equal(we.GetRunId(), wfInfo.GetRootExecution().GetRunId())
	s.Equal(we.GetRunId(), wfInfo.GetFirstRunId())
	s.NotNil(dweResponse.GetWorkflowExtendedInfo())
	s.Nil(dweResponse.GetWorkflowExtendedInfo().GetLastResetTime()) // workflow was not reset
	s.Nil(dweResponse.GetWorkflowExtendedInfo().GetExecutionExpirationTime())
	s.NotNil(dweResponse.GetWorkflowExtendedInfo().GetRunExpirationTime())
	s.NotNil(dweResponse.GetWorkflowExtendedInfo().GetOriginalStartTime())
	s.NotNil(dweResponse.GetWorkflowExtendedInfo().GetRequestIdInfos())
	s.Contains(dweResponse.GetWorkflowExtendedInfo().GetRequestIdInfos(), requestID)
	s.NotNil(dweResponse.GetWorkflowExtendedInfo().GetRequestIdInfos()[requestID])
	s.ProtoEqual(
		workflowpb.RequestIdInfo_builder{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			EventId:   common.FirstEventID,
			Buffered:  false,
		}.Build(),
		dweResponse.GetWorkflowExtendedInfo().GetRequestIdInfos()[requestID],
	)

	// workflow logic
	workflowComplete := false
	signalSent := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !signalSent {
			signalSent = true

			s.NoError(err)
			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             "1",
					ActivityType:           commonpb.ActivityType_builder{Name: "test-activity-type"}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeString("test-input"),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
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
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
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
	wfInfo = dweResponse.GetWorkflowExecutionInfo()
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, wfInfo.GetStatus())
	s.Nil(wfInfo.GetCloseTime())
	s.Nil(wfInfo.GetExecutionDuration())
	s.Equal(int64(5), wfInfo.GetHistoryLength()) // WorkflowTaskStarted, WorkflowTaskCompleted, ActivityScheduled
	s.Equal(1, len(dweResponse.GetPendingActivities()))
	s.Equal("test-activity-type", dweResponse.GetPendingActivities()[0].GetActivityType().GetName())
	s.True(timestamp.TimeValue(dweResponse.GetPendingActivities()[0].GetLastHeartbeatTime()).IsZero())

	// process activity task
	err = poller.PollAndProcessActivityTask(false)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	wfInfo = dweResponse.GetWorkflowExecutionInfo()
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, wfInfo.GetStatus())
	s.Equal(int64(8), wfInfo.GetHistoryLength()) // ActivityTaskStarted, ActivityTaskCompleted, WorkflowTaskScheduled
	s.Equal(0, len(dweResponse.GetPendingActivities()))

	// Process signal in workflow
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)

	dweResponse, err = describeWorkflowExecution()
	s.NoError(err)
	wfInfo = dweResponse.GetWorkflowExecutionInfo()
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, wfInfo.GetStatus())
	s.NotNil(wfInfo.GetCloseTime())
	s.NotNil(wfInfo.GetExecutionDuration())
	s.Equal(
		wfInfo.GetCloseTime().AsTime().Sub(wfInfo.GetExecutionTime().AsTime()),
		wfInfo.GetExecutionDuration().AsDuration(),
	)
	s.Equal(int64(11), wfInfo.GetHistoryLength()) // WorkflowTaskStarted, WorkflowTaskCompleted, WorkflowCompleted
}

func (s *DescribeTestSuite) TestDescribeTaskQueue() {
	workflowID := "functional-get-poller-history"
	wt := "functional-get-poller-history-type"
	tl := "functional-get-poller-history-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	// Start workflow execution
	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        commonpb.WorkflowType_builder{Name: wt}.Build(),
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             convert.Int32ToString(1),
					ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(25 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(25 * time.Second),
				}.Build(),
			}.Build()}, nil
		}

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
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// this function poll events from history side
	testDescribeTaskQueue := func(namespace string, taskqueue *taskqueuepb.TaskQueue, taskqueueType enumspb.TaskQueueType) []*taskqueuepb.PollerInfo {
		responseInner, errInner := s.FrontendClient().DescribeTaskQueue(testcore.NewContext(), workflowservice.DescribeTaskQueueRequest_builder{
			Namespace:     namespace,
			TaskQueue:     taskqueue,
			TaskQueueType: taskqueueType,
		}.Build())

		s.NoError(errInner)
		return responseInner.GetPollers()
	}

	before := time.Now().UTC()

	// when no one polling on the taskqueue (activity or workflow), there shall be no poller information
	tq := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
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
