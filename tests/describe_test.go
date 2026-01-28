package tests

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
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

func TestDescribe(t *testing.T) {
	t.Run("DescribeWorkflowExecution", func(t *testing.T) {
		s := testcore.NewEnv(t)

		id := s.Tv().WorkflowID()
		wt := s.Tv().WorkflowType().Name
		tq := s.Tv().TaskQueue().Name
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
		require.NoError(t, err0)

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
		require.NoError(t, err)
		wfInfo := dweResponse.WorkflowExecutionInfo
		require.Nil(t, wfInfo.CloseTime)
		require.Nil(t, wfInfo.ExecutionDuration)
		require.Equal(t, int64(2), wfInfo.HistoryLength) // WorkflowStarted, WorkflowTaskScheduled
		require.Equal(t, wfInfo.GetStartTime(), wfInfo.GetExecutionTime())
		require.Equal(t, tq, wfInfo.TaskQueue)
		require.Greater(t, wfInfo.GetHistorySizeBytes(), int64(0))
		require.Empty(t, wfInfo.GetParentNamespaceId())
		require.Nil(t, wfInfo.GetParentExecution())
		require.NotNil(t, wfInfo.GetRootExecution())
		require.Equal(t, id, wfInfo.RootExecution.GetWorkflowId())
		require.Equal(t, we.RunId, wfInfo.RootExecution.GetRunId())
		require.Equal(t, we.RunId, wfInfo.GetFirstRunId())
		require.NotNil(t, dweResponse.WorkflowExtendedInfo)
		require.Nil(t, dweResponse.WorkflowExtendedInfo.LastResetTime) // workflow was not reset
		require.Nil(t, dweResponse.WorkflowExtendedInfo.ExecutionExpirationTime)
		require.NotNil(t, dweResponse.WorkflowExtendedInfo.RunExpirationTime)
		require.NotNil(t, dweResponse.WorkflowExtendedInfo.OriginalStartTime)
		require.NotNil(t, dweResponse.WorkflowExtendedInfo.RequestIdInfos)
		require.Contains(t, dweResponse.WorkflowExtendedInfo.RequestIdInfos, requestID)
		require.NotNil(t, dweResponse.WorkflowExtendedInfo.RequestIdInfos[requestID])
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

				require.NoError(t, err)
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
			T:                   t,
		}

		// first workflow task to schedule new activity
		_, err = poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		require.NoError(t, err)

		dweResponse, err = describeWorkflowExecution()
		require.NoError(t, err)
		wfInfo = dweResponse.WorkflowExecutionInfo
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, wfInfo.GetStatus())
		require.Nil(t, wfInfo.CloseTime)
		require.Nil(t, wfInfo.ExecutionDuration)
		require.Equal(t, int64(5), wfInfo.HistoryLength) // WorkflowTaskStarted, WorkflowTaskCompleted, ActivityScheduled
		require.Equal(t, 1, len(dweResponse.PendingActivities))
		require.Equal(t, "test-activity-type", dweResponse.PendingActivities[0].ActivityType.GetName())
		require.True(t, timestamp.TimeValue(dweResponse.PendingActivities[0].GetLastHeartbeatTime()).IsZero())

		// process activity task
		err = poller.PollAndProcessActivityTask(false)

		dweResponse, err = describeWorkflowExecution()
		require.NoError(t, err)
		wfInfo = dweResponse.WorkflowExecutionInfo
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, wfInfo.GetStatus())
		require.Equal(t, int64(8), wfInfo.HistoryLength) // ActivityTaskStarted, ActivityTaskCompleted, WorkflowTaskScheduled
		require.Equal(t, 0, len(dweResponse.PendingActivities))

		// Process signal in workflow
		_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
		require.NoError(t, err)
		require.True(t, workflowComplete)

		dweResponse, err = describeWorkflowExecution()
		require.NoError(t, err)
		wfInfo = dweResponse.WorkflowExecutionInfo
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, wfInfo.GetStatus())
		require.NotNil(t, wfInfo.CloseTime)
		require.NotNil(t, wfInfo.ExecutionDuration)
		require.Equal(t,
			wfInfo.GetCloseTime().AsTime().Sub(wfInfo.ExecutionTime.AsTime()),
			wfInfo.ExecutionDuration.AsDuration(),
		)
		require.Equal(t, int64(11), wfInfo.HistoryLength) // WorkflowTaskStarted, WorkflowTaskCompleted, WorkflowCompleted
	})

	t.Run("DescribeTaskQueue", func(t *testing.T) {
		s := testcore.NewEnv(t)

		workflowID := s.Tv().WorkflowID()
		wt := s.Tv().WorkflowType().Name
		tl := s.Tv().TaskQueue().Name
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
		require.NoError(t, err0)

		s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

		// workflow logic
		activityScheduled := false
		activityData := int32(1)
		wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
			if !activityScheduled {
				activityScheduled = true
				buf := new(bytes.Buffer)
				require.Nil(t, binary.Write(buf, binary.LittleEndian, activityData))

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
			T:                   t,
		}

		// this function poll events from history side
		testDescribeTaskQueue := func(namespace string, taskqueue *taskqueuepb.TaskQueue, taskqueueType enumspb.TaskQueueType) []*taskqueuepb.PollerInfo {
			responseInner, errInner := s.FrontendClient().DescribeTaskQueue(testcore.NewContext(), &workflowservice.DescribeTaskQueueRequest{
				Namespace:     namespace,
				TaskQueue:     taskqueue,
				TaskQueueType: taskqueueType,
			})

			require.NoError(t, errInner)
			return responseInner.Pollers
		}

		before := time.Now().UTC()

		// when no one polling on the taskqueue (activity or workflow), there shall be no poller information
		tq := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
		pollerInfos := testDescribeTaskQueue(s.Namespace().String(), tq, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
		require.Empty(t, pollerInfos)
		pollerInfos = testDescribeTaskQueue(s.Namespace().String(), tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		require.Empty(t, pollerInfos)

		_, errWorkflowTask := poller.PollAndProcessWorkflowTask()
		require.NoError(t, errWorkflowTask)
		pollerInfos = testDescribeTaskQueue(s.Namespace().String(), tq, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
		require.Empty(t, pollerInfos)
		pollerInfos = testDescribeTaskQueue(s.Namespace().String(), tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		require.Equal(t, 1, len(pollerInfos))
		require.Equal(t, identity, pollerInfos[0].GetIdentity())
		require.True(t, pollerInfos[0].GetLastAccessTime().AsTime().After(before))
		require.NotEmpty(t, pollerInfos[0].GetLastAccessTime())

		errActivity := poller.PollAndProcessActivityTask(false)
		require.NoError(t, errActivity)
		pollerInfos = testDescribeTaskQueue(s.Namespace().String(), tq, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
		require.Equal(t, 1, len(pollerInfos))
		require.Equal(t, identity, pollerInfos[0].GetIdentity())
		require.True(t, pollerInfos[0].GetLastAccessTime().AsTime().After(before))
		require.NotEmpty(t, pollerInfos[0].GetLastAccessTime())
		pollerInfos = testDescribeTaskQueue(s.Namespace().String(), tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		require.Equal(t, 1, len(pollerInfos))
		require.Equal(t, identity, pollerInfos[0].GetIdentity())
		require.True(t, pollerInfos[0].GetLastAccessTime().AsTime().After(before))
		require.NotEmpty(t, pollerInfos[0].GetLastAccessTime())
	})
}
