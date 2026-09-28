package matching

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errForwarderSlowDown = errors.New("limit exceeded")

type ForwarderTestSuite struct {
	suite.Suite

	controller *gomock.Controller
	client     *matchingservicemock.MockMatchingServiceClient
	fwdr       *priForwarder
	cfg        *forwarderConfig
	partition  *tqid.NormalPartition
}

func TestPriorityForwarderSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &ForwarderTestSuite{})
}

func (t *ForwarderTestSuite) SetupTest() {
	t.controller = gomock.NewController(t.T())
	t.client = matchingservicemock.NewMockMatchingServiceClient(t.controller)
	t.cfg = &forwarderConfig{
		ForwarderMaxOutstandingPolls: func() int { return 1 },
		ForwarderMaxRatePerSecond:    func() float64 { return 2 },
		ForwarderMaxChildrenPerNode:  func() int { return 20 },
		ForwarderMaxOutstandingTasks: func() int { return 1 },
	}

	tqFam, err := tqid.NewTaskQueueFamily("fwdr", "tl0")
	t.NoError(err)
	t.partition = tqFam.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition()

	t.fwdr, err = newPriForwarder(t.cfg, UnversionedQueueKey(t.partition), t.client, testhooks.TestHooks{})
	t.NoError(err)
}

func (t *ForwarderTestSuite) TearDownTest() {
	t.controller.Finish()
}

func (t *ForwarderTestSuite) TestForwardTaskError() {
	task := newInternalTaskFromBacklog(&persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{},
	}, nil)
	t.Equal(tqid.ErrNoParent, t.fwdr.ForwardTask(context.Background(), task))
}

func (t *ForwarderTestSuite) TestForwardWorkflowTask() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_WORKFLOW)

	var request *matchingservice.AddWorkflowTaskRequest
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddWorkflowTaskRequest, arg2 ...any) {
			request = arg1
		},
	).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	taskInfo := randomTaskInfo()
	task := newInternalTaskFromBacklog(taskInfo, nil)
	t.NoError(t.fwdr.ForwardTask(context.Background(), task))
	t.NotNil(request)
	t.Equal(mustParent(t.partition, 20).RpcName(), request.TaskQueue.GetName())
	t.Equal(t.partition.Kind(), request.TaskQueue.GetKind())
	t.Equal(taskInfo.Data.GetNamespaceId(), request.GetNamespaceId())
	t.Equal(taskInfo.Data.GetWorkflowId(), request.GetExecution().GetWorkflowId())
	t.Equal(taskInfo.Data.GetRunId(), request.GetExecution().GetRunId())
	t.Equal(taskInfo.Data.GetScheduledEventId(), request.GetScheduledEventId())

	schedToStart := int32(request.GetScheduleToStartTimeout().AsDuration().Seconds())
	rewritten := convert.Int32Ceil(time.Until(taskInfo.Data.ExpiryTime.AsTime()).Seconds())
	t.Equal(schedToStart, rewritten)
	t.Equal(t.partition.RpcName(), request.GetForwardInfo().GetSourcePartition())
	t.Equal(enumsspb.TASK_SOURCE_DB_BACKLOG, request.GetForwardInfo().GetTaskSource())
}

func (t *ForwarderTestSuite) TestForwardWorkflowTask_WithBuildId() {
	bld := "my-bld"
	t.usingBuildIdQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW, bld)

	var request *matchingservice.AddWorkflowTaskRequest
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddWorkflowTaskRequest, arg2 ...any) {
			request = arg1
			t.Equal(bld, request.GetForwardInfo().GetDispatchBuildId())
		},
	).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	taskInfo := randomTaskInfo()
	task := newInternalTaskForSyncMatch(taskInfo.Data, nil, 0, nil)
	t.NoError(t.fwdr.ForwardTask(context.Background(), task))
	t.NotNil(request)
	t.Equal(mustParent(t.partition, 20).RpcName(), request.TaskQueue.GetName())
	t.Equal(t.partition.Kind(), request.TaskQueue.GetKind())
	t.Equal(taskInfo.Data.GetNamespaceId(), request.GetNamespaceId())
	t.Equal(taskInfo.Data.GetWorkflowId(), request.GetExecution().GetWorkflowId())
	t.Equal(taskInfo.Data.GetRunId(), request.GetExecution().GetRunId())
	t.Equal(taskInfo.Data.GetScheduledEventId(), request.GetScheduledEventId())

	schedToStart := int32(request.GetScheduleToStartTimeout().AsDuration().Seconds())
	rewritten := convert.Int32Ceil(time.Until(taskInfo.Data.ExpiryTime.AsTime()).Seconds())
	t.Equal(schedToStart, rewritten)
	t.Equal(t.partition.RpcName(), request.GetForwardInfo().GetSourcePartition())
	t.Equal(enumsspb.TASK_SOURCE_HISTORY, request.GetForwardInfo().GetTaskSource())
}

func (t *ForwarderTestSuite) TestForwardActivityTask() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	var request *matchingservice.AddActivityTaskRequest
	t.client.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddActivityTaskRequest, arg2 ...any) {
			request = arg1
		},
	).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	taskInfo := randomTaskInfo()
	task := newInternalTaskFromBacklog(taskInfo, nil)
	t.NoError(t.fwdr.ForwardTask(context.Background(), task))
	t.NotNil(request)
	t.Equal(mustParent(t.partition, 20).RpcName(), request.TaskQueue.GetName())
	t.Equal(t.partition.Kind(), request.TaskQueue.GetKind())
	t.Equal(taskInfo.Data.GetNamespaceId(), request.GetNamespaceId())
	t.Equal(taskInfo.Data.GetWorkflowId(), request.GetExecution().GetWorkflowId())
	t.Equal(taskInfo.Data.GetRunId(), request.GetExecution().GetRunId())
	t.Equal(taskInfo.Data.GetScheduledEventId(), request.GetScheduledEventId())
	t.Equal(convert.Int32Ceil(time.Until(taskInfo.Data.ExpiryTime.AsTime()).Seconds()),
		int32(request.GetScheduleToStartTimeout().AsDuration().Seconds()))
	t.Equal(t.partition.RpcName(), request.GetForwardInfo().GetSourcePartition())
	t.Equal(enumsspb.TASK_SOURCE_DB_BACKLOG, request.GetForwardInfo().GetTaskSource())
}

func (t *ForwarderTestSuite) TestForwardActivityTask_WithBuildId() {
	bld := "my-bld"
	t.usingBuildIdQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY, bld)

	var request *matchingservice.AddActivityTaskRequest
	t.client.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddActivityTaskRequest, arg2 ...any) {
			request = arg1
			t.Equal(bld, request.ForwardInfo.GetDispatchBuildId())
		},
	).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	taskInfo := randomTaskInfo()
	task := newInternalTaskFromBacklog(taskInfo, nil)
	t.NoError(t.fwdr.ForwardTask(context.Background(), task))
	t.NotNil(request)
	t.Equal(mustParent(t.partition, 20).RpcName(), request.TaskQueue.GetName())
	t.Equal(t.partition.Kind(), request.TaskQueue.GetKind())
	t.Equal(taskInfo.Data.GetNamespaceId(), request.GetNamespaceId())
	t.Equal(taskInfo.Data.GetWorkflowId(), request.GetExecution().GetWorkflowId())
	t.Equal(taskInfo.Data.GetRunId(), request.GetExecution().GetRunId())
	t.Equal(taskInfo.Data.GetScheduledEventId(), request.GetScheduledEventId())
	t.Equal(convert.Int32Ceil(time.Until(taskInfo.Data.ExpiryTime.AsTime()).Seconds()),
		int32(request.GetScheduleToStartTimeout().AsDuration().Seconds()))
	t.Equal(t.partition.RpcName(), request.GetForwardInfo().GetSourcePartition())
	t.Equal(enumsspb.TASK_SOURCE_DB_BACKLOG, request.GetForwardInfo().GetTaskSource())
}

func (t *ForwarderTestSuite) TestForwardTaskRateExceeded() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	rps := 2
	t.client.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.AddActivityTaskResponse{}, nil).Times(rps)
	taskInfo := randomTaskInfo()
	task := newInternalTaskFromBacklog(taskInfo, nil)
	for range rps {
		t.NoError(t.fwdr.ForwardTask(context.Background(), task))
	}
	t.Equal(errForwarderSlowDown, t.fwdr.ForwardTask(context.Background(), task))
}

func (t *ForwarderTestSuite) TestForwardQueryTaskError() {
	task := newInternalQueryTask("id1", &matchingservice.QueryWorkflowRequest{})
	_, err := t.fwdr.ForwardQueryTask(context.Background(), task)
	t.Equal(tqid.ErrNoParent, err)
}

func (t *ForwarderTestSuite) TestForwardQueryTask() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	task := newInternalQueryTask("id1", &matchingservice.QueryWorkflowRequest{})
	resp := &matchingservice.QueryWorkflowResponse{}
	var request *matchingservice.QueryWorkflowRequest
	t.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.QueryWorkflowRequest, arg2 ...any) {
			request = arg1
		},
	).Return(resp, nil)

	gotResp, err := t.fwdr.ForwardQueryTask(context.Background(), task)
	t.NoError(err)
	t.Equal(mustParent(t.partition, 20).RpcName(), request.TaskQueue.GetName())
	t.Equal(t.partition.Kind(), request.TaskQueue.GetKind())
	t.Equal(task.query.request.QueryRequest, request.QueryRequest)
	t.Equal(resp, gotResp)
	t.Equal(enumsspb.TASK_SOURCE_HISTORY, request.GetForwardInfo().GetTaskSource())
}

func (t *ForwarderTestSuite) TestForwardQueryTaskRateNotEnforced() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	task := newInternalQueryTask("id1", &matchingservice.QueryWorkflowRequest{})
	resp := &matchingservice.QueryWorkflowResponse{}
	rps := 2
	t.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(resp, nil).Times(rps + 1)
	for range rps {
		_, err := t.fwdr.ForwardQueryTask(context.Background(), task)
		t.NoError(err)
	}
	_, err := t.fwdr.ForwardQueryTask(context.Background(), task)
	t.NoError(err) // no rate limiting should be enforced for query task
}

func (t *ForwarderTestSuite) TestForwardPollError() {
	_, err := t.fwdr.ForwardPoll(context.Background(), &pollMetadata{})
	t.Equal(tqid.ErrNoParent, err)
}

func (t *ForwarderTestSuite) TestForwardPollWorkflowTaskQueue() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_WORKFLOW)

	pollerID := uuid.NewString()
	ctx := context.WithValue(context.Background(), pollerIDKey, pollerID)
	ctx = context.WithValue(ctx, identityKey, "id1")
	resp := &matchingservice.PollWorkflowTaskQueueResponse{
		TaskToken: []byte("token1"),
	}

	var request *matchingservice.PollWorkflowTaskQueueRequest
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...any) {
			request = arg1
		},
	).Return(resp, nil)

	task, err := t.fwdr.ForwardPoll(ctx, &pollMetadata{})
	t.NoError(err)
	t.NotNil(task)
	t.NotNil(request)
	t.Equal(pollerID, request.GetPollerId())
	t.Equal(t.partition.TaskQueue().NamespaceId(), request.GetNamespaceId())
	t.Equal("id1", request.GetPollRequest().GetIdentity())
	t.Equal(mustParent(t.partition, 20).RpcName(), request.GetPollRequest().GetTaskQueue().GetName())
	t.Equal(t.partition.Kind(), request.GetPollRequest().GetTaskQueue().GetKind())
	t.Equal(resp, task.pollWorkflowTaskQueueResponse())
	t.Nil(task.pollActivityTaskQueueResponse())
}

func (t *ForwarderTestSuite) TestForwardPollWorkflowTaskQueuePreservesWorkerInstanceKey() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_WORKFLOW)

	pollerID := uuid.NewString()
	workerInstanceKey := "test-worker-instance-" + uuid.NewString()
	ctx := context.WithValue(context.Background(), pollerIDKey, pollerID)
	ctx = context.WithValue(ctx, identityKey, "id1")
	resp := &matchingservice.PollWorkflowTaskQueueResponse{
		TaskToken: []byte("token1"),
	}

	var request *matchingservice.PollWorkflowTaskQueueRequest
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...any) {
			request = arg1
		},
	).Return(resp, nil)

	task, err := t.fwdr.ForwardPoll(ctx, &pollMetadata{
		workerInstanceKey: workerInstanceKey,
	})
	t.Require().NoError(err)
	t.NotNil(task)
	t.NotNil(request)
	t.Equal(workerInstanceKey, request.GetPollRequest().GetWorkerInstanceKey(),
		"WorkerInstanceKey should be preserved when forwarding workflow poll")
}

func (t *ForwarderTestSuite) TestForwardPollForActivity() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	pollerID := uuid.NewString()
	ctx := context.WithValue(context.Background(), pollerIDKey, pollerID)
	ctx = context.WithValue(ctx, identityKey, "id1")
	resp := &matchingservice.PollActivityTaskQueueResponse{
		TaskToken: []byte("token1"),
	}

	var request *matchingservice.PollActivityTaskQueueRequest
	t.client.EXPECT().PollActivityTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollActivityTaskQueueRequest, arg2 ...any) {
			request = arg1
		},
	).Return(resp, nil)

	task, err := t.fwdr.ForwardPoll(ctx, &pollMetadata{})
	t.Require().NoError(err)
	t.NotNil(task)
	t.NotNil(request)
	t.Equal(pollerID, request.GetPollerId())
	t.Equal(t.partition.TaskQueue().NamespaceId(), request.GetNamespaceId())
	t.Equal("id1", request.GetPollRequest().GetIdentity())
	t.Equal(mustParent(t.partition, 20).RpcName(), request.GetPollRequest().GetTaskQueue().GetName())
	t.Equal(t.partition.Kind(), request.GetPollRequest().GetTaskQueue().GetKind())
	t.Equal(resp, task.pollActivityTaskQueueResponse())
	t.Nil(task.pollWorkflowTaskQueueResponse())
}

func (t *ForwarderTestSuite) TestForwardPollForActivityPreservesWorkerInstanceKey() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	pollerID := uuid.NewString()
	workerInstanceKey := "test-worker-instance-" + uuid.NewString()
	ctx := context.WithValue(context.Background(), pollerIDKey, pollerID)
	ctx = context.WithValue(ctx, identityKey, "id1")
	resp := &matchingservice.PollActivityTaskQueueResponse{
		TaskToken: []byte("token1"),
	}

	var request *matchingservice.PollActivityTaskQueueRequest
	t.client.EXPECT().PollActivityTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollActivityTaskQueueRequest, arg2 ...any) {
			request = arg1
		},
	).Return(resp, nil)

	task, err := t.fwdr.ForwardPoll(ctx, &pollMetadata{
		workerInstanceKey: workerInstanceKey,
	})
	t.Require().NoError(err)
	t.NotNil(task)
	t.NotNil(request)
	t.Equal(workerInstanceKey, request.GetPollRequest().GetWorkerInstanceKey(),
		"WorkerInstanceKey should be preserved when forwarding activity poll")
}

func (t *ForwarderTestSuite) TestForwardPollForNexusPreservesWorkerInstanceKey() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_NEXUS)

	pollerID := uuid.NewString()
	workerInstanceKey := "test-worker-instance-" + uuid.NewString()
	ctx := context.WithValue(context.Background(), pollerIDKey, pollerID)
	ctx = context.WithValue(ctx, identityKey, "id1")
	resp := &matchingservice.PollNexusTaskQueueResponse{
		Response: &workflowservice.PollNexusTaskQueueResponse{
			TaskToken: []byte("token1"),
		},
	}

	var request *matchingservice.PollNexusTaskQueueRequest
	t.client.EXPECT().PollNexusTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollNexusTaskQueueRequest, arg2 ...any) {
			request = arg1
		},
	).Return(resp, nil)

	task, err := t.fwdr.ForwardPoll(ctx, &pollMetadata{
		workerInstanceKey: workerInstanceKey,
	})
	t.Require().NoError(err)
	t.NotNil(task)
	t.NotNil(request)
	t.Equal(workerInstanceKey, request.GetRequest().GetWorkerInstanceKey(),
		"WorkerInstanceKey should be preserved when forwarding Nexus poll")
}

func (t *ForwarderTestSuite) usingTaskqueuePartition(taskType enumspb.TaskQueueType) {
	f, err := tqid.NewTaskQueueFamily("fwdr", "tl0")
	t.NoError(err)
	t.partition = f.TaskQueue(taskType).NormalPartition(1)
	t.fwdr, err = newPriForwarder(t.cfg, UnversionedQueueKey(t.partition), t.client, testhooks.TestHooks{})
	t.NoError(err)
}

func (t *ForwarderTestSuite) usingBuildIdQueue(taskType enumspb.TaskQueueType, buildId string) {
	f, err := tqid.NewTaskQueueFamily("fwdr", "tl0")
	t.NoError(err)
	t.partition = f.TaskQueue(taskType).NormalPartition(1)
	t.fwdr, err = newPriForwarder(t.cfg, BuildIdQueueKey(t.partition, buildId), t.client, testhooks.TestHooks{})
	t.NoError(err)
}

func mustParent(tn *tqid.NormalPartition, n int) *tqid.NormalPartition {
	parent, err := tn.ParentPartition(n)
	if err != nil {
		panic(err)
	}
	return parent
}

func randomTaskInfo() *persistencespb.AllocatedTaskInfo {
	rt1 := time.Date(rand.Intn(9999), time.Month(rand.Intn(12)+1), rand.Intn(28)+1, rand.Intn(24)+1, rand.Intn(60), rand.Intn(60), rand.Intn(1e9), time.UTC)
	rt2 := time.Date(rand.Intn(5000)+3000, time.Month(rand.Intn(12)+1), rand.Intn(28)+1, rand.Intn(24)+1, rand.Intn(60), rand.Intn(60), rand.Intn(1e9), time.UTC)

	return &persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{
			NamespaceId:      uuid.NewString(),
			WorkflowId:       uuid.NewString(),
			RunId:            uuid.NewString(),
			ScheduledEventId: rand.Int63(),
			CreateTime:       timestamppb.New(rt1),
			ExpiryTime:       timestamppb.New(rt2),
		},
		TaskId: rand.Int63(),
	}
}
