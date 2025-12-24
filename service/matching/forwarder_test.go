package matching

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
)

type ForwarderTestSuite struct {
	suite.Suite

	newFwdr    bool
	controller *gomock.Controller
	client     *matchingservicemock.MockMatchingServiceClient
	fwdr       forwarder
	cfg        *forwarderConfig
	partition  *tqid.NormalPartition
}

type forwarder interface {
	ForwardTask(background context.Context, task *internalTask) error
	ForwardQueryTask(background context.Context, task *internalTask) (*matchingservice.QueryWorkflowResponse, error)
	ForwardPoll(ctx context.Context, p *pollMetadata) (*internalTask, error)
}

// TODO(pri): cleanup; delete this
func TestForwarderSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &ForwarderTestSuite{newFwdr: false})
}

func TestPriorityForwarderSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &ForwarderTestSuite{newFwdr: true})
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

	if t.newFwdr {
		t.fwdr, err = newPriForwarder(t.cfg, UnversionedQueueKey(t.partition), t.client)
		t.NoError(err)
	} else {
		t.fwdr, err = newForwarder(t.cfg, UnversionedQueueKey(t.partition), t.client)
		t.NoError(err)
	}
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
		func(arg0 context.Context, arg1 *matchingservice.AddWorkflowTaskRequest, arg2 ...interface{}) {
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
	t.EqualValues(schedToStart, rewritten)
	t.Equal(t.partition.RpcName(), request.GetForwardInfo().GetSourcePartition())
	t.Equal(enumsspb.TASK_SOURCE_DB_BACKLOG, request.GetForwardInfo().GetTaskSource())
}

func (t *ForwarderTestSuite) TestForwardWorkflowTask_WithBuildId() {
	bld := "my-bld"
	t.usingBuildIdQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW, bld)

	var request *matchingservice.AddWorkflowTaskRequest
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddWorkflowTaskRequest, arg2 ...interface{}) {
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
	t.EqualValues(schedToStart, rewritten)
	t.Equal(t.partition.RpcName(), request.GetForwardInfo().GetSourcePartition())
	t.Equal(enumsspb.TASK_SOURCE_HISTORY, request.GetForwardInfo().GetTaskSource())
}

func (t *ForwarderTestSuite) TestForwardActivityTask() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	var request *matchingservice.AddActivityTaskRequest
	t.client.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddActivityTaskRequest, arg2 ...interface{}) {
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
	t.EqualValues(convert.Int32Ceil(time.Until(taskInfo.Data.ExpiryTime.AsTime()).Seconds()),
		int32(request.GetScheduleToStartTimeout().AsDuration().Seconds()))
	t.Equal(t.partition.RpcName(), request.GetForwardInfo().GetSourcePartition())
	t.Equal(enumsspb.TASK_SOURCE_DB_BACKLOG, request.GetForwardInfo().GetTaskSource())
}

func (t *ForwarderTestSuite) TestForwardActivityTask_WithBuildId() {
	bld := "my-bld"
	t.usingBuildIdQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY, bld)

	var request *matchingservice.AddActivityTaskRequest
	t.client.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddActivityTaskRequest, arg2 ...interface{}) {
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
	t.EqualValues(convert.Int32Ceil(time.Until(taskInfo.Data.ExpiryTime.AsTime()).Seconds()),
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
	for i := 0; i < rps; i++ {
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
		func(arg0 context.Context, arg1 *matchingservice.QueryWorkflowRequest, arg2 ...interface{}) {
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
	for i := 0; i < rps; i++ {
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
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
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
		func(arg0 context.Context, arg1 *matchingservice.PollActivityTaskQueueRequest, arg2 ...interface{}) {
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
	t.Equal(resp, task.pollActivityTaskQueueResponse())
	t.Nil(task.pollWorkflowTaskQueueResponse())
}

// TODO(pri): old matcher cleanup
func (t *ForwarderTestSuite) TestMaxOutstandingConcurrency() {
	if t.newFwdr {
		t.T().Skip("priority forwarder is not compatible with this test")
	}
	fwdr := t.fwdr.(*Forwarder)

	concurrency := 50
	testCases := []struct {
		name          string
		mustLeakToken bool
		output        int32
	}{
		{"contention", false, int32(concurrency)},
		{"token_leak", true, 1},
	}

	var adds int32
	var polls int32
	var wg sync.WaitGroup

	for _, tc := range testCases {
		adds = 0
		polls = 0
		t.Run(tc.name, func() {
			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func() {
					timer := time.NewTimer(time.Millisecond * 200)
					select {
					case token := <-fwdr.AddReqTokenC():
						timer.Stop()
						if !tc.mustLeakToken {
							token.release()
						}
						atomic.AddInt32(&adds, 1)
					case <-timer.C:
					}

					timer = time.NewTimer(time.Millisecond * 200)
					select {
					case token := <-fwdr.PollReqTokenC():
						timer.Stop()
						if !tc.mustLeakToken {
							token.release()
						}
						atomic.AddInt32(&polls, 1)
					case <-timer.C:
					}
					wg.Done()
				}()
			}
			t.True(common.AwaitWaitGroup(&wg, time.Second))
			t.Equal(tc.output, adds)
			t.Equal(tc.output, polls)
		})
	}
}

// TODO(pri): old matcher cleanup
func (t *ForwarderTestSuite) TestMaxOutstandingConfigUpdate() {
	if t.newFwdr {
		t.T().Skip("priority forwarder is not compatible with this test")
	}
	fwdr := t.fwdr.(*Forwarder)

	maxOutstandingTasks := int32(1)
	maxOutstandingPolls := int32(1)
	t.cfg.ForwarderMaxOutstandingTasks = func() int { return int(atomic.LoadInt32(&maxOutstandingTasks)) }
	t.cfg.ForwarderMaxOutstandingPolls = func() int { return int(atomic.LoadInt32(&maxOutstandingPolls)) }

	startC := make(chan struct{})
	doneWG := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		doneWG.Add(1)
		go func() {
			<-startC
			token1 := <-fwdr.AddReqTokenC()
			token1.release()
			token2 := <-fwdr.PollReqTokenC()
			token2.release()
			doneWG.Done()
		}()
	}

	maxOutstandingTasks = 10
	maxOutstandingPolls = 10
	close(startC)
	t.True(common.AwaitWaitGroup(&doneWG, time.Second))

	t.Equal(10, cap(fwdr.addReqToken.Load().(*ForwarderReqToken).ch))
	t.Equal(10, cap(fwdr.pollReqToken.Load().(*ForwarderReqToken).ch))
}

func (t *ForwarderTestSuite) usingTaskqueuePartition(taskType enumspb.TaskQueueType) {
	f, err := tqid.NewTaskQueueFamily("fwdr", "tl0")
	t.NoError(err)
	t.partition = f.TaskQueue(taskType).NormalPartition(1)
	t.fwdr, err = newForwarder(t.cfg, UnversionedQueueKey(t.partition), t.client)
	t.Nil(err)
}

func (t *ForwarderTestSuite) usingBuildIdQueue(taskType enumspb.TaskQueueType, buildId string) {
	f, err := tqid.NewTaskQueueFamily("fwdr", "tl0")
	t.NoError(err)
	t.partition = f.TaskQueue(taskType).NormalPartition(1)
	t.fwdr, err = newForwarder(t.cfg, BuildIdQueueKey(t.partition, buildId), t.client)
	t.Nil(err)
}

func mustParent(tn *tqid.NormalPartition, n int) *tqid.NormalPartition {
	parent, err := tn.ParentPartition(n)
	if err != nil {
		panic(err)
	}
	return parent
}
