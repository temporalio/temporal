// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqid"
)

type ForwarderTestSuite struct {
	suite.Suite
	controller *gomock.Controller
	client     *matchingservicemock.MockMatchingServiceClient
	fwdr       *Forwarder
	cfg        *forwarderConfig
	partition  *tqid.NormalPartition
}

func TestForwarderSuite(t *testing.T) {
	suite.Run(t, new(ForwarderTestSuite))
}

func (t *ForwarderTestSuite) SetupTest() {
	t.controller = gomock.NewController(t.T())
	t.client = matchingservicemock.NewMockMatchingServiceClient(t.controller)
	t.cfg = &forwarderConfig{
		ForwarderMaxOutstandingPolls: func() int { return 1 },
		ForwarderMaxRatePerSecond:    func() int { return 2 },
		ForwarderMaxChildrenPerNode:  func() int { return 20 },
		ForwarderMaxOutstandingTasks: func() int { return 1 },
	}
	f, err := tqid.NewTaskQueueFamily("fwdr", "tl0")
	t.Assert().NoError(err)
	t.partition = f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition()
	t.fwdr, err = newForwarder(t.cfg, UnversionedQueueKey(t.partition), t.client)
	t.Assert().NoError(err)
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
	t.Equal(t.fwdr.partition.Kind(), request.TaskQueue.GetKind())
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
	task := newInternalTaskForSyncMatch(taskInfo.Data, nil)
	t.NoError(t.fwdr.ForwardTask(context.Background(), task))
	t.NotNil(request)
	t.Equal(mustParent(t.partition, 20).RpcName(), request.TaskQueue.GetName())
	t.Equal(t.fwdr.partition.Kind(), request.TaskQueue.GetKind())
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
	t.Equal(t.fwdr.partition.Kind(), request.TaskQueue.GetKind())
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
	t.Equal(t.fwdr.partition.Kind(), request.TaskQueue.GetKind())
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
	t.Equal(t.fwdr.partition.Kind(), request.TaskQueue.GetKind())
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

	pollerID := uuid.New()
	ctx := context.WithValue(context.Background(), pollerIDKey, pollerID)
	ctx = context.WithValue(ctx, identityKey, "id1")
	resp := &matchingservice.PollWorkflowTaskQueueResponse{}

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
	t.Equal(t.partition.TaskQueue().NamespaceId(), namespace.ID(request.GetNamespaceId()))
	t.Equal("id1", request.GetPollRequest().GetIdentity())
	t.Equal(mustParent(t.partition, 20).RpcName(), request.GetPollRequest().GetTaskQueue().GetName())
	t.Equal(t.fwdr.partition.Kind(), request.GetPollRequest().GetTaskQueue().GetKind())
	t.Equal(resp, task.pollWorkflowTaskQueueResponse())
	t.Nil(task.pollActivityTaskQueueResponse())
}

func (t *ForwarderTestSuite) TestForwardPollForActivity() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	pollerID := uuid.New()
	ctx := context.WithValue(context.Background(), pollerIDKey, pollerID)
	ctx = context.WithValue(ctx, identityKey, "id1")
	resp := &matchingservice.PollActivityTaskQueueResponse{}

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
	t.Equal(t.partition.TaskQueue().NamespaceId(), namespace.ID(request.GetNamespaceId()))
	t.Equal("id1", request.GetPollRequest().GetIdentity())
	t.Equal(mustParent(t.partition, 20).RpcName(), request.GetPollRequest().GetTaskQueue().GetName())
	t.Equal(t.fwdr.partition.Kind(), request.GetPollRequest().GetTaskQueue().GetKind())
	t.Equal(resp, task.pollActivityTaskQueueResponse())
	t.Nil(task.pollWorkflowTaskQueueResponse())
}

func (t *ForwarderTestSuite) TestMaxOutstandingConcurrency() {
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
					case token := <-t.fwdr.AddReqTokenC():
						timer.Stop()
						if !tc.mustLeakToken {
							token.release()
						}
						atomic.AddInt32(&adds, 1)
					case <-timer.C:
					}

					timer = time.NewTimer(time.Millisecond * 200)
					select {
					case token := <-t.fwdr.PollReqTokenC():
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

func (t *ForwarderTestSuite) TestMaxOutstandingConfigUpdate() {
	maxOutstandingTasks := int32(1)
	maxOutstandingPolls := int32(1)
	t.fwdr.cfg.ForwarderMaxOutstandingTasks = func() int { return int(atomic.LoadInt32(&maxOutstandingTasks)) }
	t.fwdr.cfg.ForwarderMaxOutstandingPolls = func() int { return int(atomic.LoadInt32(&maxOutstandingPolls)) }

	startC := make(chan struct{})
	doneWG := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		doneWG.Add(1)
		go func() {
			<-startC
			token1 := <-t.fwdr.AddReqTokenC()
			token1.release()
			token2 := <-t.fwdr.PollReqTokenC()
			token2.release()
			doneWG.Done()
		}()
	}

	maxOutstandingTasks = 10
	maxOutstandingPolls = 10
	close(startC)
	t.True(common.AwaitWaitGroup(&doneWG, time.Second))

	t.Equal(10, cap(t.fwdr.addReqToken.Load().(*ForwarderReqToken).ch))
	t.Equal(10, cap(t.fwdr.pollReqToken.Load().(*ForwarderReqToken).ch))
}

func (t *ForwarderTestSuite) usingTaskqueuePartition(taskType enumspb.TaskQueueType) {
	f, err := tqid.NewTaskQueueFamily("fwdr", "tl0")
	t.Assert().NoError(err)
	t.partition = f.TaskQueue(taskType).NormalPartition(1)
	t.fwdr, err = newForwarder(t.cfg, UnversionedQueueKey(t.partition), t.client)
	t.Nil(err)
}

func (t *ForwarderTestSuite) usingBuildIdQueue(taskType enumspb.TaskQueueType, buildId string) {
	f, err := tqid.NewTaskQueueFamily("fwdr", "tl0")
	t.Assert().NoError(err)
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
