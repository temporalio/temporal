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

	"github.com/pborman/uuid"
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

func (s *ForwarderTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.client = matchingservicemock.NewMockMatchingServiceClient(s.controller)
	s.cfg = &forwarderConfig{
		ForwarderMaxOutstandingPolls: func() int { return 1 },
		ForwarderMaxRatePerSecond:    func() float64 { return 2 },
		ForwarderMaxChildrenPerNode:  func() int { return 20 },
		ForwarderMaxOutstandingTasks: func() int { return 1 },
	}

	tqFam, err := tqid.NewTaskQueueFamily("fwdr", "tl0")
	s.NoError(err)
	s.partition = tqFam.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition()

	if s.newFwdr {
		s.fwdr, err = newPriForwarder(s.cfg, UnversionedQueueKey(s.partition), s.client)
		s.NoError(err)
	} else {
		s.fwdr, err = newForwarder(s.cfg, UnversionedQueueKey(s.partition), s.client)
		s.NoError(err)
	}
}

func (s *ForwarderTestSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *ForwarderTestSuite) TestForwardTaskError() {
	task := newInternalTaskFromBacklog(&persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{},
	}, nil)
	s.Equal(tqid.ErrNoParent, s.fwdr.ForwardTask(context.Background(), task))
}

func (s *ForwarderTestSuite) TestForwardWorkflowTask() {
	s.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_WORKFLOW)

	var request *matchingservice.AddWorkflowTaskRequest
	s.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddWorkflowTaskRequest, arg2 ...interface{}) {
			request = arg1
		},
	).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	taskInfo := randomTaskInfo()
	task := newInternalTaskFromBacklog(taskInfo, nil)
	s.NoError(s.fwdr.ForwardTask(context.Background(), task))
	s.NotNil(request)
	s.Equal(mustParent(s.partition, 20).RpcName(), request.TaskQueue.GetName())
	s.Equal(s.partition.Kind(), request.TaskQueue.GetKind())
	s.Equal(taskInfo.Data.GetNamespaceId(), request.GetNamespaceId())
	s.Equal(taskInfo.Data.GetWorkflowId(), request.GetExecution().GetWorkflowId())
	s.Equal(taskInfo.Data.GetRunId(), request.GetExecution().GetRunId())
	s.Equal(taskInfo.Data.GetScheduledEventId(), request.GetScheduledEventId())

	schedToStart := int32(request.GetScheduleToStartTimeout().AsDuration().Seconds())
	rewritten := convert.Int32Ceil(time.Until(taskInfo.Data.ExpiryTime.AsTime()).Seconds())
	s.EqualValues(schedToStart, rewritten)
	s.Equal(s.partition.RpcName(), request.GetForwardInfo().GetSourcePartition())
	s.Equal(enumsspb.TASK_SOURCE_DB_BACKLOG, request.GetForwardInfo().GetTaskSource())
}

func (s *ForwarderTestSuite) TestForwardWorkflowTask_WithBuildId() {
	bld := "my-bld"
	s.usingBuildIdQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW, bld)

	var request *matchingservice.AddWorkflowTaskRequest
	s.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddWorkflowTaskRequest, arg2 ...interface{}) {
			request = arg1
			s.Equal(bld, request.GetForwardInfo().GetDispatchBuildId())
		},
	).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	taskInfo := randomTaskInfo()
	task := newInternalTaskForSyncMatch(taskInfo.Data, nil)
	s.NoError(s.fwdr.ForwardTask(context.Background(), task))
	s.NotNil(request)
	s.Equal(mustParent(s.partition, 20).RpcName(), request.TaskQueue.GetName())
	s.Equal(s.partition.Kind(), request.TaskQueue.GetKind())
	s.Equal(taskInfo.Data.GetNamespaceId(), request.GetNamespaceId())
	s.Equal(taskInfo.Data.GetWorkflowId(), request.GetExecution().GetWorkflowId())
	s.Equal(taskInfo.Data.GetRunId(), request.GetExecution().GetRunId())
	s.Equal(taskInfo.Data.GetScheduledEventId(), request.GetScheduledEventId())

	schedToStart := int32(request.GetScheduleToStartTimeout().AsDuration().Seconds())
	rewritten := convert.Int32Ceil(time.Until(taskInfo.Data.ExpiryTime.AsTime()).Seconds())
	s.EqualValues(schedToStart, rewritten)
	s.Equal(s.partition.RpcName(), request.GetForwardInfo().GetSourcePartition())
	s.Equal(enumsspb.TASK_SOURCE_HISTORY, request.GetForwardInfo().GetTaskSource())
}

func (s *ForwarderTestSuite) TestForwardActivityTask() {
	s.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	var request *matchingservice.AddActivityTaskRequest
	s.client.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddActivityTaskRequest, arg2 ...interface{}) {
			request = arg1
		},
	).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	taskInfo := randomTaskInfo()
	task := newInternalTaskFromBacklog(taskInfo, nil)
	s.NoError(s.fwdr.ForwardTask(context.Background(), task))
	s.NotNil(request)
	s.Equal(mustParent(s.partition, 20).RpcName(), request.TaskQueue.GetName())
	s.Equal(s.partition.Kind(), request.TaskQueue.GetKind())
	s.Equal(taskInfo.Data.GetNamespaceId(), request.GetNamespaceId())
	s.Equal(taskInfo.Data.GetWorkflowId(), request.GetExecution().GetWorkflowId())
	s.Equal(taskInfo.Data.GetRunId(), request.GetExecution().GetRunId())
	s.Equal(taskInfo.Data.GetScheduledEventId(), request.GetScheduledEventId())
	s.EqualValues(convert.Int32Ceil(time.Until(taskInfo.Data.ExpiryTime.AsTime()).Seconds()),
		int32(request.GetScheduleToStartTimeout().AsDuration().Seconds()))
	s.Equal(s.partition.RpcName(), request.GetForwardInfo().GetSourcePartition())
	s.Equal(enumsspb.TASK_SOURCE_DB_BACKLOG, request.GetForwardInfo().GetTaskSource())
}

func (s *ForwarderTestSuite) TestForwardActivityTask_WithBuildId() {
	bld := "my-bld"
	s.usingBuildIdQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY, bld)

	var request *matchingservice.AddActivityTaskRequest
	s.client.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddActivityTaskRequest, arg2 ...interface{}) {
			request = arg1
			s.Equal(bld, request.ForwardInfo.GetDispatchBuildId())
		},
	).Return(&matchingservice.AddActivityTaskResponse{}, nil)

	taskInfo := randomTaskInfo()
	task := newInternalTaskFromBacklog(taskInfo, nil)
	s.NoError(s.fwdr.ForwardTask(context.Background(), task))
	s.NotNil(request)
	s.Equal(mustParent(s.partition, 20).RpcName(), request.TaskQueue.GetName())
	s.Equal(s.partition.Kind(), request.TaskQueue.GetKind())
	s.Equal(taskInfo.Data.GetNamespaceId(), request.GetNamespaceId())
	s.Equal(taskInfo.Data.GetWorkflowId(), request.GetExecution().GetWorkflowId())
	s.Equal(taskInfo.Data.GetRunId(), request.GetExecution().GetRunId())
	s.Equal(taskInfo.Data.GetScheduledEventId(), request.GetScheduledEventId())
	s.EqualValues(convert.Int32Ceil(time.Until(taskInfo.Data.ExpiryTime.AsTime()).Seconds()),
		int32(request.GetScheduleToStartTimeout().AsDuration().Seconds()))
	s.Equal(s.partition.RpcName(), request.GetForwardInfo().GetSourcePartition())
	s.Equal(enumsspb.TASK_SOURCE_DB_BACKLOG, request.GetForwardInfo().GetTaskSource())
}

func (s *ForwarderTestSuite) TestForwardTaskRateExceeded() {
	s.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	rps := 2
	s.client.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.AddActivityTaskResponse{}, nil).Times(rps)
	taskInfo := randomTaskInfo()
	task := newInternalTaskFromBacklog(taskInfo, nil)
	for i := 0; i < rps; i++ {
		s.NoError(s.fwdr.ForwardTask(context.Background(), task))
	}
	s.Equal(errForwarderSlowDown, s.fwdr.ForwardTask(context.Background(), task))
}

func (s *ForwarderTestSuite) TestForwardQueryTaskError() {
	task := newInternalQueryTask("id1", &matchingservice.QueryWorkflowRequest{})
	_, err := s.fwdr.ForwardQueryTask(context.Background(), task)
	s.Equal(tqid.ErrNoParent, err)
}

func (s *ForwarderTestSuite) TestForwardQueryTask() {
	s.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	task := newInternalQueryTask("id1", &matchingservice.QueryWorkflowRequest{})
	resp := &matchingservice.QueryWorkflowResponse{}
	var request *matchingservice.QueryWorkflowRequest
	s.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.QueryWorkflowRequest, arg2 ...interface{}) {
			request = arg1
		},
	).Return(resp, nil)

	gotResp, err := s.fwdr.ForwardQueryTask(context.Background(), task)
	s.NoError(err)
	s.Equal(mustParent(s.partition, 20).RpcName(), request.TaskQueue.GetName())
	s.Equal(s.partition.Kind(), request.TaskQueue.GetKind())
	s.Equal(task.query.request.QueryRequest, request.QueryRequest)
	s.Equal(resp, gotResp)
	s.Equal(enumsspb.TASK_SOURCE_HISTORY, request.GetForwardInfo().GetTaskSource())
}

func (s *ForwarderTestSuite) TestForwardQueryTaskRateNotEnforced() {
	s.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	task := newInternalQueryTask("id1", &matchingservice.QueryWorkflowRequest{})
	resp := &matchingservice.QueryWorkflowResponse{}
	rps := 2
	s.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(resp, nil).Times(rps + 1)
	for i := 0; i < rps; i++ {
		_, err := s.fwdr.ForwardQueryTask(context.Background(), task)
		s.NoError(err)
	}
	_, err := s.fwdr.ForwardQueryTask(context.Background(), task)
	s.NoError(err) // no rate limiting should be enforced for query task
}

func (s *ForwarderTestSuite) TestForwardPollError() {
	_, err := s.fwdr.ForwardPoll(context.Background(), &pollMetadata{})
	s.Equal(tqid.ErrNoParent, err)
}

func (s *ForwarderTestSuite) TestForwardPollWorkflowTaskQueue() {
	s.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_WORKFLOW)

	pollerID := uuid.New()
	ctx := context.WithValue(context.Background(), pollerIDKey, pollerID)
	ctx = context.WithValue(ctx, identityKey, "id1")
	resp := &matchingservice.PollWorkflowTaskQueueResponse{}

	var request *matchingservice.PollWorkflowTaskQueueRequest
	s.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
			request = arg1
		},
	).Return(resp, nil)

	task, err := s.fwdr.ForwardPoll(ctx, &pollMetadata{})
	s.NoError(err)
	s.NotNil(task)
	s.NotNil(request)
	s.Equal(pollerID, request.GetPollerId())
	s.Equal(s.partition.TaskQueue().NamespaceId(), request.GetNamespaceId())
	s.Equal("id1", request.GetPollRequest().GetIdentity())
	s.Equal(mustParent(s.partition, 20).RpcName(), request.GetPollRequest().GetTaskQueue().GetName())
	s.Equal(s.partition.Kind(), request.GetPollRequest().GetTaskQueue().GetKind())
	s.Equal(resp, task.pollWorkflowTaskQueueResponse())
	s.Nil(task.pollActivityTaskQueueResponse())
}

func (s *ForwarderTestSuite) TestForwardPollForActivity() {
	s.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	pollerID := uuid.New()
	ctx := context.WithValue(context.Background(), pollerIDKey, pollerID)
	ctx = context.WithValue(ctx, identityKey, "id1")
	resp := &matchingservice.PollActivityTaskQueueResponse{}

	var request *matchingservice.PollActivityTaskQueueRequest
	s.client.EXPECT().PollActivityTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollActivityTaskQueueRequest, arg2 ...interface{}) {
			request = arg1
		},
	).Return(resp, nil)

	task, err := s.fwdr.ForwardPoll(ctx, &pollMetadata{})
	s.NoError(err)
	s.NotNil(task)
	s.NotNil(request)
	s.Equal(pollerID, request.GetPollerId())
	s.Equal(s.partition.TaskQueue().NamespaceId(), request.GetNamespaceId())
	s.Equal("id1", request.GetPollRequest().GetIdentity())
	s.Equal(mustParent(s.partition, 20).RpcName(), request.GetPollRequest().GetTaskQueue().GetName())
	s.Equal(s.partition.Kind(), request.GetPollRequest().GetTaskQueue().GetKind())
	s.Equal(resp, task.pollActivityTaskQueueResponse())
	s.Nil(task.pollWorkflowTaskQueueResponse())
}

func (s *ForwarderTestSuite) TestMaxOutstandingConcurrency() {
	if s.newFwdr {
		s.T().Skip("priority forwarder is not compatible with this test")
	}
	fwdr := s.fwdr.(*Forwarder)

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
		s.Run(tc.name, func() {
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
			s.True(common.AwaitWaitGroup(&wg, time.Second))
			s.Equal(tc.output, adds)
			s.Equal(tc.output, polls)
		})
	}
}

func (s *ForwarderTestSuite) TestMaxOutstandingConfigUpdate() {
	if s.newFwdr {
		s.T().Skip("priority forwarder is not compatible with this test")
	}
	fwdr := s.fwdr.(*Forwarder)

	maxOutstandingTasks := int32(1)
	maxOutstandingPolls := int32(1)
	s.cfg.ForwarderMaxOutstandingTasks = func() int { return int(atomic.LoadInt32(&maxOutstandingTasks)) }
	s.cfg.ForwarderMaxOutstandingPolls = func() int { return int(atomic.LoadInt32(&maxOutstandingPolls)) }

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
	s.True(common.AwaitWaitGroup(&doneWG, time.Second))

	s.Equal(10, cap(fwdr.addReqToken.Load().(*ForwarderReqToken).ch))
	s.Equal(10, cap(fwdr.pollReqToken.Load().(*ForwarderReqToken).ch))
}

func (s *ForwarderTestSuite) usingTaskqueuePartition(taskType enumspb.TaskQueueType) {
	f, err := tqid.NewTaskQueueFamily("fwdr", "tl0")
	s.NoError(err)
	s.partition = f.TaskQueue(taskType).NormalPartition(1)
	s.fwdr, err = newForwarder(s.cfg, UnversionedQueueKey(s.partition), s.client)
	s.Nil(err)
}

func (s *ForwarderTestSuite) usingBuildIdQueue(taskType enumspb.TaskQueueType, buildId string) {
	f, err := tqid.NewTaskQueueFamily("fwdr", "tl0")
	s.NoError(err)
	s.partition = f.TaskQueue(taskType).NormalPartition(1)
	s.fwdr, err = newForwarder(s.cfg, BuildIdQueueKey(s.partition, buildId), s.client)
	s.Nil(err)
}

func mustParent(tn *tqid.NormalPartition, n int) *tqid.NormalPartition {
	parent, err := tn.ParentPartition(n)
	if err != nil {
		panic(err)
	}
	return parent
}
