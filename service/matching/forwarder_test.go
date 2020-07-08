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
	enumspb "go.temporal.io/temporal-proto/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/primitives/timestamp"
)

type ForwarderTestSuite struct {
	suite.Suite
	controller *gomock.Controller
	client     *matchingservicemock.MockMatchingServiceClient
	fwdr       *Forwarder
	cfg        *forwarderConfig
	taskQueue  *taskQueueID
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
	t.taskQueue = newTestTaskQueueID("fwdr", "tl0", enumspb.TASK_QUEUE_TYPE_DECISION)
	t.fwdr = newForwarder(t.cfg, t.taskQueue, enumspb.TASK_QUEUE_KIND_NORMAL, t.client)
}

func (t *ForwarderTestSuite) TearDownTest() {
	t.controller.Finish()
}

func (t *ForwarderTestSuite) TestForwardTaskError() {
	task := newInternalTask(&persistenceblobs.AllocatedTaskInfo{}, nil, enumsspb.TASK_SOURCE_HISTORY, "", false)
	t.Equal(errNoParent, t.fwdr.ForwardTask(context.Background(), task))

	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	t.fwdr.taskQueueKind = enumspb.TASK_QUEUE_KIND_STICKY
	t.Equal(errTaskQueueKind, t.fwdr.ForwardTask(context.Background(), task))
}

func (t *ForwarderTestSuite) TestForwardDecisionTask() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_DECISION)

	var request *matchingservice.AddDecisionTaskRequest
	t.client.EXPECT().AddDecisionTask(gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddDecisionTaskRequest) {
			request = arg1
		},
	).Return(&matchingservice.AddDecisionTaskResponse{}, nil).Times(1)

	taskInfo := randomTaskInfo()
	task := newInternalTask(taskInfo, nil, enumsspb.TASK_SOURCE_HISTORY, "", false)
	t.NoError(t.fwdr.ForwardTask(context.Background(), task))
	t.NotNil(request)
	t.Equal(t.taskQueue.Parent(20), request.TaskQueue.GetName())
	t.Equal(enumspb.TaskQueueKind(t.fwdr.taskQueueKind), request.TaskQueue.GetKind())
	t.Equal(taskInfo.Data.GetNamespaceId(), request.GetNamespaceId())
	t.Equal(taskInfo.Data.GetWorkflowId(), request.GetExecution().GetWorkflowId())
	t.Equal(taskInfo.Data.GetRunId(), request.GetExecution().GetRunId())
	t.Equal(taskInfo.Data.GetScheduleId(), request.GetScheduleId())

	schedToStart := request.GetScheduleToStartTimeoutSeconds()
	rewritten := convert.Int32Ceil(time.Until(*timestamp.TimestampFromProto(taskInfo.Data.Expiry).ToTime()).Seconds())
	t.Equal(schedToStart, rewritten)
	t.Equal(t.taskQueue.name, request.GetForwardedFrom())
}

func (t *ForwarderTestSuite) TestForwardActivityTask() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	var request *matchingservice.AddActivityTaskRequest
	t.client.EXPECT().AddActivityTask(gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddActivityTaskRequest) {
			request = arg1
		},
	).Return(&matchingservice.AddActivityTaskResponse{}, nil).Times(1)

	taskInfo := randomTaskInfo()
	task := newInternalTask(taskInfo, nil, enumsspb.TASK_SOURCE_HISTORY, "", false)
	t.NoError(t.fwdr.ForwardTask(context.Background(), task))
	t.NotNil(request)
	t.Equal(t.taskQueue.Parent(20), request.TaskQueue.GetName())
	t.Equal(enumspb.TaskQueueKind(t.fwdr.taskQueueKind), request.TaskQueue.GetKind())
	t.Equal(t.taskQueue.namespaceID, request.GetNamespaceId())
	t.Equal(taskInfo.Data.GetNamespaceId(), request.GetSourceNamespaceId())
	t.Equal(taskInfo.Data.GetWorkflowId(), request.GetExecution().GetWorkflowId())
	t.Equal(taskInfo.Data.GetRunId(), request.GetExecution().GetRunId())
	t.Equal(taskInfo.Data.GetScheduleId(), request.GetScheduleId())
	t.EqualValues(convert.Int32Ceil(time.Until(*timestamp.TimestampFromProto(taskInfo.Data.Expiry).ToTime()).Seconds()),
		request.GetScheduleToStartTimeoutSeconds())
	t.Equal(t.taskQueue.name, request.GetForwardedFrom())
}

func (t *ForwarderTestSuite) TestForwardTaskRateExceeded() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	rps := 2
	t.client.EXPECT().AddActivityTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.AddActivityTaskResponse{}, nil).Times(rps)
	taskInfo := randomTaskInfo()
	task := newInternalTask(taskInfo, nil, enumsspb.TASK_SOURCE_HISTORY, "", false)
	for i := 0; i < rps; i++ {
		t.NoError(t.fwdr.ForwardTask(context.Background(), task))
	}
	t.Equal(errForwarderSlowDown, t.fwdr.ForwardTask(context.Background(), task))
}

func (t *ForwarderTestSuite) TestForwardQueryTaskError() {
	task := newInternalQueryTask("id1", &matchingservice.QueryWorkflowRequest{})
	_, err := t.fwdr.ForwardQueryTask(context.Background(), task)
	t.Equal(errNoParent, err)

	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_DECISION)
	t.fwdr.taskQueueKind = enumspb.TASK_QUEUE_KIND_STICKY
	_, err = t.fwdr.ForwardQueryTask(context.Background(), task)
	t.Equal(errTaskQueueKind, err)
}

func (t *ForwarderTestSuite) TestForwardQueryTask() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_DECISION)
	task := newInternalQueryTask("id1", &matchingservice.QueryWorkflowRequest{})
	resp := &matchingservice.QueryWorkflowResponse{}
	var request *matchingservice.QueryWorkflowRequest
	t.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.QueryWorkflowRequest) {
			request = arg1
		},
	).Return(resp, nil).Times(1)

	gotResp, err := t.fwdr.ForwardQueryTask(context.Background(), task)
	t.NoError(err)
	t.Equal(t.taskQueue.Parent(20), request.TaskQueue.GetName())
	t.Equal(enumspb.TaskQueueKind(t.fwdr.taskQueueKind), request.TaskQueue.GetKind())
	t.Equal(task.query.request.QueryRequest, request.QueryRequest)
	t.Equal(resp, gotResp)
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
	t.NoError(err) // no rateliming should be enforced for query task
}

func (t *ForwarderTestSuite) TestForwardPollError() {
	_, err := t.fwdr.ForwardPoll(context.Background())
	t.Equal(errNoParent, err)

	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	t.fwdr.taskQueueKind = enumspb.TASK_QUEUE_KIND_STICKY
	_, err = t.fwdr.ForwardPoll(context.Background())
	t.Equal(errTaskQueueKind, err)

}

func (t *ForwarderTestSuite) TestForwardPollForDecision() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_DECISION)

	pollerID := uuid.New()
	ctx := context.WithValue(context.Background(), pollerIDKey, pollerID)
	ctx = context.WithValue(ctx, identityKey, "id1")
	resp := &matchingservice.PollForDecisionTaskResponse{}

	var request *matchingservice.PollForDecisionTaskRequest
	t.client.EXPECT().PollForDecisionTask(gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollForDecisionTaskRequest) {
			request = arg1
		},
	).Return(resp, nil).Times(1)

	task, err := t.fwdr.ForwardPoll(ctx)
	t.NoError(err)
	t.NotNil(task)
	t.NotNil(request)
	t.Equal(pollerID, request.GetPollerId())
	t.Equal(t.taskQueue.namespaceID, request.GetNamespaceId())
	t.Equal("id1", request.GetPollRequest().GetIdentity())
	t.Equal(t.taskQueue.Parent(20), request.GetPollRequest().GetTaskQueue().GetName())
	t.Equal(enumspb.TaskQueueKind(t.fwdr.taskQueueKind), request.GetPollRequest().GetTaskQueue().GetKind())
	t.Equal(resp, task.pollForDecisionResponse())
	t.Nil(task.pollForActivityResponse())
}

func (t *ForwarderTestSuite) TestForwardPollForActivity() {
	t.usingTaskqueuePartition(enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	pollerID := uuid.New()
	ctx := context.WithValue(context.Background(), pollerIDKey, pollerID)
	ctx = context.WithValue(ctx, identityKey, "id1")
	resp := &matchingservice.PollForActivityTaskResponse{}

	var request *matchingservice.PollForActivityTaskRequest
	t.client.EXPECT().PollForActivityTask(gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollForActivityTaskRequest) {
			request = arg1
		},
	).Return(resp, nil).Times(1)

	task, err := t.fwdr.ForwardPoll(ctx)
	t.NoError(err)
	t.NotNil(task)
	t.NotNil(request)
	t.Equal(pollerID, request.GetPollerId())
	t.Equal(t.taskQueue.namespaceID, request.GetNamespaceId())
	t.Equal("id1", request.GetPollRequest().GetIdentity())
	t.Equal(t.taskQueue.Parent(20), request.GetPollRequest().GetTaskQueue().GetName())
	t.Equal(enumspb.TaskQueueKind(t.fwdr.taskQueueKind), request.GetPollRequest().GetTaskQueue().GetKind())
	t.Equal(resp, task.pollForActivityResponse())
	t.Nil(task.pollForDecisionResponse())
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
					select {
					case token := <-t.fwdr.AddReqTokenC():
						if !tc.mustLeakToken {
							token.release()
						}
						atomic.AddInt32(&adds, 1)
					case <-time.After(time.Millisecond * 200):
						break
					}

					select {
					case token := <-t.fwdr.PollReqTokenC():
						if !tc.mustLeakToken {
							token.release()
						}
						atomic.AddInt32(&polls, 1)
					case <-time.After(time.Millisecond * 200):
						break
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
	t.taskQueue = newTestTaskQueueID("fwdr", taskQueuePartitionPrefix+"tl0/1", taskType)
	t.fwdr.taskQueueID = t.taskQueue
}
