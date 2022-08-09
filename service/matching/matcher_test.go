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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"go.uber.org/atomic"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
)

var errMatchingHostThrottleTest = serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, "Matching host RPS exceeded.")

type MatcherTestSuite struct {
	suite.Suite
	controller  *gomock.Controller
	client      *matchingservicemock.MockMatchingServiceClient
	fwdr        *Forwarder
	cfg         *taskQueueConfig
	taskQueue   *taskQueueID
	matcher     *TaskMatcher // matcher for child partition
	rootMatcher *TaskMatcher // matcher for parent partition
}

func TestMatcherSuite(t *testing.T) {
	suite.Run(t, new(MatcherTestSuite))
}

func (t *MatcherTestSuite) SetupTest() {
	t.controller = gomock.NewController(t.T())
	t.client = matchingservicemock.NewMockMatchingServiceClient(t.controller)
	cfg := NewConfig(dynamicconfig.NewNoopCollection())
	t.taskQueue = newTestTaskQueueID(namespace.ID(uuid.New()), taskQueuePartitionPrefix+"tl0/1", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	tlCfg := newTaskQueueConfig(t.taskQueue, cfg, "test-namespace")
	tlCfg.forwarderConfig = forwarderConfig{
		ForwarderMaxOutstandingPolls: func() int { return 1 },
		ForwarderMaxOutstandingTasks: func() int { return 1 },
		ForwarderMaxRatePerSecond:    func() int { return 2 },
		ForwarderMaxChildrenPerNode:  func() int { return 20 },
	}
	t.cfg = tlCfg
	t.fwdr = newForwarder(&t.cfg.forwarderConfig, t.taskQueue, enumspb.TASK_QUEUE_KIND_NORMAL, t.client)
	t.matcher = newTaskMatcher(tlCfg, t.fwdr, metrics.NoopScope)

	rootTaskQueue := newTestTaskQueueID(t.taskQueue.namespaceID, t.taskQueue.Parent(20), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	rootTaskqueueCfg := newTaskQueueConfig(rootTaskQueue, cfg, "test-namespace")
	t.rootMatcher = newTaskMatcher(rootTaskqueueCfg, nil, metrics.NoopScope)
}

func (t *MatcherTestSuite) TearDownTest() {
	t.controller.Finish()
}

func (t *MatcherTestSuite) TestLocalSyncMatch() {
	// force disable remote forwarding
	<-t.fwdr.AddReqTokenC()
	<-t.fwdr.PollReqTokenC()

	pollStarted := make(chan struct{})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		close(pollStarted)
		task, err := t.matcher.Poll(ctx)
		cancel()
		if err == nil {
			task.finish(nil)
		}
	}()

	<-pollStarted
	time.Sleep(10 * time.Millisecond)
	task := newInternalTask(randomTaskInfo(), nil, enumsspb.TASK_SOURCE_HISTORY, "", true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	syncMatch, err := t.matcher.Offer(ctx, task)
	cancel()
	t.NoError(err)
	t.True(syncMatch)
}

func (t *MatcherTestSuite) TestRemoteSyncMatch() {
	t.testRemoteSyncMatch(enumsspb.TASK_SOURCE_HISTORY)
}

func (t *MatcherTestSuite) TestRemoteSyncMatchBlocking() {
	t.testRemoteSyncMatch(enumsspb.TASK_SOURCE_DB_BACKLOG)
}

func (t *MatcherTestSuite) testRemoteSyncMatch(taskSource enumsspb.TaskSource) {
	pollSigC := make(chan struct{})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		<-pollSigC
		if taskSource == enumsspb.TASK_SOURCE_DB_BACKLOG {
			// when task is from dbBacklog, sync match SHOULD block
			// so lets delay polling by a bit to verify that
			time.Sleep(time.Millisecond * 10)
		}
		task, err := t.matcher.Poll(ctx)
		cancel()
		if err == nil && !task.isStarted() {
			task.finish(nil)
		}
	}()

	var remotePollErr error
	var remotePollResp matchingservice.PollWorkflowTaskQueueResponse
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
			task, err := t.rootMatcher.Poll(arg0)
			if err != nil {
				remotePollErr = err
			} else {
				task.finish(nil)
				remotePollResp = matchingservice.PollWorkflowTaskQueueResponse{
					WorkflowExecution: task.workflowExecution(),
				}
			}
		},
	).Return(&remotePollResp, remotePollErr).AnyTimes()

	task := newInternalTask(randomTaskInfo(), nil, taskSource, "", true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	var err error
	var remoteSyncMatch bool
	var req *matchingservice.AddWorkflowTaskRequest
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddWorkflowTaskRequest, arg2 ...interface{}) {
			req = arg1
			task.forwardedFrom = req.GetForwardedSource()
			close(pollSigC)
			if taskSource != enumsspb.TASK_SOURCE_DB_BACKLOG {
				// when task is not from backlog, wait a bit for poller
				// to arrive first - when task is from backlog, offer
				// blocks - so we don't need to do this
				time.Sleep(10 * time.Millisecond)
			}
			remoteSyncMatch, err = t.rootMatcher.Offer(ctx, task)
		},
	).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	_, err0 := t.matcher.Offer(ctx, task)
	t.NoError(err0)
	cancel()
	t.NotNil(req)
	t.NoError(err)
	t.True(remoteSyncMatch)
	t.Equal(t.taskQueue.name, req.GetForwardedSource())
	t.Equal(t.taskQueue.Parent(20), req.GetTaskQueue().GetName())
}

func (t *MatcherTestSuite) TestSyncMatchFailure() {
	task := newInternalTask(randomTaskInfo(), nil, enumsspb.TASK_SOURCE_HISTORY, "", true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	var req *matchingservice.AddWorkflowTaskRequest
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddWorkflowTaskRequest, arg2 ...interface{}) {
			req = arg1
		},
	).Return(&matchingservice.AddWorkflowTaskResponse{}, errMatchingHostThrottleTest)

	syncMatch, err := t.matcher.Offer(ctx, task)
	cancel()
	t.NotNil(req)
	t.NoError(err)
	t.False(syncMatch)
}

func (t *MatcherTestSuite) TestQueryLocalSyncMatch() {
	// force disable remote forwarding
	<-t.fwdr.AddReqTokenC()
	<-t.fwdr.PollReqTokenC()

	pollStarted := make(chan struct{})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		close(pollStarted)
		task, err := t.matcher.PollForQuery(ctx)
		cancel()
		if err == nil && task.isQuery() {
			task.finish(nil)
		}
	}()

	<-pollStarted
	time.Sleep(10 * time.Millisecond)
	task := newInternalQueryTask(uuid.New(), &matchingservice.QueryWorkflowRequest{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := t.matcher.OfferQuery(ctx, task)
	cancel()
	t.NoError(err)
	t.Nil(resp)
}

func (t *MatcherTestSuite) TestQueryRemoteSyncMatch() {
	pollSigC := make(chan struct{})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		<-pollSigC
		task, err := t.matcher.PollForQuery(ctx)
		cancel()
		if err == nil && task.isQuery() {
			task.finish(nil)
		}
	}()

	var querySet = atomic.NewBool(false)
	var remotePollErr error
	var remotePollResp matchingservice.PollWorkflowTaskQueueResponse
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
			task, err := t.rootMatcher.PollForQuery(arg0)
			if err != nil {
				remotePollErr = err
			} else if task.isQuery() {
				task.finish(nil)
				querySet.Swap(true)
				remotePollResp = matchingservice.PollWorkflowTaskQueueResponse{
					Query: &querypb.WorkflowQuery{},
				}
			}
		},
	).Return(&remotePollResp, remotePollErr).AnyTimes()

	task := newInternalQueryTask(uuid.New(), &matchingservice.QueryWorkflowRequest{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	var req *matchingservice.QueryWorkflowRequest
	t.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.QueryWorkflowRequest, arg2 ...interface{}) {
			req = arg1
			task.forwardedFrom = req.GetForwardedSource()
			close(pollSigC)
			time.Sleep(10 * time.Millisecond)
			t.rootMatcher.OfferQuery(ctx, task)
		},
	).Return(&matchingservice.QueryWorkflowResponse{QueryResult: payloads.EncodeString("answer")}, nil)

	result, err := t.matcher.OfferQuery(ctx, task)
	cancel()
	t.NotNil(req)
	t.NoError(err)
	t.NotNil(result)
	t.True(querySet.Load())

	var answer string
	err = payloads.Decode(result.GetQueryResult(), &answer)
	t.NoError(err)
	t.Equal("answer", answer)
	t.Equal(t.taskQueue.name, req.GetForwardedSource())
	t.Equal(t.taskQueue.Parent(20), req.GetTaskQueue().GetName())
}

func (t *MatcherTestSuite) TestQueryRemoteSyncMatchError() {
	<-t.fwdr.PollReqTokenC()

	matched := false
	pollSigC := make(chan struct{})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		<-pollSigC
		task, err := t.matcher.PollForQuery(ctx)
		cancel()
		if err == nil && task.isQuery() {
			matched = true
			task.finish(nil)
		}
	}()

	task := newInternalQueryTask(uuid.New(), &matchingservice.QueryWorkflowRequest{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	var req *matchingservice.QueryWorkflowRequest
	t.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.QueryWorkflowRequest, arg2 ...interface{}) {
			req = arg1
			close(pollSigC)
			time.Sleep(10 * time.Millisecond)
		},
	).Return(nil, errMatchingHostThrottleTest)

	result, err := t.matcher.OfferQuery(ctx, task)
	cancel()
	t.NotNil(req)
	t.NoError(err)
	t.Nil(result)
	t.True(matched)
}

// todo: note from shawn, when does this case happen in production?
func (t *MatcherTestSuite) TestMustOfferLocalMatch() {
	// force disable remote forwarding
	<-t.fwdr.AddReqTokenC()
	<-t.fwdr.PollReqTokenC()

	pollStarted := make(chan struct{})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		close(pollStarted)
		task, err := t.matcher.Poll(ctx)
		cancel()
		if err == nil {
			task.finish(nil)
		}
	}()

	<-pollStarted
	time.Sleep(10 * time.Millisecond)
	task := newInternalTask(randomTaskInfo(), nil, enumsspb.TASK_SOURCE_HISTORY, "", false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := t.matcher.MustOffer(ctx, task)
	cancel()
	t.NoError(err)
}

func (t *MatcherTestSuite) TestMustOfferRemoteMatch() {
	var wg sync.WaitGroup
	wg.Add(1)

	pollSigC := make(chan struct{})
	var remotePollErr error
	var remotePollResp matchingservice.PollWorkflowTaskQueueResponse
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
			wg.Done()
			<-pollSigC
			time.Sleep(time.Millisecond * 500) // delay poll to verify that offer blocks on parent
			task, err := t.rootMatcher.Poll(arg0)
			if err != nil {
				remotePollErr = err
			} else {
				task.finish(nil)
				remotePollResp = matchingservice.PollWorkflowTaskQueueResponse{
					WorkflowExecution: task.workflowExecution(),
				}
			}
		},
	).Return(&remotePollResp, remotePollErr).AnyTimes()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		t.matcher.Poll(ctx)
		cancel()
	}()

	taskCompleted := false
	completionFunc := func(*persistencespb.AllocatedTaskInfo, error) {
		taskCompleted = true
	}

	task := newInternalTask(randomTaskInfo(), completionFunc, enumsspb.TASK_SOURCE_DB_BACKLOG, "", false)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)

	var err error
	var remoteSyncMatch bool
	var req *matchingservice.AddWorkflowTaskRequest
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, errMatchingHostThrottleTest)
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddWorkflowTaskRequest, arg2 ...interface{}) {
			req = arg1
			task := newInternalTask(task.event.AllocatedTaskInfo, nil, enumsspb.TASK_SOURCE_DB_BACKLOG, req.GetForwardedSource(), true)
			close(pollSigC)
			remoteSyncMatch, err = t.rootMatcher.Offer(ctx, task)
		},
	).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	// This ensures that the poll request has been forwarded to the parent partition before the offer is made.
	// Without this, there is a chance that the request is matched on the child partition, which will
	// fail the test as the PollWorkflowTaskQueue and the 2nd AddWorkflowTask expectations would then never be met.
	wg.Wait()

	t.NoError(t.matcher.MustOffer(ctx, task))
	cancel()

	t.NotNil(req)
	t.NoError(err)
	t.True(remoteSyncMatch)
	t.True(taskCompleted)
	t.Equal(t.taskQueue.name, req.GetForwardedSource())
	t.Equal(t.taskQueue.Parent(20), req.GetTaskQueue().GetName())
}

func (t *MatcherTestSuite) TestRemotePoll() {
	pollToken := <-t.fwdr.PollReqTokenC()

	var req *matchingservice.PollWorkflowTaskQueueRequest
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
			req = arg1
		},
	).Return(&matchingservice.PollWorkflowTaskQueueResponse{}, nil)

	go func() {
		time.Sleep(10 * time.Millisecond)
		pollToken.release()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	task, err := t.matcher.Poll(ctx)
	cancel()
	t.NoError(err)
	t.NotNil(req)
	t.NotNil(task)
	t.True(task.isStarted())
}

func (t *MatcherTestSuite) TestRemotePollForQuery() {
	pollToken := <-t.fwdr.PollReqTokenC()

	var req *matchingservice.PollWorkflowTaskQueueRequest
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
			req = arg1
		},
	).Return(&matchingservice.PollWorkflowTaskQueueResponse{}, nil)

	go func() {
		time.Sleep(10 * time.Millisecond)
		pollToken.release()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	task, err := t.matcher.PollForQuery(ctx)
	cancel()
	t.NoError(err)
	t.NotNil(req)
	t.NotNil(task)
	t.True(task.isStarted())
}

func randomTaskInfo() *persistencespb.AllocatedTaskInfo {
	rt1 := time.Date(rand.Intn(9999), time.Month(rand.Intn(12)+1), rand.Intn(28)+1, rand.Intn(24)+1, rand.Intn(60), rand.Intn(60), rand.Intn(1e9), time.UTC)
	rt2 := time.Date(rand.Intn(5000)+3000, time.Month(rand.Intn(12)+1), rand.Intn(28)+1, rand.Intn(24)+1, rand.Intn(60), rand.Intn(60), rand.Intn(1e9), time.UTC)

	return &persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{
			NamespaceId:      uuid.New(),
			WorkflowId:       uuid.New(),
			RunId:            uuid.New(),
			ScheduledEventId: rand.Int63(),
			CreateTime:       &rt1,
			ExpiryTime:       &rt2,
		},
		TaskId: rand.Int63(),
	}
}
