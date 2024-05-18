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
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
)

var errMatchingHostThrottleTest = &serviceerror.ResourceExhausted{
	Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
	Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
	Message: "Matching host RPS exceeded.",
}

type MatcherTestSuite struct {
	suite.Suite
	controller  *gomock.Controller
	client      *matchingservicemock.MockMatchingServiceClient
	fwdr        *Forwarder
	cfg         *taskQueueConfig
	queue       *PhysicalTaskQueueKey
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
	cfg.BacklogNegligibleAge = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(5 * time.Second)
	cfg.MaxWaitForPollerBeforeFwd = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(5 * time.Millisecond)

	f, err := tqid.NewTaskQueueFamily("", "tl0")
	t.Assert().NoError(err)
	prtn := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(1)
	t.queue = UnversionedQueueKey(prtn)
	tlCfg := newTaskQueueConfig(prtn.TaskQueue(), cfg, "test-namespace")
	tlCfg.forwarderConfig = forwarderConfig{
		ForwarderMaxOutstandingPolls: func() int { return 1 },
		ForwarderMaxOutstandingTasks: func() int { return 1 },
		ForwarderMaxRatePerSecond:    func() int { return 2 },
		ForwarderMaxChildrenPerNode:  func() int { return 20 },
	}
	t.cfg = tlCfg
	t.fwdr, err = newForwarder(&t.cfg.forwarderConfig, t.queue, t.client)
	t.Assert().NoError(err)
	t.matcher = newTaskMatcher(tlCfg, t.fwdr, metrics.NoopMetricsHandler)

	rootTaskqueueCfg := newTaskQueueConfig(prtn.TaskQueue(), cfg, "test-namespace")
	t.rootMatcher = newTaskMatcher(rootTaskqueueCfg, nil, metrics.NoopMetricsHandler)
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
		task, err := t.matcher.Poll(ctx, &pollMetadata{})
		cancel()
		if err == nil {
			task.finish(nil)
		}
	}()

	<-pollStarted
	time.Sleep(10 * time.Millisecond)
	task := newInternalTaskForSyncMatch(randomTaskInfo().Data, nil)
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
		task, err := t.matcher.Poll(ctx, &pollMetadata{})
		cancel()
		if err == nil && !task.isStarted() {
			task.finish(nil)
		}
	}()

	var remotePollErr error
	var remotePollResp matchingservice.PollWorkflowTaskQueueResponse
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
			task, err := t.rootMatcher.Poll(arg0, &pollMetadata{})
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

	task := newInternalTaskForSyncMatch(randomTaskInfo().Data, nil)
	if taskSource == enumsspb.TASK_SOURCE_DB_BACKLOG {
		task = newInternalTaskForSyncMatch(randomTaskInfo().Data, &taskqueuespb.TaskForwardInfo{
			TaskSource:      enumsspb.TASK_SOURCE_DB_BACKLOG,
			SourcePartition: "p123",
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	var err error
	var remoteSyncMatch bool
	var req *matchingservice.AddWorkflowTaskRequest
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddWorkflowTaskRequest, arg2 ...interface{}) {
			req = arg1
			task.forwardInfo = req.GetForwardInfo()
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
	t.Equal(t.queue.PersistenceName(), req.GetForwardInfo().GetSourcePartition())
	t.Equal(mustParent(t.queue.partition.(*tqid.NormalPartition), 20).RpcName(), req.GetTaskQueue().GetName())
}

func (t *MatcherTestSuite) TestRejectSyncMatchWhenBacklog() {
	historyTask := newInternalTaskForSyncMatch(randomTaskInfo().Data, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	// sync match happens when there is no backlog
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.PollWorkflowTaskQueueResponse{}, errMatchingHostThrottleTest)
	go t.matcher.Poll(ctx, &pollMetadata{}) //nolint:errcheck
	time.Sleep(time.Millisecond)
	go func() { historyTask.responseC <- nil }()
	happened, err := t.matcher.Offer(ctx, historyTask)
	t.True(happened)
	t.Nil(err)

	intruptC := make(chan struct{})
	youngBacklogTask := newInternalTaskFromBacklog(randomTaskInfoWithAge(time.Second), nil)
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, errMatchingHostThrottleTest)
	go t.matcher.MustOffer(ctx, youngBacklogTask, intruptC) //nolint:errcheck
	time.Sleep(time.Millisecond)

	// should allow sync match when there is only young tasks in the backlog
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, errMatchingHostThrottleTest)
	happened, err = t.matcher.Offer(ctx, historyTask)
	t.Nil(err)
	t.False(happened) // sync match did not happen, but we called the forwarder client

	// should not allow sync match when there is an old task in backlog
	oldBacklogTask := newInternalTaskFromBacklog(randomTaskInfoWithAge(time.Minute), nil)
	go t.matcher.MustOffer(ctx, oldBacklogTask, intruptC) //nolint:errcheck
	time.Sleep(time.Millisecond)
	happened, err = t.matcher.Offer(ctx, historyTask)
	t.False(happened)
	t.Nil(err)

	// poll both tasks
	task, _ := t.matcher.Poll(ctx, &pollMetadata{})
	t.NotNil(task)
	task, _ = t.matcher.Poll(ctx, &pollMetadata{})
	t.NotNil(task)
	time.Sleep(time.Millisecond)

	// should allow sync match now
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.PollWorkflowTaskQueueResponse{}, errMatchingHostThrottleTest)
	go t.matcher.Poll(ctx, &pollMetadata{}) //nolint:errcheck
	time.Sleep(time.Millisecond)
	go func() { historyTask.responseC <- nil }()
	happened, err = t.matcher.Offer(ctx, historyTask)
	t.True(happened)
	t.Nil(err)

	cancel()
}

func (t *MatcherTestSuite) TestForwardingWhenBacklogIsYoung() {
	historyTask := newInternalTaskForSyncMatch(randomTaskInfo().Data, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	intruptC := make(chan struct{})

	// poll forwarding attempt happens when there is no backlog
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.PollWorkflowTaskQueueResponse{}, errMatchingHostThrottleTest)
	go t.matcher.Poll(ctx, &pollMetadata{}) //nolint:errcheck
	time.Sleep(time.Millisecond)

	// task is not forwarded because there is a poller waiting
	youngBacklogTask := newInternalTaskFromBacklog(randomTaskInfoWithAge(time.Second), nil)
	err := t.matcher.MustOffer(ctx, historyTask, intruptC)
	t.Nil(err)
	cancel()

	// young task is forwarded
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, errMatchingHostThrottleTest)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	go t.matcher.MustOffer(ctx, youngBacklogTask, intruptC) //nolint:errcheck
	time.Sleep(time.Millisecond)
	cancel()
}

func (t *MatcherTestSuite) TestAvoidForwardingWhenBacklogIsOld() {
	intruptC := make(chan struct{})

	// poll forwarding attempt happens when there is no backlog
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.PollWorkflowTaskQueueResponse{}, errMatchingHostThrottleTest)
	go t.matcher.Poll(ctx, &pollMetadata{}) //nolint:errcheck
	time.Sleep(time.Millisecond)
	cancel()

	// old task is not forwarded (forwarded client is not called)
	oldBacklogTask := newInternalTaskFromBacklog(randomTaskInfoWithAge(time.Minute), nil)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	go t.matcher.MustOffer(ctx, oldBacklogTask, intruptC) //nolint:errcheck
	time.Sleep(time.Millisecond)

	// poll both tasks
	task, _ := t.matcher.Poll(ctx, &pollMetadata{})
	t.NotNil(task)
	cancel()

	// even old task is forwarded if last poll is not recent enough
	time.Sleep(t.cfg.MaxWaitForPollerBeforeFwd() + time.Millisecond)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, errMatchingHostThrottleTest)
	go t.matcher.MustOffer(ctx, oldBacklogTask, intruptC) //nolint:errcheck
	time.Sleep(time.Millisecond)
	cancel()
}

func (t *MatcherTestSuite) TestBacklogAge() {
	t.Equal(emptyBacklogAge, t.rootMatcher.getBacklogAge())

	youngBacklogTask := newInternalTaskFromBacklog(randomTaskInfoWithAge(time.Second), nil)

	intruptC := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	go t.rootMatcher.MustOffer(ctx, youngBacklogTask, intruptC) //nolint:errcheck
	time.Sleep(time.Millisecond)
	t.InEpsilon(t.rootMatcher.getBacklogAge(), time.Second, float64(10*time.Millisecond))

	// offering the same task twice to make sure of correct counting
	go t.rootMatcher.MustOffer(ctx, youngBacklogTask, intruptC) //nolint:errcheck
	time.Sleep(time.Millisecond)
	t.InEpsilon(t.rootMatcher.getBacklogAge(), time.Second, float64(10*time.Millisecond))

	oldBacklogTask := newInternalTaskFromBacklog(randomTaskInfoWithAge(time.Minute), nil)
	go t.rootMatcher.MustOffer(ctx, oldBacklogTask, intruptC) //nolint:errcheck
	time.Sleep(time.Millisecond)
	t.InEpsilon(t.rootMatcher.getBacklogAge(), time.Minute, float64(10*time.Millisecond))

	task, _ := t.rootMatcher.Poll(ctx, &pollMetadata{})
	time.Sleep(time.Millisecond)
	t.NotNil(task)
	t.NotEqual(emptyBacklogAge, t.rootMatcher.getBacklogAge())
	task, _ = t.rootMatcher.Poll(ctx, &pollMetadata{})
	time.Sleep(time.Millisecond)
	t.NotNil(task)
	t.NotEqual(emptyBacklogAge, t.rootMatcher.getBacklogAge())
	task, _ = t.rootMatcher.Poll(ctx, &pollMetadata{})
	time.Sleep(time.Millisecond)
	t.NotNil(task)
	t.Equal(emptyBacklogAge, t.rootMatcher.getBacklogAge())

	cancel()
}

func (t *MatcherTestSuite) TestSyncMatchFailure() {
	task := newInternalTaskForSyncMatch(randomTaskInfo().Data, nil)
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

func (t *MatcherTestSuite) TestQueryNoCurrentPollersButRecentPollers() {
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
			_, err := t.rootMatcher.PollForQuery(arg0, &pollMetadata{})
			t.Assert().Error(err, context.DeadlineExceeded)
		},
	).Return(nil, context.DeadlineExceeded).AnyTimes()

	// make a poll that expires
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	_, err := t.matcher.PollForQuery(ctx, &pollMetadata{})
	t.Assert().Error(err, context.DeadlineExceeded)
	cancel()

	// send query and expect generic DeadlineExceeded error
	task := newInternalQueryTask(uuid.New(), &matchingservice.QueryWorkflowRequest{})
	t.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(ctx context.Context, req *matchingservice.QueryWorkflowRequest, arg2 ...interface{}) {
			task.forwardInfo = req.GetForwardInfo()
			resp, err := t.rootMatcher.OfferQuery(ctx, task)
			t.Nil(resp)
			t.Assert().Error(err, context.DeadlineExceeded)
		},
	).Return(nil, context.DeadlineExceeded)

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*10)
	_, err = t.matcher.OfferQuery(ctx, task)
	cancel()
	t.Error(err, context.DeadlineExceeded)
}

func (t *MatcherTestSuite) TestQueryNoRecentPoller() {
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
			_, err := t.rootMatcher.PollForQuery(arg0, &pollMetadata{})
			t.Assert().Error(err, context.DeadlineExceeded)
		},
	).Return(nil, context.DeadlineExceeded).AnyTimes()

	// make a poll that expires
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	_, err := t.matcher.PollForQuery(ctx, &pollMetadata{})
	t.Assert().Error(err, context.DeadlineExceeded)
	cancel()

	// wait 10ms after the poll
	time.Sleep(time.Millisecond * 10)

	// set the window to 5ms
	t.cfg.QueryPollerUnavailableWindow = func() time.Duration {
		return time.Millisecond * 5
	}

	// make the query and expect errNoRecentPoller
	task := newInternalQueryTask(uuid.New(), &matchingservice.QueryWorkflowRequest{})
	t.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(ctx context.Context, req *matchingservice.QueryWorkflowRequest, arg2 ...interface{}) {
			task.forwardInfo = req.GetForwardInfo()
			resp, err := t.rootMatcher.OfferQuery(ctx, task)
			t.Nil(resp)
			t.Assert().Error(err, errNoRecentPoller)
		},
	).Return(nil, errNoRecentPoller)

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*10)
	_, err = t.matcher.OfferQuery(ctx, task)
	cancel()
	t.Error(err, errNoRecentPoller)
}

func (t *MatcherTestSuite) TestQueryNoPollerAtAll() {
	task := newInternalQueryTask(uuid.New(), &matchingservice.QueryWorkflowRequest{})

	t.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(ctx context.Context, req *matchingservice.QueryWorkflowRequest, arg2 ...interface{}) {
			task.forwardInfo = req.GetForwardInfo()
			resp, err := t.rootMatcher.OfferQuery(ctx, task)
			t.Nil(resp)
			t.Assert().Error(err, errNoRecentPoller)
		},
	).Return(nil, errNoRecentPoller)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	_, err := t.matcher.OfferQuery(ctx, task)
	cancel()
	t.Error(err, errNoRecentPoller)
}

func (t *MatcherTestSuite) TestQueryLocalSyncMatch() {
	// force disable remote forwarding
	<-t.fwdr.AddReqTokenC()
	<-t.fwdr.PollReqTokenC()

	pollStarted := make(chan struct{})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		close(pollStarted)
		task, err := t.matcher.PollForQuery(ctx, &pollMetadata{})
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
		task, err := t.matcher.PollForQuery(ctx, &pollMetadata{})
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
			task, err := t.rootMatcher.PollForQuery(arg0, &pollMetadata{})
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
			close(pollSigC)
			time.Sleep(10 * time.Millisecond)
			_, err := t.rootMatcher.OfferQuery(ctx, task)
			t.Assert().NoError(err)
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
	t.Equal(t.queue.PersistenceName(), req.GetForwardInfo().GetSourcePartition())
	t.Equal(mustParent(t.queue.partition.(*tqid.NormalPartition), 20).RpcName(), req.GetTaskQueue().GetName())
}

func (t *MatcherTestSuite) TestQueryRemoteSyncMatchError() {
	<-t.fwdr.PollReqTokenC()

	matched := false
	pollSigC := make(chan struct{})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		<-pollSigC
		task, err := t.matcher.PollForQuery(ctx, &pollMetadata{})
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
		task, err := t.matcher.Poll(ctx, &pollMetadata{})
		cancel()
		if err == nil {
			task.finish(nil)
		}
	}()

	<-pollStarted
	time.Sleep(10 * time.Millisecond)
	task := newInternalTaskForSyncMatch(randomTaskInfo().Data, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := t.matcher.MustOffer(ctx, task, nil)
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
			task, err := t.rootMatcher.Poll(arg0, &pollMetadata{})
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
		_, err := t.matcher.Poll(ctx, &pollMetadata{})
		t.Assert().NoError(err)
		cancel()
	}()

	taskCompleted := false
	completionFunc := func(*persistencespb.AllocatedTaskInfo, error) {
		taskCompleted = true
	}

	task := newInternalTaskFromBacklog(randomTaskInfo(), completionFunc)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)

	var err error
	var remoteSyncMatch bool
	var req *matchingservice.AddWorkflowTaskRequest
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(&matchingservice.AddWorkflowTaskResponse{}, errMatchingHostThrottleTest)
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddWorkflowTaskRequest, arg2 ...interface{}) {
			req = arg1
			task := newInternalTaskForSyncMatch(task.event.AllocatedTaskInfo.Data, req.ForwardInfo)
			close(pollSigC)
			remoteSyncMatch, err = t.rootMatcher.Offer(ctx, task)
		},
	).Return(&matchingservice.AddWorkflowTaskResponse{}, nil)

	// This ensures that the poll request has been forwarded to the parent partition before the offer is made.
	// Without this, there is a chance that the request is matched on the child partition, which will
	// fail the test as the PollWorkflowTaskQueue and the 2nd AddWorkflowTask expectations would then never be met.
	wg.Wait()

	t.NoError(t.matcher.MustOffer(ctx, task, nil))
	cancel()

	t.NotNil(req)
	t.NoError(err)
	t.True(remoteSyncMatch)
	t.True(taskCompleted)
	t.Equal(t.queue.PersistenceName(), req.GetForwardInfo().GetSourcePartition())
	t.Equal(mustParent(t.queue.partition.(*tqid.NormalPartition), 20).RpcName(), req.GetTaskQueue().GetName())
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
	task, forwarded, err := t.matcher.poll(ctx, &pollMetadata{}, false)
	cancel()
	t.NoError(err)
	t.NotNil(req)
	t.NotNil(task)
	t.True(task.isStarted())
	t.True(forwarded)
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
	task, err := t.matcher.PollForQuery(ctx, &pollMetadata{})
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
			CreateTime:       timestamppb.New(rt1),
			ExpiryTime:       timestamppb.New(rt2),
		},
		TaskId: rand.Int63(),
	}
}

func randomTaskInfoWithAge(age time.Duration) *persistencespb.AllocatedTaskInfo {
	rt1 := time.Now().Add(-age)
	rt2 := rt1.Add(time.Hour)

	return &persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{
			NamespaceId:      uuid.New(),
			WorkflowId:       uuid.New(),
			RunId:            uuid.New(),
			ScheduledEventId: rand.Int63(),
			CreateTime:       timestamppb.New(rt1),
			ExpiryTime:       timestamppb.New(rt2),
		},
		TaskId: rand.Int63(),
	}
}
