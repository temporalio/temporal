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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errMatchingHostThrottleTest = &serviceerror.ResourceExhausted{
	Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
	Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
	Message: "Matching host RPS exceeded.",
}

type MatcherTestSuite struct {
	suite.Suite
	controller   *gomock.Controller
	client       *matchingservicemock.MockMatchingServiceClient
	fwdr         *Forwarder
	childConfig  *taskQueueConfig
	rootConfig   *taskQueueConfig
	queue        *PhysicalTaskQueueKey
	childMatcher *TaskMatcher // matcher for child partition
	rootMatcher  *TaskMatcher // matcher for parent partition
}

func TestMatcherSuite(t *testing.T) {
	suite.Run(t, new(MatcherTestSuite))
}

func (t *MatcherTestSuite) SetupTest() {
	t.controller = gomock.NewController(t.T())
	t.client = matchingservicemock.NewMockMatchingServiceClient(t.controller)
	cfg := NewConfig(dynamicconfig.NewNoopCollection())
	cfg.BacklogNegligibleAge = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(5 * time.Second)
	cfg.MaxWaitForPollerBeforeFwd = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(10 * time.Millisecond)

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
	t.childConfig = tlCfg
	t.fwdr, err = newForwarder(&t.childConfig.forwarderConfig, t.queue, t.client)
	t.Assert().NoError(err)
	t.childMatcher = newTaskMatcher(tlCfg, t.fwdr, metrics.NoopMetricsHandler)

	t.rootConfig = newTaskQueueConfig(prtn.TaskQueue(), cfg, "test-namespace")
	t.rootMatcher = newTaskMatcher(t.rootConfig, nil, metrics.NoopMetricsHandler)
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
		task, err := t.childMatcher.Poll(ctx, &pollMetadata{})
		cancel()
		if err == nil {
			task.finish(nil, true)
		}
	}()

	<-pollStarted
	time.Sleep(10 * time.Millisecond)
	task := newInternalTaskForSyncMatch(randomTaskInfo().Data, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	syncMatch, err := t.childMatcher.Offer(ctx, task)
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
		task, err := t.childMatcher.Poll(ctx, &pollMetadata{})
		cancel()
		if err == nil && !task.isStarted() {
			task.finish(nil, true)
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
				task.finish(nil, true)
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

	_, err0 := t.childMatcher.Offer(ctx, task)
	t.NoError(err0)
	cancel()
	t.NotNil(req)
	t.NoError(err)
	t.True(remoteSyncMatch)
	t.Equal(t.queue.PersistenceName(), req.GetForwardInfo().GetSourcePartition())
	t.Equal(mustParent(t.queue.partition.(*tqid.NormalPartition), 20).RpcName(), req.GetTaskQueue().GetName())
}

//nolint:errcheck
func (t *MatcherTestSuite) TestRejectSyncMatchWhenBacklog() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	intruptC := make(chan struct{})

	// task waits for a local poller
	oldBacklogTask := newInternalTaskFromBacklog(randomTaskInfoWithAge(time.Minute), nil)

	go func() {
		t.rootMatcher.MustOffer(ctx, oldBacklogTask, intruptC) //nolint:revive
	}()

	// Wait for the task to be added to the map
	t.EventuallyWithT(func(c *assert.CollectT) {
		assert.False(c, t.rootMatcher.isBacklogNegligible())
	}, 30*time.Second, 1*time.Millisecond)

	// should not allow sync match when there is an old task in backlog
	syncMatchTask := newInternalTaskForSyncMatch(randomTaskInfoWithAge(time.Minute).Data, nil)
	// Adding forwardInfo to replicate a task being forwarded from the child partition.
	// This field is required to be non-nil for the matcher to offer this task locally to a poller, which is desired.
	syncMatchTask.forwardInfo = &taskqueuespb.TaskForwardInfo{
		SourcePartition:    "",
		TaskSource:         0,
		RedirectInfo:       nil,
		DispatchBuildId:    "",
		DispatchVersionSet: "",
	}
	newCtx, newCtxCancel := context.WithTimeout(context.Background(), 1*time.Second)
	// When the root partition has no pollers and is offered a task for sync match, the task
	// gets blocked locally (until a local poller arrives) *unless* the partition has a non-negligible backlog.
	// Since the partition currently has no pollers, the test verifies that the task is not blocked locally
	// by asserting a context cancellation due to a timeout never occurred and we received a *false* due to a
	// non-negligible backlog.
	happened, err := t.rootMatcher.Offer(newCtx, syncMatchTask)
	if newCtx.Err() != nil {
		t.FailNow("waited on a local poller due to a negligible backlog")

	}
	t.False(happened)
	t.Nil(err)
	newCtxCancel()

	// poll old task which is from the backlog
	task, _ := t.rootMatcher.Poll(ctx, &pollMetadata{})
	t.NotNil(task)
	t.Equal(enumsspb.TASK_SOURCE_DB_BACKLOG, task.source)
	cancel()
}

func (t *MatcherTestSuite) TestForwardingWhenBacklogIsYoung() {
	historyTask := newInternalTaskForSyncMatch(randomTaskInfo().Data, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	intruptC := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)

	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
			wg.Done()
		},
	).Return(&matchingservice.PollWorkflowTaskQueueResponse{}, errMatchingHostThrottleTest)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		// poll forwarding attempt happens when there is no backlog
		_, err := t.childMatcher.Poll(ctx, &pollMetadata{})
		t.Assert().NoError(err)
		cancel()
	}()
	// This ensures that the poll request has been forwarded to the parent partition before the offer is made.
	// Without this, there is a chance that the request is matched on the child partition, which will fail the test by
	// complaining about a missing PollWorkflowTaskQueue.
	wg.Wait()

	// to ensure poller is now blocked locally
	time.Sleep(2 * time.Millisecond)

	// task is not forwarded because there is a local poller waiting
	err := t.childMatcher.MustOffer(ctx, historyTask, intruptC)
	t.Nil(err)
	cancel()

	// young task is forwarded
	youngBacklogTask := newInternalTaskFromBacklog(randomTaskInfoWithAge(time.Second), nil)

	wg.Add(1)
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.AddWorkflowTaskRequest, arg2 ...interface{}) {
			// Offer forwarding has occured
			wg.Done()
		},
	).Return(&matchingservice.AddWorkflowTaskResponse{}, errMatchingHostThrottleTest)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	go t.childMatcher.MustOffer(ctx, youngBacklogTask, intruptC) //nolint:errcheck
	wg.Wait()
	time.Sleep(time.Millisecond)
	cancel()
}

func (t *MatcherTestSuite) TestForwardingWhenBacklogIsEmpty() {
	// poll forwarding attempt happens when there is no backlog
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.client.EXPECT().PollWorkflowTaskQueue(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).Return(&matchingservice.PollWorkflowTaskQueueResponse{}, errMatchingHostThrottleTest)
	_, e := t.childMatcher.Poll(ctx, &pollMetadata{})

	t.ErrorIs(e, errNoTasks)
	cancel()
}

func (t *MatcherTestSuite) TestAvoidForwardingWhenBacklogIsOld() {
	t.childConfig.MaxWaitForPollerBeforeFwd = dynamicconfig.GetDurationPropertyFn(20 * time.Millisecond)

	interruptC := make(chan struct{})
	// forwarding will be triggered after t.cfg.MaxWaitForPollerBeforeFwd() so steps in this test should finish sooner.
	maxWait := t.childConfig.MaxWaitForPollerBeforeFwd() * 2 / 3

	// poll forwarding attempt happens when there is no backlog.
	// important to make this empty poll to set the last poll timestamp to a recent time
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	forwardPoll := make(chan struct{})
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, in *matchingservice.PollWorkflowTaskQueueRequest, opts ...grpc.CallOption) (*matchingservice.PollWorkflowTaskQueueResponse, error) {
			forwardPoll <- struct{}{}
			return &matchingservice.PollWorkflowTaskQueueResponse{}, errMatchingHostThrottleTest
		})
	go t.childMatcher.Poll(ctx, &pollMetadata{}) //nolint:errcheck
	select {
	case <-forwardPoll:
	case <-ctx.Done():
		t.FailNow("timed out")
	}
	cancel()

	// old task is not forwarded (forwarded client is not called)
	oldBacklogTask := newInternalTaskFromBacklog(randomTaskInfoWithAge(time.Minute), nil)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	go t.childMatcher.MustOffer(ctx, oldBacklogTask, interruptC) //nolint:errcheck
	t.Require().Eventually(
		func() bool {
			return t.childMatcher.getBacklogAge() > 0
		}, maxWait, time.Millisecond/2)

	// poll the task
	task, _ := t.childMatcher.Poll(ctx, &pollMetadata{})
	t.NotNil(task)
	cancel()

	// should be back to no backlog
	t.Require().Eventually(
		func() bool {
			return t.childMatcher.getBacklogAge() == emptyBacklogAge
		}, maxWait, time.Millisecond/2)

	// even old task is forwarded if last poll is not recent enough
	time.Sleep(t.childConfig.MaxWaitForPollerBeforeFwd() + time.Millisecond) //nolint:forbidigo
	t.Greater(t.childMatcher.timeSinceLastPoll(), t.childConfig.MaxWaitForPollerBeforeFwd())

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	forwardTask := make(chan struct{})
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, in *matchingservice.AddWorkflowTaskRequest, opts ...grpc.CallOption) (*matchingservice.AddWorkflowTaskResponse, error) {
			forwardTask <- struct{}{}
			return &matchingservice.AddWorkflowTaskResponse{}, nil // return success so it won't retry
		})
	go t.childMatcher.MustOffer(ctx, oldBacklogTask, interruptC) //nolint:errcheck
	select {
	case <-forwardTask:
	case <-ctx.Done():
		t.FailNow("timed out")
	}
	cancel()
}

func (t *MatcherTestSuite) TestAvoidForwardingWhenBacklogIsOldButReconsider() {
	t.childConfig.MaxWaitForPollerBeforeFwd = dynamicconfig.GetDurationPropertyFn(20 * time.Millisecond)

	// make it look like there was a recent poll
	t.childMatcher.lastPoller.Store(time.Now().UnixNano())

	// old task is not forwarded, until after the reconsider time
	oldBacklogTask := newInternalTaskFromBacklog(randomTaskInfoWithAge(time.Minute), nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	forwardTask := make(chan time.Time)
	t.client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, in *matchingservice.AddWorkflowTaskRequest, opts ...grpc.CallOption) (*matchingservice.AddWorkflowTaskResponse, error) {
			forwardTask <- time.Now()
			return &matchingservice.AddWorkflowTaskResponse{}, nil // return success so it won't retry
		})
	go t.childMatcher.MustOffer(ctx, oldBacklogTask, make(chan struct{})) //nolint:errcheck

	select {
	case fwdTime := <-forwardTask:
		// check forward was after reconsider time. add a little buffer for last poller time calculation in MustOffer.
		t.Greater(fwdTime.Sub(start), t.childConfig.MaxWaitForPollerBeforeFwd()*9/10)
	case <-ctx.Done():
		t.FailNow("timed out")
	}
}

func (t *MatcherTestSuite) TestBacklogAge() {
	t.Equal(emptyBacklogAge, t.rootMatcher.getBacklogAge())

	youngBacklogTask := newInternalTaskFromBacklog(randomTaskInfoWithAge(time.Second), nil)

	intruptC := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	go t.rootMatcher.MustOffer(ctx, youngBacklogTask, intruptC) //nolint:errcheck

	time.Sleep(time.Millisecond)
	t.InDelta(t.rootMatcher.getBacklogAge(), time.Second, float64(10*time.Millisecond))

	// offering the same task twice to make sure of correct counting
	go t.rootMatcher.MustOffer(ctx, youngBacklogTask, intruptC) //nolint:errcheck
	time.Sleep(time.Millisecond)
	t.InDelta(t.rootMatcher.getBacklogAge(), time.Second, float64(10*time.Millisecond))

	oldBacklogTask := newInternalTaskFromBacklog(randomTaskInfoWithAge(time.Minute), nil)
	go t.rootMatcher.MustOffer(ctx, oldBacklogTask, intruptC) //nolint:errcheck
	time.Sleep(time.Millisecond)
	t.InDelta(t.rootMatcher.getBacklogAge(), time.Minute, float64(10*time.Millisecond))

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

	syncMatch, err := t.childMatcher.Offer(ctx, task)
	cancel()
	t.NotNil(req)
	t.NoError(err)
	t.False(syncMatch)
}

func (t *MatcherTestSuite) TestQueryNoCurrentPollersButRecentPollers() {
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
			_, err := t.rootMatcher.PollForQuery(arg0, &pollMetadata{})
			t.Assert().ErrorIs(err, errNoTasks)
		},
	).Return(emptyPollWorkflowTaskQueueResponse, nil).AnyTimes()

	// make a poll that expires
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	task, err := t.childMatcher.PollForQuery(ctx, &pollMetadata{})
	t.Assert().Empty(task.started.workflowTaskInfo)
	t.Assert().NoError(err)
	cancel()

	// send query and expect generic DeadlineExceeded error
	task = newInternalQueryTask(uuid.New(), &matchingservice.QueryWorkflowRequest{})
	t.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(ctx context.Context, req *matchingservice.QueryWorkflowRequest, arg2 ...interface{}) {
			task.forwardInfo = req.GetForwardInfo()
			resp, err := t.rootMatcher.OfferQuery(ctx, task)
			t.Nil(resp)
			t.Assert().ErrorIs(err, context.DeadlineExceeded)
		},
	).Return(nil, context.DeadlineExceeded)

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*10)
	_, err = t.childMatcher.OfferQuery(ctx, task)
	cancel()
	t.ErrorIs(err, context.DeadlineExceeded)
}

func (t *MatcherTestSuite) TestQueryNoRecentPoller() {
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
			_, err := t.rootMatcher.PollForQuery(arg0, &pollMetadata{})
			t.Assert().ErrorIs(err, errNoTasks)
		},
	).Return(emptyPollWorkflowTaskQueueResponse, nil).AnyTimes()

	// make a poll that expires
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	task, err := t.childMatcher.PollForQuery(ctx, &pollMetadata{})
	t.Assert().Empty(task.started.workflowTaskInfo)
	t.Assert().NoError(err)
	cancel()

	// wait 10ms after the poll
	time.Sleep(time.Millisecond * 10)

	// set the window to 5ms
	t.childConfig.QueryPollerUnavailableWindow = dynamicconfig.GetDurationPropertyFn(time.Millisecond * 5)
	t.rootConfig.QueryPollerUnavailableWindow = dynamicconfig.GetDurationPropertyFn(time.Millisecond * 5)

	// make the query and expect errNoRecentPoller
	task = newInternalQueryTask(uuid.New(), &matchingservice.QueryWorkflowRequest{})
	t.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(ctx context.Context, req *matchingservice.QueryWorkflowRequest, arg2 ...interface{}) {
			task.forwardInfo = req.GetForwardInfo()
			resp, err := t.rootMatcher.OfferQuery(ctx, task)
			t.Nil(resp)
			t.Assert().ErrorIs(err, errNoRecentPoller)
		},
	).Return(nil, errNoRecentPoller)

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*10)
	_, err = t.childMatcher.OfferQuery(ctx, task)
	cancel()
	t.ErrorIs(err, errNoRecentPoller)
}

func (t *MatcherTestSuite) TestQueryNoPollerAtAll() {
	task := newInternalQueryTask(uuid.New(), &matchingservice.QueryWorkflowRequest{})

	t.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(ctx context.Context, req *matchingservice.QueryWorkflowRequest, arg2 ...interface{}) {
			task.forwardInfo = req.GetForwardInfo()
			resp, err := t.rootMatcher.OfferQuery(ctx, task)
			t.Nil(resp)
			t.Assert().ErrorIs(err, errNoRecentPoller)
		},
	).Return(nil, errNoRecentPoller)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	_, err := t.childMatcher.OfferQuery(ctx, task)
	cancel()
	t.ErrorIs(err, errNoRecentPoller)
}

func (t *MatcherTestSuite) TestQueryLocalSyncMatch() {
	// force disable remote forwarding
	<-t.fwdr.AddReqTokenC()
	<-t.fwdr.PollReqTokenC()

	pollStarted := make(chan struct{})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		close(pollStarted)
		task, err := t.childMatcher.PollForQuery(ctx, &pollMetadata{})
		cancel()
		if err == nil && task.isQuery() {
			task.finish(nil, true)
		}
	}()

	<-pollStarted
	time.Sleep(10 * time.Millisecond)
	task := newInternalQueryTask(uuid.New(), &matchingservice.QueryWorkflowRequest{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := t.childMatcher.OfferQuery(ctx, task)
	cancel()
	t.NoError(err)
	t.Nil(resp)
}

func (t *MatcherTestSuite) TestQueryRemoteSyncMatch() {
	pollSigC := make(chan struct{})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		<-pollSigC
		task, err := t.childMatcher.PollForQuery(ctx, &pollMetadata{})
		cancel()
		if err == nil && task.isQuery() {
			task.finish(nil, true)
		}
	}()

	var querySet = atomic.Bool{}
	var remotePollErr error
	var remotePollResp matchingservice.PollWorkflowTaskQueueResponse
	t.client.EXPECT().PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *matchingservice.PollWorkflowTaskQueueRequest, arg2 ...interface{}) {
			task, err := t.rootMatcher.PollForQuery(arg0, &pollMetadata{})
			if err != nil {
				remotePollErr = err
			} else if task.isQuery() {
				task.finish(nil, true)
				querySet.Store(true)
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

	result, err := t.childMatcher.OfferQuery(ctx, task)
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
		task, err := t.childMatcher.PollForQuery(ctx, &pollMetadata{})
		cancel()
		if err == nil && task.isQuery() {
			matched = true
			task.finish(nil, true)
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

	result, err := t.childMatcher.OfferQuery(ctx, task)
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
		task, err := t.childMatcher.Poll(ctx, &pollMetadata{})
		cancel()
		if err == nil {
			task.finish(nil, true)
		}
	}()

	<-pollStarted
	time.Sleep(10 * time.Millisecond)
	task := newInternalTaskForSyncMatch(randomTaskInfo().Data, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := t.childMatcher.MustOffer(ctx, task, nil)
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
				task.finish(nil, true)
				remotePollResp = matchingservice.PollWorkflowTaskQueueResponse{
					WorkflowExecution: task.workflowExecution(),
				}
			}
		},
	).Return(&remotePollResp, remotePollErr).AnyTimes()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		_, err := t.childMatcher.Poll(ctx, &pollMetadata{})
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

	t.NoError(t.childMatcher.MustOffer(ctx, task, nil))
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
	task, forwarded, err := t.childMatcher.poll(ctx, &pollMetadata{}, false)
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
	task, err := t.childMatcher.PollForQuery(ctx, &pollMetadata{})
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
