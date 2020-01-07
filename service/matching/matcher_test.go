// Copyright (c) 2019 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"

	gen "github.com/uber/cadence/.gen/go/matching"
	"github.com/uber/cadence/.gen/go/matching/matchingservicetest"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type MatcherTestSuite struct {
	suite.Suite
	controller *gomock.Controller
	client     *matchingservicetest.MockClient
	fwdr       *Forwarder
	cfg        *taskListConfig
	taskList   *taskListID
	matcher    *TaskMatcher
}

func TestMatcherSuite(t *testing.T) {
	suite.Run(t, new(MatcherTestSuite))
}

func (t *MatcherTestSuite) SetupTest() {
	t.controller = gomock.NewController(t.T())
	t.client = matchingservicetest.NewMockClient(t.controller)
	cfg := NewConfig(dynamicconfig.NewNopCollection())
	t.taskList = newTestTaskListID(uuid.New(), taskListPartitionPrefix+"tl0/1", persistence.TaskListTypeDecision)
	tlCfg, err := newTaskListConfig(t.taskList, cfg, t.newDomainCache())
	t.NoError(err)
	tlCfg.forwarderConfig = forwarderConfig{
		ForwarderMaxOutstandingPolls: func() int { return 1 },
		ForwarderMaxOutstandingTasks: func() int { return 1 },
		ForwarderMaxRatePerSecond:    func() int { return 2 },
		ForwarderMaxChildrenPerNode:  func() int { return 20 },
	}
	t.cfg = tlCfg
	scope := func() metrics.Scope { return metrics.NoopScope(metrics.Matching) }
	t.fwdr = newForwarder(&t.cfg.forwarderConfig, t.taskList, shared.TaskListKindNormal, t.client, scope)
	t.matcher = newTaskMatcher(tlCfg, t.fwdr, func() metrics.Scope { return metrics.NoopScope(metrics.Matching) })
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
	task := newInternalTask(t.newTaskInfo(), nil, "", true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	syncMatch, err := t.matcher.Offer(ctx, task)
	cancel()
	t.NoError(err)
	t.True(syncMatch)
}

func (t *MatcherTestSuite) TestRemoteSyncMatch() {
	<-t.fwdr.PollReqTokenC()

	pollSigC := make(chan struct{})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		<-pollSigC
		task, err := t.matcher.Poll(ctx)
		cancel()
		if err == nil {
			task.finish(nil)
		}
	}()

	task := newInternalTask(t.newTaskInfo(), nil, "", true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	var err error
	var syncMatch bool
	var req *gen.AddDecisionTaskRequest
	t.client.EXPECT().AddDecisionTask(gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *gen.AddDecisionTaskRequest) {
			req = arg1
			task.forwardedFrom = req.GetForwardedFrom()
			close(pollSigC)
			time.Sleep(10 * time.Millisecond)
			syncMatch, err = t.matcher.Offer(ctx, task)
		},
	).Return(nil)

	_, err0 := t.matcher.Offer(ctx, task)
	t.NoError(err0)
	cancel()
	t.NotNil(req)
	t.NoError(err)
	t.True(syncMatch)
	t.Equal(t.taskList.name, req.GetForwardedFrom())
	t.Equal(t.taskList.Parent(20), req.GetTaskList().GetName())
}

func (t *MatcherTestSuite) TestSyncMatchFailure() {
	task := newInternalTask(t.newTaskInfo(), nil, "", true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	var req *gen.AddDecisionTaskRequest
	t.client.EXPECT().AddDecisionTask(gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *gen.AddDecisionTaskRequest) {
			req = arg1
		},
	).Return(errMatchingHostThrottle)

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
	task := newInternalQueryTask(uuid.New(), &gen.QueryWorkflowRequest{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := t.matcher.OfferQuery(ctx, task)
	cancel()
	t.NoError(err)
	t.Nil(resp)
}

func (t *MatcherTestSuite) TestQueryRemoteSyncMatch() {
	<-t.fwdr.PollReqTokenC()
	addToken := <-t.fwdr.AddReqTokenC()

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

	task := newInternalQueryTask(uuid.New(), &gen.QueryWorkflowRequest{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	var req *gen.QueryWorkflowRequest
	t.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *gen.QueryWorkflowRequest) {
			req = arg1
			task.forwardedFrom = req.GetForwardedFrom()
			close(pollSigC)
			time.Sleep(10 * time.Millisecond)
			t.matcher.OfferQuery(ctx, task)
		},
	).Return(&shared.QueryWorkflowResponse{QueryResult: []byte("answer")}, nil)

	go func() {
		time.Sleep(time.Millisecond * 5)
		addToken.release()
	}()

	result, err := t.matcher.OfferQuery(ctx, task)
	cancel()
	t.NotNil(req)
	t.NoError(err)
	t.NotNil(result)
	t.Equal("answer", string(result.QueryResult))
	t.Equal(t.taskList.name, req.GetForwardedFrom())
	t.Equal(t.taskList.Parent(20), req.GetTaskList().GetName())
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

	task := newInternalQueryTask(uuid.New(), &gen.QueryWorkflowRequest{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	var req *gen.QueryWorkflowRequest
	t.client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *gen.QueryWorkflowRequest) {
			req = arg1
			close(pollSigC)
			time.Sleep(10 * time.Millisecond)
		},
	).Return(nil, errMatchingHostThrottle)

	result, err := t.matcher.OfferQuery(ctx, task)
	cancel()
	t.NotNil(req)
	t.NoError(err)
	t.Nil(result)
	t.True(matched)
}

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
	task := newInternalTask(t.newTaskInfo(), nil, "", false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := t.matcher.MustOffer(ctx, task)
	cancel()
	t.NoError(err)
}

func (t *MatcherTestSuite) TestMustOfferRemoteMatch() {
	<-t.fwdr.PollReqTokenC()

	pollSigC := make(chan struct{})

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		<-pollSigC
		task, err := t.matcher.Poll(ctx)
		cancel()
		if err == nil {
			task.finish(nil)
		}
	}()

	task := newInternalTask(t.newTaskInfo(), nil, "", true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	var err error
	var syncMatch bool
	var req *gen.AddDecisionTaskRequest
	t.client.EXPECT().AddDecisionTask(gomock.Any(), gomock.Any()).Return(errMatchingHostThrottle).Times(1)
	t.client.EXPECT().AddDecisionTask(gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *gen.AddDecisionTaskRequest) {
			req = arg1
			task.forwardedFrom = req.GetForwardedFrom()
			close(pollSigC)
			time.Sleep(10 * time.Millisecond)
			syncMatch, err = t.matcher.Offer(ctx, task)
		},
	).Return(nil)

	t.NoError(t.matcher.MustOffer(ctx, task))
	cancel()
	t.NotNil(req)
	t.NoError(err)
	t.True(syncMatch)
	t.Equal(t.taskList.name, req.GetForwardedFrom())
	t.Equal(t.taskList.Parent(20), req.GetTaskList().GetName())
}

func (t *MatcherTestSuite) TestRemotePoll() {
	pollToken := <-t.fwdr.PollReqTokenC()

	var req *gen.PollForDecisionTaskRequest
	t.client.EXPECT().PollForDecisionTask(gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *gen.PollForDecisionTaskRequest) {
			req = arg1
		},
	).Return(&gen.PollForDecisionTaskResponse{}, nil)

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

	var req *gen.PollForDecisionTaskRequest
	t.client.EXPECT().PollForDecisionTask(gomock.Any(), gomock.Any()).Do(
		func(arg0 context.Context, arg1 *gen.PollForDecisionTaskRequest) {
			req = arg1
		},
	).Return(&gen.PollForDecisionTaskResponse{}, nil)

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

func (t *MatcherTestSuite) newDomainCache() cache.DomainCache {
	entry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{Name: "test-domain"},
		&persistence.DomainConfig{},
		"",
		nil)
	dc := cache.NewMockDomainCache(t.controller)
	dc.EXPECT().GetDomainByID(gomock.Any()).Return(entry, nil).AnyTimes()
	return dc
}

func (t *MatcherTestSuite) newTaskInfo() *persistence.TaskInfo {
	return &persistence.TaskInfo{
		DomainID:               uuid.New(),
		WorkflowID:             uuid.New(),
		RunID:                  uuid.New(),
		TaskID:                 rand.Int63(),
		ScheduleID:             rand.Int63(),
		ScheduleToStartTimeout: rand.Int31(),
	}
}
