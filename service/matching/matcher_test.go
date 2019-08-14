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

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	gen "github.com/uber/cadence/.gen/go/matching"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type MatcherTestSuite struct {
	suite.Suite
	client   *mocks.MatchingClient
	fwdr     *Forwarder
	cfg      *taskListConfig
	taskList *taskListID
	matcher  *TaskMatcher
}

func TestMatcherSuite(t *testing.T) {
	suite.Run(t, new(MatcherTestSuite))
}

func (t *MatcherTestSuite) SetupTest() {
	t.client = new(mocks.MatchingClient)
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
	t.client.On("AddDecisionTask", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		req = args.Get(1).(*gen.AddDecisionTaskRequest)
		task.forwardedFrom = req.GetForwardedFrom()
		close(pollSigC)
		time.Sleep(10 * time.Millisecond)
		syncMatch, err = t.matcher.Offer(ctx, task)
	}).Return(nil)

	t.matcher.Offer(ctx, task)
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
	t.client.On("AddDecisionTask", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		req = args.Get(1).(*gen.AddDecisionTaskRequest)
	}).Return(errMatchingHostThrottle)

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
	t.client.On("QueryWorkflow", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		req = args.Get(1).(*gen.QueryWorkflowRequest)
		task.forwardedFrom = req.GetForwardedFrom()
		close(pollSigC)
		time.Sleep(10 * time.Millisecond)
		t.matcher.OfferQuery(ctx, task)
	}).Return(&shared.QueryWorkflowResponse{QueryResult: []byte("answer")}, nil)

	go func() {
		time.Sleep(time.Millisecond * 5)
		addToken.release()
	}()

	result, err := t.matcher.OfferQuery(ctx, task)
	cancel()
	t.NotNil(req)
	t.NoError(err)
	t.NotNil(result)
	t.Equal("answer", string(result))
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
	t.client.On("QueryWorkflow", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		req = args.Get(1).(*gen.QueryWorkflowRequest)
		close(pollSigC)
		time.Sleep(10 * time.Millisecond)
	}).Return(nil, errMatchingHostThrottle)

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
	t.client.On("AddDecisionTask", mock.Anything, mock.Anything).Return(errMatchingHostThrottle).Once()
	t.client.On("AddDecisionTask", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		req = args.Get(1).(*gen.AddDecisionTaskRequest)
		task.forwardedFrom = req.GetForwardedFrom()
		close(pollSigC)
		time.Sleep(10 * time.Millisecond)
		syncMatch, err = t.matcher.Offer(ctx, task)
	}).Return(nil)

	t.matcher.MustOffer(ctx, task)
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
	t.client.On("PollForDecisionTask", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		req = args.Get(1).(*gen.PollForDecisionTaskRequest)
	}).Return(&gen.PollForDecisionTaskResponse{}, nil)

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
	t.client.On("PollForDecisionTask", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		req = args.Get(1).(*gen.PollForDecisionTaskRequest)
	}).Return(&gen.PollForDecisionTaskResponse{}, nil)

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
	dc := &cache.DomainCacheMock{}
	dc.On("GetDomainByID", mock.Anything).Return(entry, nil)
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
