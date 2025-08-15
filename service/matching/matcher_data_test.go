package matching

import (
	"context"
	"errors"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type MatcherDataSuite struct {
	suite.Suite
	ts *clock.EventTimeSource
	md matcherData
}

func TestMatcherDataSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(MatcherDataSuite))
}

func (s *MatcherDataSuite) SetupTest() {
	cfg := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY),
		NewConfig(dynamicconfig.NewNoopCollection()),
		"nsname",
	)
	logger := testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	s.ts = clock.NewEventTimeSource().Update(time.Now())
	s.ts.UseAsyncTimers(true)
	rateLimitManager := newRateLimitManager(&mockUserDataManager{}, cfg, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.md = newMatcherData(cfg, logger, s.ts, true, rateLimitManager)
}

func (s *MatcherDataSuite) now() time.Time {
	return s.ts.Now()
}

func (s *MatcherDataSuite) pollRealTime(timeout time.Duration) *matchResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.pollContext(ctx)
}

func (s *MatcherDataSuite) pollFakeTime(timeout time.Duration) *matchResult {
	ctx, cancel := clock.ContextWithTimeout(context.Background(), timeout, s.ts)
	defer cancel()
	return s.pollContext(ctx)
}

func (s *MatcherDataSuite) pollContext(ctx context.Context) *matchResult {
	return s.md.EnqueuePollerAndWait([]context.Context{ctx}, &waitingPoller{
		startTime:    s.now(),
		forwardCtx:   ctx,
		pollMetadata: &pollMetadata{},
	})
}

func (s *MatcherDataSuite) queryFakeTime(duration time.Duration, respC chan<- taskResponse) {
	ctx, cancel := clock.ContextWithTimeout(context.Background(), duration, s.ts)
	defer cancel()
	t := s.newQueryTask("1")
	tres := s.md.EnqueueTaskAndWait([]context.Context{ctx}, t)
	s.NoError(tres.ctxErr)

	resp, ok := t.getResponse()
	s.True(ok)
	respC <- resp
}

func (s *MatcherDataSuite) newSyncTask(fwdInfo *taskqueuespb.TaskForwardInfo) *internalTask {
	t := &persistencespb.TaskInfo{
		CreateTime: timestamppb.New(s.now()),
	}
	return newInternalTaskForSyncMatch(t, fwdInfo)
}

func (s *MatcherDataSuite) newQueryTask(id string) *internalTask {
	return newInternalQueryTask(id, &matchingservice.QueryWorkflowRequest{})
}

func (s *MatcherDataSuite) newBacklogTask(id int64, age time.Duration, f func(*internalTask, taskResponse)) *internalTask {
	return s.newBacklogTaskWithPriority(id, age, f, nil)
}

func (s *MatcherDataSuite) newBacklogTaskWithPriority(id int64, age time.Duration, f func(*internalTask, taskResponse), pri *commonpb.Priority) *internalTask {
	t := &persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{
			CreateTime: timestamppb.New(s.now().Add(-age)),
			Priority:   pri,
		},
		TaskId: id,
	}
	return newInternalTaskFromBacklog(t, f)
}

func (s *MatcherDataSuite) waitForPollers(n int) {
	s.Eventually(func() bool {
		s.md.lock.Lock()
		defer s.md.lock.Unlock()
		return s.md.pollers.Len() >= n
	}, time.Second, time.Millisecond)
}

func (s *MatcherDataSuite) waitForTasks(n int) {
	s.Eventually(func() bool {
		s.md.lock.Lock()
		defer s.md.lock.Unlock()
		return s.md.tasks.Len() >= n
	}, time.Second, time.Millisecond)
}

func (s *MatcherDataSuite) TestMatchBacklogTask() {
	poller := &waitingPoller{startTime: s.now()}

	// no task yet, context should time out
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	pres := s.md.EnqueuePollerAndWait([]context.Context{ctx}, poller)
	s.Error(context.DeadlineExceeded, pres.ctxErr)
	s.Equal(0, pres.ctxErrIdx)

	// add a task
	gotResponse := false
	done := func(t *internalTask, tres taskResponse) {
		gotResponse = true
		s.NoError(tres.startErr)
	}
	t := s.newBacklogTask(123, 0, done)
	s.md.EnqueueTaskNoWait(t)

	// now should match with task
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	pres = s.md.EnqueuePollerAndWait([]context.Context{ctx}, poller)
	s.NoError(pres.ctxErr)
	s.Equal(t, pres.task)

	// finish task
	pres.task.finish(nil, true)
	s.True(gotResponse)

	// one more, context should time out again. note two contexts this time.
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	pres = s.md.EnqueuePollerAndWait([]context.Context{context.Background(), ctx}, poller)
	s.Error(context.DeadlineExceeded, pres.ctxErr)
	s.Equal(1, pres.ctxErrIdx, "deadline context was index 1")
}

func (s *MatcherDataSuite) TestMatchTaskImmediately() {
	t := s.newSyncTask(nil)

	// no match yet
	canSyncMatch, gotSyncMatch := s.md.MatchTaskImmediately(t)
	s.True(canSyncMatch)
	s.False(gotSyncMatch)

	// poll in a goroutine
	ch := make(chan *matchResult, 1)
	go func() {
		poller := &waitingPoller{startTime: s.now()}
		ch <- s.md.EnqueuePollerAndWait(nil, poller)
	}()

	// wait until poller queued
	s.waitForPollers(1)

	// should match this time
	canSyncMatch, gotSyncMatch = s.md.MatchTaskImmediately(t)
	s.True(canSyncMatch)
	s.True(gotSyncMatch)

	// check match
	pres := <-ch
	s.NoError(pres.ctxErr)
	s.Equal(t, pres.task)
}

func (s *MatcherDataSuite) TestMatchTaskImmediatelyDisabledBacklog() {
	// register some backlog with old tasks
	s.md.EnqueueTaskNoWait(s.newBacklogTask(123, 10*time.Minute, nil))

	t := s.newSyncTask(nil)
	canSyncMatch, gotSyncMatch := s.md.MatchTaskImmediately(t)
	s.False(canSyncMatch)
	s.False(gotSyncMatch)
}

func (s *MatcherDataSuite) TestQuery() {
	respC := make(chan taskResponse)
	go s.queryFakeTime(time.Second, respC)

	pres := s.pollFakeTime(time.Second)
	s.NotNil(pres.task)
	s.True(pres.task.isQuery())
	// wake up getResponse. use some error just to check it's passed through.
	someError := errors.New("some error")
	pres.task.finish(someError, true)

	resp := <-respC
	s.False(resp.forwarded)
	s.ErrorIs(resp.startErr, someError)
}

func (s *MatcherDataSuite) TestQueryForwardNil() {
	respC := make(chan taskResponse)
	go s.queryFakeTime(time.Second, respC)

	pres := s.pollFakeTime(time.Second)
	s.NotNil(pres.task)
	s.True(pres.task.isQuery())
	var fres *matchingservice.QueryWorkflowResponse
	pres.task.finishForward(fres, nil, true)

	resp := <-respC
	s.True(resp.forwarded)
	s.NoError(resp.forwardErr)
	s.True(resp.forwardRes != nil) // typed nil
	s.Nil(resp.forwardRes.(*matchingservice.QueryWorkflowResponse))
}

func (s *MatcherDataSuite) TestQueryForwardError() {
	respC := make(chan taskResponse)
	go s.queryFakeTime(time.Second, respC)

	pres := s.pollFakeTime(time.Second)
	s.NotNil(pres.task)
	s.True(pres.task.isQuery())
	var fres *matchingservice.QueryWorkflowResponse
	someError := errors.New("some error")
	pres.task.finishForward(fres, someError, true)

	resp := <-respC
	s.True(resp.forwarded)
	s.ErrorIs(resp.forwardErr, someError)
}

func (s *MatcherDataSuite) TestQueryForwardResponse() {
	respC := make(chan taskResponse)
	go s.queryFakeTime(time.Second, respC)

	pres := s.pollFakeTime(time.Second)
	s.NotNil(pres.task)
	s.True(pres.task.isQuery())
	fres := &matchingservice.QueryWorkflowResponse{
		QueryResult: payloads.EncodeString("ok"),
	}
	pres.task.finishForward(fres, nil, true)

	resp := <-respC
	s.True(resp.forwarded)
	s.NoError(resp.forwardErr)
	s.Contains(payloads.ToString(resp.forwardRes.(*matchingservice.QueryWorkflowResponse).QueryResult), "ok")
}

func (s *MatcherDataSuite) TestTaskForward() {
	someError := errors.New("some error")
	// one task forwarder
	go func() {
		poller := &waitingPoller{isTaskForwarder: true}
		pres := s.md.EnqueuePollerAndWait(nil, poller)
		s.NotNil(pres.task)
		// use some error just to check it's passed through
		pres.task.finishForward(nil, someError, true)
	}()
	// two normal pollers
	go s.pollFakeTime(time.Second)
	go s.pollFakeTime(time.Second)

	s.waitForPollers(3)

	t1 := s.newSyncTask(nil)
	t2 := s.newSyncTask(nil)
	t3 := s.newSyncTask(nil)

	// two tasks will get matched with normal pollers first
	tres := s.md.EnqueueTaskAndWait(nil, t1)
	s.NotNil(tres.poller)
	s.False(tres.poller.isTaskForwarder)

	tres = s.md.EnqueueTaskAndWait(nil, t2)
	s.NotNil(tres.poller)
	s.False(tres.poller.isTaskForwarder)

	// third task will get matched with forwarder
	tres = s.md.EnqueueTaskAndWait(nil, t3)
	s.NotNil(tres.poller)
	s.True(tres.poller.isTaskForwarder)
	fres, ok := t3.getResponse()
	s.True(ok)
	s.True(fres.forwarded)
	s.ErrorIs(fres.forwardErr, someError)
}

func (s *MatcherDataSuite) TestRateLimitedBacklog() {
	s.md.rateLimitManager.SetEffectiveRPSAndSourceForTesting(10.0, enumspb.RATE_LIMIT_SOURCE_API)
	s.md.rateLimitManager.UpdateSimpleRateLimitForTesting(300 * time.Millisecond)

	// register some backlog with old tasks
	for i := range 100 {
		s.md.EnqueueTaskNoWait(s.newBacklogTask(123+int64(i), 0, nil))
	}

	start := s.ts.Now()

	// start 10 poll loops to poll them
	var running atomic.Int64
	var lastTask atomic.Int64
	for range 10 {
		running.Add(1)
		go func() {
			defer running.Add(-1)
			for {
				if pres := s.pollFakeTime(time.Second); pres.ctxErr != nil {
					return
				}
				lastTask.Store(s.now().UnixNano())
			}
		}()
	}

	// advance fake time until done
	for running.Load() > 0 {
		s.ts.Advance(time.Duration(rand.Int63n(int64(1 * time.Millisecond))))
		gosched(3)
	}

	elapsed := time.Unix(0, lastTask.Load()).Sub(start)
	s.Greater(elapsed, 9*time.Second)
	// with very unlucky scheduling, we might end up taking longer to poll the tasks
	s.Less(elapsed, 20*time.Second)
}

func (s *MatcherDataSuite) TestPerKeyRateLimit() {
	s.md.rateLimitManager.SetFairnessKeyRateLimitDefaultForTesting(10.0, enumspb.RATE_LIMIT_SOURCE_API)
	s.md.rateLimitManager.UpdatePerKeySimpleRateLimitForTesting(300 * time.Millisecond)
	// register some backlog with three keys
	keys := []string{"key1", "key2", "key3"}
	for i := range 300 {
		t := s.newBacklogTaskWithPriority(123+int64(i), 0, nil, &commonpb.Priority{
			FairnessKey: keys[i%3],
		})
		s.md.EnqueueTaskNoWait(t)
	}

	start := s.ts.Now()

	// start 10 poll loops to poll them
	var running atomic.Int64
	var lastTask atomic.Int64
	for range 10 {
		running.Add(1)
		go func() {
			defer running.Add(-1)
			for {
				if pres := s.pollFakeTime(time.Second); pres.ctxErr != nil {
					return
				}
				lastTask.Store(s.now().UnixNano())
			}
		}()
	}

	// advance fake time until done
	for running.Load() > 0 {
		s.ts.Advance(time.Duration(rand.Int63n(int64(1 * time.Millisecond))))
		gosched(3)
	}

	elapsed := time.Unix(0, lastTask.Load()).Sub(start)
	s.Greater(elapsed, 9*time.Second)
	// with very unlucky scheduling, we might end up taking longer to poll the tasks
	s.Less(elapsed, 20*time.Second)
}

func (s *MatcherDataSuite) TestOrder() {
	t1 := s.newBacklogTaskWithPriority(1, 0, nil, &commonpb.Priority{PriorityKey: 1})
	t2 := s.newBacklogTaskWithPriority(2, 0, nil, &commonpb.Priority{PriorityKey: 2})
	t3 := s.newBacklogTaskWithPriority(3, 0, nil, &commonpb.Priority{PriorityKey: 3})
	tf := newPollForwarderTask()

	s.md.EnqueueTaskNoWait(t3)
	s.md.EnqueueTaskNoWait(tf)
	s.md.EnqueueTaskNoWait(t1)
	s.md.EnqueueTaskNoWait(t2)

	s.Equal(t1, s.pollFakeTime(time.Second).task)
	s.Equal(t2, s.pollFakeTime(time.Second).task)
	s.Equal(t3, s.pollFakeTime(time.Second).task)
	// poll forwarder is last to match, but it does a half-match so we won't see it here
}

func (s *MatcherDataSuite) TestPollForwardSuccess() {
	t1 := s.newBacklogTask(1, 0, nil)
	t2 := s.newBacklogTask(2, 0, nil)

	s.md.EnqueueTaskNoWait(t1)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		tres := s.md.EnqueueTaskAndWait([]context.Context{ctx}, newPollForwarderTask())
		// task is woken up with poller to forward
		s.NotNil(tres.poller)
		// forward succeeded, pass back task
		s.md.FinishMatchAfterPollForward(tres.poller, t2)
	}()

	s.waitForTasks(2)

	s.Equal(t1, s.pollFakeTime(time.Second).task)
	s.Equal(t2, s.pollFakeTime(time.Second).task)
}

func (s *MatcherDataSuite) TestPollForwardFailed() {
	t1 := s.newBacklogTask(1, 0, nil)
	t2 := s.newBacklogTask(2, 0, nil)

	s.md.EnqueueTaskNoWait(t1)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		tres := s.md.EnqueueTaskAndWait([]context.Context{ctx}, newPollForwarderTask())
		// task is woken up with poller to forward
		s.NotNil(tres.poller)
		// there's a new task in the meantime
		s.md.EnqueueTaskNoWait(t2)
		// forward failed, re-enqueue poller so it can match again
		s.md.ReenqueuePollerIfNotMatched(tres.poller)
	}()

	s.waitForTasks(2)

	s.Equal(t1, s.pollFakeTime(time.Second).task)
	s.Equal(t2, s.pollFakeTime(time.Second).task)
}

func (s *MatcherDataSuite) TestPollForwardFailedTimedOut() {
	t1 := s.newBacklogTask(1, 0, nil)
	t2 := s.newBacklogTask(2, 0, nil)

	s.md.EnqueueTaskNoWait(t1)

	done := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		tres := s.md.EnqueueTaskAndWait([]context.Context{ctx}, newPollForwarderTask())
		// task is woken up with poller to forward
		s.NotNil(tres.poller)
		// there's a new task in the meantime
		s.md.EnqueueTaskNoWait(t2)
		time.Sleep(11 * time.Millisecond) // nolint:forbidigo
		// but we waited too long, poller timed out, so this does nothing (but doesn't crash or assert)
		s.md.ReenqueuePollerIfNotMatched(tres.poller)
		done <- struct{}{}
	}()

	s.waitForTasks(2)

	s.Equal(t1, s.pollRealTime(10*time.Millisecond).task)
	s.Error(s.pollRealTime(10 * time.Millisecond).ctxErr)

	<-done
}

func (s *MatcherDataSuite) TestReprocessTasks() {
	// add 100 tasks in reverse order
	for i := range 100 {
		s.md.EnqueueTaskNoWait(s.newBacklogTask(int64(101-i), 0, nil))
	}

	// remove 1/4 of them
	removed := s.md.ReprocessTasks(func(t *internalTask) bool {
		return t.event.TaskId%4 == 0
	})

	s.Equal(25, len(removed))
	for _, t := range removed {
		s.True(t.event.TaskId%4 == 0)
		s.NotNil(t.matchResult)
		s.Equal(errReprocessTask, t.matchResult.ctxErr)
	}

	// pull out remaining ones, should be in order
	prev := int64(0)
	for range 75 {
		t := s.pollRealTime(time.Microsecond).task
		s.NotNil(t)
		s.False(t.event.TaskId%4 == 0)
		s.Greater(t.event.TaskId, prev)
		prev = t.event.TaskId
	}

	// no more
	res := s.pollRealTime(time.Microsecond)
	s.Nil(res.task)
	s.Error(res.ctxErr)
}

// simple limiter tests

func TestSimpleLimiter(t *testing.T) {
	p := makeSimpleLimiterParams(10, time.Second)

	base := time.Now().UnixNano()
	now := base
	var ready simpleLimiter

	// can consume 11 tokens immediately (1 since we're starting from 0 and 10 burst)
	for range 11 {
		require.GreaterOrEqual(t, now, ready)
		ready = ready.consume(p, now, 1)
	}
	// now not ready anymore
	require.Less(t, now, ready)

	// after 100 ms, we can consume one more
	now += int64(99 * time.Millisecond)
	require.Less(t, now, ready)
	now += int64(1 * time.Millisecond)
	require.GreaterOrEqual(t, now, ready)
	ready = ready.consume(p, now, 1)
	require.Less(t, now, ready)
}

func TestSimpleLimiterOverTime(t *testing.T) {
	p := makeSimpleLimiterParams(10, time.Second)

	base := time.Now().UnixNano()
	now := base
	var ready simpleLimiter

	consumed := int64(0)
	for range 10000 {
		// sleep for some random time, average < 100ms, so we are limited on average
		// but have some gaps too.
		now += (70 + rand.Int63n(50)) * int64(time.Millisecond)

		if now >= int64(ready) {
			ready = ready.consume(p, now, 1)
			consumed++
		}
	}

	effectiveRate := float64(consumed) / float64(now-base) * float64(time.Second)
	require.InEpsilon(t, 10, effectiveRate, 0.01)
}

func TestSimpleLimiterRecycle(t *testing.T) {
	p := makeSimpleLimiterParams(10, time.Second)

	base := time.Now().UnixNano()
	now := base
	var ready simpleLimiter

	consumed := int64(0)
	for range 10000 {
		// sleep for some random time, always < 100ms, so we are always limited
		now += (30 + rand.Int63n(30)) * int64(time.Millisecond)

		if now >= int64(ready) {
			ready = ready.consume(p, now, 1)
			consumed++

			// 20% of the time, recycle the token we took
			if rand.Intn(100) < 20 {
				now += int64(5 * time.Millisecond)
				ready = ready.consume(p, now, -1)
				consumed--
			}
		}
	}

	effectiveRate := float64(consumed) / float64(now-base) * float64(time.Second)
	require.InEpsilon(t, 10, effectiveRate, 0.01)
}

func TestSimpleLimiterUnlimited(t *testing.T) {
	now := time.Now().UnixNano()
	var ready simpleLimiter

	pInf := makeSimpleLimiterParams(1e12, 0)
	require.False(t, pInf.never())
	require.False(t, pInf.limited())

	for range 1000 {
		ready = ready.consume(pInf, now, 1)
		require.LessOrEqual(t, ready.delay(now), time.Duration(0))
	}
}

func TestSimpleLimiterLowToHigh(t *testing.T) {
	for _, lowRate := range []float64{
		0,
		1e-8, // 1 per 1000+ days
	} {
		pLow := makeSimpleLimiterParams(lowRate, time.Second)
		require.True(t, pLow.never() == (lowRate == 0))

		now := time.Now().UnixNano()
		var ready simpleLimiter
		ready = ready.consume(pLow, now, 1)
		// not ready yet
		require.Greater(t, ready.delay(now), time.Duration(0))
		// not ready even after 1 day
		require.Greater(t, ready.delay(now+(24*time.Hour).Nanoseconds()), time.Duration(0))

		// try clipping using the low limit
		ready = ready.clip(pLow, now, 1)
		// still not ready now or in 1 day
		require.Greater(t, ready.delay(now), time.Duration(0))
		require.Greater(t, ready.delay(now+(24*time.Hour).Nanoseconds()), time.Duration(0))

		// switch to higher rate limit
		pHigh := makeSimpleLimiterParams(10, time.Second)
		require.False(t, pHigh.never())
		require.True(t, pHigh.limited())

		// clip to high limit
		ready = ready.clip(pHigh, now, 1)
		// not ready yet
		require.Greater(t, ready.delay(now), time.Duration(0))
		// ready within one minute
		require.Less(t, ready.delay(now+time.Minute.Nanoseconds()), time.Duration(0))
	}
}

func FuzzMatcherData(f *testing.F) {
	f.Fuzz(func(t *testing.T, tape []byte) {
		cfg := newTaskQueueConfig(
			tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY),
			NewConfig(dynamicconfig.NewNoopCollection()),
			"nsname",
		)
		ts := clock.NewEventTimeSource()
		ts.UseAsyncTimers(true)
		logger := log.NewNoopLogger()
		rateLimitManager := newRateLimitManager(&mockUserDataManager{}, cfg, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
		md := newMatcherData(cfg, logger, ts, true, rateLimitManager)

		next := func() int {
			if len(tape) == 0 {
				return -1
			}
			v := tape[0]
			tape = tape[1:]
			return int(v)
		}
		randms := func(n int) time.Duration { return time.Duration(next()%n+1) * time.Millisecond }
		randage := func(n int) time.Duration { return -time.Duration(next()%n) * time.Second }

		tid := int64(0)
		var pollForwarders, taskForwarders atomic.Int64
		const ops = 20

	loop:
		for {
			switch (next() + 1) % ops {
			case 0:
				break loop

			case 1: // add backlog task
				tid++
				ati := &persistencespb.AllocatedTaskInfo{
					Data: &persistencespb.TaskInfo{
						CreateTime: timestamppb.New(ts.Now().Add(randage(10))),
					},
					TaskId: tid,
				}
				md.EnqueueTaskNoWait(newInternalTaskFromBacklog(ati, nil))

			case 2: // add backlog task with priority
				tid++
				ati := &persistencespb.AllocatedTaskInfo{
					Data: &persistencespb.TaskInfo{
						CreateTime: timestamppb.New(ts.Now().Add(randage(10))),
						Priority: &commonpb.Priority{
							PriorityKey: int32(1 + next()%5),
						},
					},
					TaskId: tid,
				}
				md.EnqueueTaskNoWait(newInternalTaskFromBacklog(ati, nil))

			case 3: // add poller
				timeout := randms(100)
				queryOnly := next()%8 == 0
				go func() {
					ctx, cancel := clock.ContextWithTimeout(context.Background(), timeout, ts)
					defer cancel()
					md.EnqueuePollerAndWait([]context.Context{ctx}, &waitingPoller{
						startTime:    ts.Now(),
						forwardCtx:   ctx, // TODO: not always forwardable?
						pollMetadata: &pollMetadata{},
						queryOnly:    queryOnly,
					})
				}()

			case 4: // add query task
				timeout := randms(100)
				go func() {
					ctx, cancel := clock.ContextWithTimeout(context.Background(), timeout, ts)
					defer cancel()
					t := newInternalQueryTask("123", nil)
					t.forwardCtx = ctx
					md.EnqueueTaskAndWait([]context.Context{ctx}, t)
				}()

			case 5: // add poll forwarder
				if pollForwarders.Load() >= 2 {
					continue
				}
				pollForwarders.Add(1)
				sleepTime := randms(100)
				go func() {
					defer pollForwarders.Add(-1)
					res := md.EnqueueTaskAndWait(nil, newPollForwarderTask())
					softassert.That(md.logger, res.ctxErr == nil && res.poller != nil, "")
					ts.Sleep(sleepTime)
					t := &persistencespb.TaskInfo{
						CreateTime: timestamppb.New(ts.Now()),
					}
					md.FinishMatchAfterPollForward(res.poller, newInternalTaskForSyncMatch(t, nil))
				}()

			case 6: // add task forwarder
				if taskForwarders.Load() >= 2 {
					continue
				}
				taskForwarders.Add(1)
				sleepTime := randms(100)
				go func() {
					defer taskForwarders.Add(-1)
					res := md.EnqueuePollerAndWait(nil, &waitingPoller{isTaskForwarder: true})
					softassert.That(md.logger, res.ctxErr == nil && res.task != nil, "")
					ts.Sleep(sleepTime)
					res.task.finishForward(nil, nil, true)
				}()

			case ops - 1: // jump ahead, just to speed things up
				ts.AdvanceNext()
				gosched(3)

			default:
				ts.Advance(time.Millisecond)
				gosched(3)
			}
		}

		for ts.NumTimers() > 0 {
			ts.AdvanceNext()
			gosched(3)
		}
	})
}

func gosched(n int) {
	for range n {
		runtime.Gosched()
	}
}
