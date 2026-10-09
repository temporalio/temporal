package matching

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PriMatcherSuite struct {
	suite.Suite
	controller *gomock.Controller
	logger     log.Logger
}

func TestPriMatcherSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PriMatcherSuite))
}

func (s *PriMatcherSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
}

var testMatcherTaskQueue = tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW)

// testMatcherOpts configures newTestMatcher. The zero value is a root partition with no
// client and no validator.
type testMatcherOpts struct {
	// partition of testMatcherTaskQueue to use. Defaults to the root partition. Child
	// partitions get a forwarder that uses client.
	partition *tqid.NormalPartition
	client    matchingservice.MatchingServiceClient
	validator taskValidator
	// config, if set, can modify the task queue config before the matcher is created.
	config func(*taskQueueConfig)
}

// newTestMatcher returns a priTaskMatcher for tests. It's not started: the caller should call
// Start (after adding any initial tasks) and Stop.
func (s *PriMatcherSuite) newTestMatcher(ctx context.Context, opts testMatcherOpts) *priTaskMatcher {
	cfg := newTaskQueueConfig(testMatcherTaskQueue, NewConfig(dynamicconfig.NewNoopCollection()), "nsname")
	if opts.config != nil {
		opts.config(cfg)
	}
	partition := opts.partition
	if partition == nil {
		partition = testMatcherTaskQueue.RootPartition()
	}
	var fwdr *priForwarder
	if partition.IsChild() {
		var err error
		fwdr, err = newPriForwarder(&cfg.forwarderConfig, UnversionedQueueKey(partition), opts.client, testhooks.TestHooks{})
		if err != nil {
			panic(err) // only fails for non-normal partitions
		}
	}
	rateLimitManager := newRateLimitManager(&mockUserDataManager{}, cfg, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	rateLimitManager.Start()
	return newPriTaskMatcher(
		ctx,
		cfg,
		partition,
		fwdr,
		opts.client,
		opts.validator,
		s.logger,
		metrics.NoopMetricsHandler,
		rateLimitManager,
		func() {}, // onRateLimited
		func() {}, // markAlive
	)
}

// TestValidatorWorksOnRoot tests that the validator goroutine can pick up tasks
// on a root partition (where there is no forwarder).
func (s *PriMatcherSuite) TestValidatorWorksOnRoot() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var validatorValidatedTask atomic.Bool

	// record validator calls
	mockValidator := NewMocktaskValidator(s.controller)
	mockValidator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).DoAndReturn(func(task *persistencespb.AllocatedTaskInfo, taskType enumspb.TaskQueueType) bool {
		validatorValidatedTask.Store(true)
		return true // task is valid
	})

	tm := s.newTestMatcher(ctx, testMatcherOpts{validator: mockValidator})
	tm.Start()
	defer tm.Stop()

	completionCalled := make(chan taskResponse)
	task := newInternalTaskFromBacklog(&persistencespb.AllocatedTaskInfo{
		TaskId: 1,
		Data: &persistencespb.TaskInfo{
			CreateTime: timestamppb.Now(),
		},
	}, func(t *internalTask, res taskResponse) {
		completionCalled <- res
	})

	// add the task
	task.resetMatcherState()
	_ = tm.AddTask(task)

	// validator should pick up and check task
	select {
	case res := <-completionCalled:
		// error should be errReprocessTask
		s.ErrorIs(res.err(), errReprocessTask) //nolint:testifylint
	case <-time.After(2 * time.Second):
		s.Fail("Timeout waiting for validator to process task")
	}

	s.True(validatorValidatedTask.Load(), "Validator should have called maybeValidate")
}

// TestForwardPollRetriesOnResourceExhausted verifies that when a child partition's
// ForwardPoll gets a ResourceExhausted error (rate limited), the poller is re-enqueued
// with forwarding still enabled and retries until it succeeds. This is a regression test
// for a bug where ForwardPoll permanently disabled forwarding on transient rate-limit
// errors, causing polls to wait for the full 60s timeout instead of retrying.
func (s *PriMatcherSuite) TestForwardPollRetriesOnResourceExhausted() {
	// Use synctest to virtualize time so the backoff sleep is instant.
	synctest.Test(s.T(), func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockClient := matchingservicemock.NewMockMatchingServiceClient(s.controller)

		// First ForwardPoll call: return ResourceExhausted (simulating rate limit storm).
		// Second call: return a valid task response.
		rateLimitErr := serviceerror.NewResourceExhausted(
			enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, "rate limit exceeded",
		)
		taskToken := []byte("test-task-token")

		gomock.InOrder(
			mockClient.EXPECT().
				PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil, rateLimitErr),
			mockClient.EXPECT().
				PollWorkflowTaskQueue(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(&matchingservice.PollWorkflowTaskQueueResponse{
					TaskToken: taskToken,
				}, nil),
		)

		tm := s.newTestMatcher(ctx, testMatcherOpts{
			partition: testMatcherTaskQueue.NormalPartition(1),
			client:    mockClient,
			config: func(cfg *taskQueueConfig) {
				// Use a generous poll timeout so we can distinguish retry success from timeout.
				cfg.LongPollExpirationInterval = func() time.Duration { return 10 * time.Second }
			},
		})
		tm.Start()
		defer tm.Stop()

		// Poll from the child partition. The forwardPolls goroutine should:
		// 1. Pick up this poller
		// 2. Try ForwardPoll → get ResourceExhausted
		// 3. Re-enqueue poller with forwarding still enabled
		// 4. Try ForwardPoll again → succeed with task token
		// 5. Return the task to the poller
		pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
		defer pollCancel()

		task, err := tm.Poll(pollCtx, &pollMetadata{})
		require.NoError(t, err)
		require.NotNil(t, task, "poll should have received a task via forwarding retry")
		require.True(t, task.isStarted(), "task should be a started (forwarded) task")
		require.Equal(t, taskToken, task.started.workflowTaskInfo.TaskToken)
	})
}

// TestValidatorDrop_SetsDropReason verifies that when the root-partition validator
// rejects a backlog task, it finishes the task with the right drop reason — which
// reader.completeTask records in tasks_dropped: expired_memory for a time-expired task,
// invalid for one that only failed validation.
func (s *PriMatcherSuite) TestValidatorDrop_SetsDropReason() {
	cases := []struct {
		name       string
		expiryTime *timestamppb.Timestamp
		wantReason dropReason
	}{
		{"ExpiredMemory", timestamppb.New(time.Now().Add(-time.Hour)), dropReasonExpiredMemory},
		{"Invalid", nil, dropReasonInvalid},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Validator rejects every task it sees, forcing the drop path.
			mockValidator := NewMocktaskValidator(s.controller)
			mockValidator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).Return(false).AnyTimes()

			// root partition -> validateTasksOnRoot path
			tm := s.newTestMatcher(ctx, testMatcherOpts{validator: mockValidator})
			tm.Start()
			defer tm.Stop()

			completionCalled := make(chan taskResponse, 1)
			task := newInternalTaskFromBacklog(&persistencespb.AllocatedTaskInfo{
				TaskId: 1,
				Data: &persistencespb.TaskInfo{
					CreateTime: timestamppb.Now(),
					ExpiryTime: tc.expiryTime,
				},
			}, func(_ *internalTask, res taskResponse) {
				completionCalled <- res
			})

			task.resetMatcherState()
			_ = tm.AddTask(task)

			select {
			case res := <-completionCalled:
				s.Require().NoError(res.err()) // task is dropped, not reprocessed
				s.Equal(tc.wantReason, res.dropReason)
			case <-time.After(2 * time.Second):
				s.Fail("timed out waiting for validator to drop task")
			}
		})
	}
}

func newTestBacklogTask(age time.Duration, completionFunc func(*internalTask, taskResponse)) *internalTask {
	task := newInternalTaskFromBacklog(&persistencespb.AllocatedTaskInfo{
		TaskId: 1,
		Data: &persistencespb.TaskInfo{
			CreateTime: timestamppb.New(time.Now().Add(-age)),
			ExpiryTime: timestamppb.New(time.Now().Add(time.Hour)),
		},
	}, completionFunc)
	task.resetMatcherState()
	return task
}

// TestChildOfferForwardsToParent checks that a sync match offer on a child partition with no
// local pollers is forwarded to the parent, and that the parent's response determines the
// outcome.
func (s *PriMatcherSuite) TestChildOfferForwardsToParent() {
	cases := []struct {
		name        string
		forwardErr  error
		wantOutcome syncMatchOutcome
	}{
		{"Success", nil, syncMatchSuccess},
		{"Failure", serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, "throttled"), syncMatchNoPoller},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			synctest.Test(s.T(), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				child := testMatcherTaskQueue.NormalPartition(1)
				client := matchingservicemock.NewMockMatchingServiceClient(s.controller)
				var req *matchingservice.AddWorkflowTaskRequest
				client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, r *matchingservice.AddWorkflowTaskRequest, _ ...any) (*matchingservice.AddWorkflowTaskResponse, error) {
						req = r
						return &matchingservice.AddWorkflowTaskResponse{}, tc.forwardErr
					})

				tm := s.newTestMatcher(ctx, testMatcherOpts{partition: child, client: client})
				tm.Start()
				defer tm.Stop()
				synctest.Wait() // let the task forwarder get in place

				task := newInternalTaskForSyncMatch(&persistencespb.TaskInfo{CreateTime: timestamppb.Now()}, nil, 0, nil)
				outcome, err := tm.Offer(ctx, task)
				require.NoError(t, err)
				require.Equal(t, tc.wantOutcome, outcome)
				require.NotNil(t, req)
				require.Equal(t, testMatcherTaskQueue.RootPartition().RpcName(), req.GetTaskQueue().GetName())
				require.Equal(t, child.RpcName(), req.GetForwardInfo().GetSourcePartition())
				require.Equal(t, enumsspb.TASK_SOURCE_HISTORY, req.GetForwardInfo().GetTaskSource())
			})
		})
	}
}

// TestChildOfferQueryForwardsToParent checks that a query offered on a child partition with no
// local pollers is forwarded to the parent, and the parent's result is returned.
func (s *PriMatcherSuite) TestChildOfferQueryForwardsToParent() {
	someErr := serviceerror.NewInternal("query failed")
	cases := []struct {
		name       string
		forwardErr error
	}{
		{"Success", nil},
		{"Failure", someErr},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			synctest.Test(s.T(), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				child := testMatcherTaskQueue.NormalPartition(1)
				client := matchingservicemock.NewMockMatchingServiceClient(s.controller)
				var resp *matchingservice.QueryWorkflowResponse
				if tc.forwardErr == nil {
					resp = &matchingservice.QueryWorkflowResponse{QueryResult: payloads.EncodeString("answer")}
				}
				var req *matchingservice.QueryWorkflowRequest
				client.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, r *matchingservice.QueryWorkflowRequest, _ ...any) (*matchingservice.QueryWorkflowResponse, error) {
						req = r
						return resp, tc.forwardErr
					})

				tm := s.newTestMatcher(ctx, testMatcherOpts{partition: child, client: client})
				tm.Start()
				defer tm.Stop()
				synctest.Wait()

				queryCtx, queryCancel := context.WithTimeout(ctx, 10*time.Second)
				defer queryCancel()
				task := newInternalQueryTask("query-id", &matchingservice.QueryWorkflowRequest{})
				result, err := tm.OfferQuery(queryCtx, task)
				require.NotNil(t, req)
				require.Equal(t, testMatcherTaskQueue.RootPartition().RpcName(), req.GetTaskQueue().GetName())
				require.Equal(t, child.RpcName(), req.GetForwardInfo().GetSourcePartition())
				if tc.forwardErr != nil {
					require.ErrorIs(t, err, tc.forwardErr)
					return
				}
				require.NoError(t, err)
				var answer string
				require.NoError(t, payloads.Decode(result.GetQueryResult(), &answer))
				require.Equal(t, "answer", answer)
			})
		})
	}
}

// TestRootOfferBlocksOnlyForForwardedBacklogTasks checks that the root partition only waits for a
// poller when offered a task that was forwarded from a child's backlog (the child is waiting on
// the result); other sync match offers return immediately so the caller can spool the task. If
// the root has a non-negligible backlog of its own, it doesn't accept the forwarded task at all.
func (s *PriMatcherSuite) TestRootOfferBlocksOnlyForForwardedBacklogTasks() {
	childForwardInfo := func(source enumsspb.TaskSource) *taskqueuespb.TaskForwardInfo {
		return &taskqueuespb.TaskForwardInfo{
			SourcePartition: testMatcherTaskQueue.NormalPartition(1).RpcName(),
			TaskSource:      source,
		}
	}
	newSyncTask := func(fwdInfo *taskqueuespb.TaskForwardInfo) *internalTask {
		return newInternalTaskForSyncMatch(&persistencespb.TaskInfo{CreateTime: timestamppb.Now()}, fwdInfo, 0, nil)
	}

	s.Run("NotForwarded", func() {
		synctest.Test(s.T(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tm := s.newTestMatcher(ctx, testMatcherOpts{})
			tm.Start()
			defer tm.Stop()

			outcome, err := tm.Offer(ctx, newSyncTask(nil))
			require.NoError(t, err)
			require.Equal(t, syncMatchNoPoller, outcome)
		})
	})

	s.Run("ForwardedFromHistory", func() {
		synctest.Test(s.T(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tm := s.newTestMatcher(ctx, testMatcherOpts{})
			tm.Start()
			defer tm.Stop()

			outcome, err := tm.Offer(ctx, newSyncTask(childForwardInfo(enumsspb.TASK_SOURCE_HISTORY)))
			require.NoError(t, err)
			require.Equal(t, syncMatchNoPoller, outcome)
		})
	})

	s.Run("ForwardedFromBacklog", func() {
		synctest.Test(s.T(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tm := s.newTestMatcher(ctx, testMatcherOpts{})
			tm.Start()
			defer tm.Stop()

			task := newSyncTask(childForwardInfo(enumsspb.TASK_SOURCE_DB_BACKLOG))
			var offerDone atomic.Bool
			var outcome syncMatchOutcome
			var offerErr error
			go func() {
				outcome, offerErr = tm.Offer(ctx, task)
				offerDone.Store(true)
			}()
			time.Sleep(time.Minute) //nolint:forbidigo // virtual time
			synctest.Wait()
			require.False(t, offerDone.Load(), "offer of forwarded backlog task should wait for a poller")

			polled, err := tm.Poll(ctx, &pollMetadata{})
			require.NoError(t, err)
			require.Equal(t, task, polled)
			polled.finish(taskFinishResult{consumedToken: true})
			synctest.Wait()
			require.True(t, offerDone.Load())
			require.NoError(t, offerErr)
			require.Equal(t, syncMatchSuccess, outcome)
		})
	})

	s.Run("ForwardedFromBacklogWithLocalBacklog", func() {
		synctest.Test(s.T(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tm := s.newTestMatcher(ctx, testMatcherOpts{})
			tm.Start()
			defer tm.Stop()

			// The root validator will periodically take the task and send it back for
			// reprocessing, so put it back when that happens.
			var backlogTask *internalTask
			backlogTask = newTestBacklogTask(time.Hour, func(*internalTask, taskResponse) {
				backlogTask.resetMatcherState()
				_ = tm.AddTask(backlogTask)
			})
			_ = tm.AddTask(backlogTask)
			synctest.Wait()

			outcome, err := tm.Offer(ctx, newSyncTask(childForwardInfo(enumsspb.TASK_SOURCE_DB_BACKLOG)))
			require.NoError(t, err)
			require.Equal(t, syncMatchBacklogPresent, outcome)
		})
	})
}

// TestRootOfferQueryNoRecentPoller checks that a query on the root partition fails fast with
// errNoRecentPoller when no poller has been seen within QueryPollerUnavailableWindow, and
// otherwise waits for the full deadline.
func (s *PriMatcherSuite) TestRootOfferQueryNoRecentPoller() {
	const queryTimeout = 10 * time.Second
	cases := []struct {
		name          string
		pollerAge     time.Duration // 0 means no poller at all
		wantErr       error
		wantQueryTime time.Duration
	}{
		{"NoPollerAtAll", 0, errNoRecentPoller, queryTimeout - returnEmptyTaskTimeBudget},
		{"RecentPoller", time.Second, context.DeadlineExceeded, queryTimeout},
		{"OldPoller", time.Minute, errNoRecentPoller, queryTimeout - returnEmptyTaskTimeBudget},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			synctest.Test(s.T(), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				tm := s.newTestMatcher(ctx, testMatcherOpts{})
				tm.Start()
				defer tm.Stop()

				if tc.pollerAge > 0 {
					pollCtx, pollCancel := context.WithTimeout(ctx, time.Millisecond)
					_, err := tm.PollForQuery(pollCtx, &pollMetadata{})
					pollCancel()
					require.ErrorIs(t, err, errNoTasks)
					time.Sleep(tc.pollerAge) //nolint:forbidigo // virtual time
				}

				queryCtx, queryCancel := context.WithTimeout(ctx, queryTimeout)
				defer queryCancel()
				start := time.Now()
				_, err := tm.OfferQuery(queryCtx, newInternalQueryTask("query-id", &matchingservice.QueryWorkflowRequest{}))
				require.ErrorIs(t, err, tc.wantErr)
				require.Equal(t, tc.wantQueryTime, time.Since(start))
			})
		})
	}
}

// TestChildBacklogForwarding checks when a child partition forwards backlog tasks to the parent.
// It holds back an old (non-negligible) backlog while it has recent pollers of its own, so that
// all partitions work through their backlogs at a similar rate, but forwards once there haven't
// been pollers for MaxWaitForPollerBeforeFwd.
func (s *PriMatcherSuite) TestChildBacklogForwarding() {
	cases := []struct {
		name         string
		taskAge      time.Duration
		recentPoller bool
		wantDelay    bool // true means delay by MaxWaitForPollerBeforeFwd
	}{
		{"YoungBacklogRecentPoller", time.Second, true, false},
		{"OldBacklogNoPoller", time.Hour, false, false},
		{"OldBacklogRecentPoller", time.Hour, true, true},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			synctest.Test(s.T(), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				child := testMatcherTaskQueue.NormalPartition(1)
				client := matchingservicemock.NewMockMatchingServiceClient(s.controller)
				forwardedAt := make(chan time.Time, 1)
				client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, r *matchingservice.AddWorkflowTaskRequest, _ ...any) (*matchingservice.AddWorkflowTaskResponse, error) {
						require.Equal(t, enumsspb.TASK_SOURCE_DB_BACKLOG, r.GetForwardInfo().GetTaskSource())
						forwardedAt <- time.Now()
						return &matchingservice.AddWorkflowTaskResponse{}, nil
					})
				validator := NewMocktaskValidator(s.controller)
				validator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).Return(true)

				tm := s.newTestMatcher(ctx, testMatcherOpts{partition: child, client: client, validator: validator})
				tm.Start()
				defer tm.Stop()
				synctest.Wait()

				if tc.recentPoller {
					tm.data.lock.Lock()
					tm.data.lastPoller = time.Now()
					tm.data.lock.Unlock()
				}

				start := time.Now()
				completed := make(chan taskResponse, 1)
				_ = tm.AddTask(newTestBacklogTask(tc.taskAge, func(_ *internalTask, res taskResponse) {
					completed <- res
				}))

				var wantDelay time.Duration
				if tc.wantDelay {
					wantDelay = tm.config.MaxWaitForPollerBeforeFwd()
				}
				require.Equal(t, wantDelay, (<-forwardedAt).Sub(start))
				res := <-completed
				require.True(t, res.forwarded)
				require.NoError(t, res.forwardErr)
			})
		})
	}
}
