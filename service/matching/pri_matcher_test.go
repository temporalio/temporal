package matching

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
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
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/await"
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

func (s *PriMatcherSuite) newBacklogTask(id int64, done chan taskResponse) *internalTask {
	task := newInternalTaskFromBacklog(&persistencespb.AllocatedTaskInfo{
		TaskId: id,
		Data: &persistencespb.TaskInfo{
			CreateTime: timestamppb.Now(),
		},
	}, func(_ *internalTask, res taskResponse) {
		done <- res
	})
	task.resetMatcherState()
	return task
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

// TestEmitDispatchLatency_FairnessKeyTag verifies the matcher tags
// TaskDispatchLatencyPerTaskQueue with the task's fairness key, and that the
// breakdownByFairnessKey gate replaces it with "__omitted__" when disabled.
func (s *PriMatcherSuite) TestEmitDispatchLatency_FairnessKeyTag() {
	cases := []struct {
		name      string
		breakdown bool
		taskKey   string
		wantKey   string
	}{
		{"breakdown enabled tags the real key", true, "orders", "orders"},
		{"breakdown enabled maps an empty key to none", true, "", "__none__"},
		{"breakdown disabled omits a real key", false, "orders", "__omitted__"},
		{"breakdown disabled still marks an empty key as none", false, "", "__none__"},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cfgClient := dynamicconfig.NewMemoryClient()
			// Emit at the matcher (not at poll) so emitDispatchLatency actually records.
			cfgClient.OverrideValue(dynamicconfig.MatchingEmitTaskDispatchLatencyAtPoll.Key(), false)
			cfgClient.OverrideValue(dynamicconfig.MetricsBreakdownByFairnessKey.Key(), tc.breakdown)

			tq := tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
			cfg := newTaskQueueConfig(tq, NewConfig(dynamicconfig.NewCollection(cfgClient, s.logger)), "nsname")

			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)

			rateLimitManager := newRateLimitManager(&mockUserDataManager{}, cfg, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
			rateLimitManager.Start()
			tm := newPriTaskMatcher(
				ctx, cfg, tq.RootPartition(),
				nil, nil, // nil forwarder = root partition, no client
				NewMocktaskValidator(s.controller),
				s.logger, handler, rateLimitManager,
				func() {}, func() {},
			)

			task := newInternalTaskForSyncMatch(&persistencespb.TaskInfo{
				CreateTime: timestamppb.New(time.Now().Add(-time.Second)),
				Priority:   &commonpb.Priority{PriorityKey: 3, FairnessKey: tc.taskKey},
			}, nil, 0, nil)

			tm.emitDispatchLatency(task, false)

			recs := capture.Snapshot()[metrics.TaskDispatchLatencyPerTaskQueue.Name()]
			s.Require().Len(recs, 1)
			s.Equal(tc.wantKey, recs[0].Tags[metrics.FairnessKeyTagName])
		})
	}
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
				validator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).Return(true).AnyTimes()

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
				require.NoError(t, tm.AddTask(newTestBacklogTask(tc.taskAge, func(task *internalTask, res taskResponse) {
					if res.err() == errReprocessTask {
						task.resetMatcherState()
						if err := tm.AddTask(task); err != nil {
							completed <- taskResponse{startErr: err}
						}
						return
					}
					completed <- res
				})))

				var wantDelay time.Duration
				if tc.wantDelay {
					wantDelay = tm.config.MaxWaitForPollerBeforeFwd()
				}
				res := await.Rcv(t, completed)
				require.NoError(t, res.err())
				require.True(t, res.forwarded)
				require.Equal(t, wantDelay, await.Rcv(t, forwardedAt).Sub(start))
			})
		})
	}
}

func (s *PriMatcherSuite) TestValidatorBatch_AllInvalidDropsAll() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockValidator := NewMocktaskValidator(s.controller)
	mockValidator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).Return(false).Times(3)

	tm := s.newTestMatcher(ctx, testMatcherOpts{
		validator: mockValidator,
		config: func(cfg *taskQueueConfig) {
			cfg.ValidatorBatchSize = func() int { return 3 }
		},
	})
	defer tm.Stop()

	done := make(chan taskResponse, 3)
	for id := int64(1); id <= 3; id++ {
		s.Require().NoError(tm.AddTask(s.newBacklogTask(id, done)))
	}
	tm.Start()

	for range 3 {
		res := await.Rcv(s.T(), done)
		s.Require().NoError(res.err())
		s.Equal(dropReasonInvalid, res.dropReason)
	}
}

func (s *PriMatcherSuite) TestValidatorBatch_AllValidReprocessesAll() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockValidator := NewMocktaskValidator(s.controller)
	mockValidator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).Return(true).Times(3)

	tm := s.newTestMatcher(ctx, testMatcherOpts{
		validator: mockValidator,
		config: func(cfg *taskQueueConfig) {
			cfg.ValidatorBatchSize = func() int { return 3 }
		},
	})
	defer tm.Stop()

	done := make(chan taskResponse, 3)
	for id := int64(1); id <= 3; id++ {
		s.Require().NoError(tm.AddTask(s.newBacklogTask(id, done)))
	}
	tm.Start()

	for range 3 {
		s.Require().ErrorIs(await.Rcv(s.T(), done).err(), errReprocessTask)
	}
}

// TODO: Add tests for verifying the validator doesn't consume rate limit tokens.

func (s *PriMatcherSuite) TestValidatorBatch_MixedInvalidContinuesImmediately() {
	// synctest: after a mixed batch the validator must not sleep before the next match.
	synctest.Test(s.T(), func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockValidator := NewMocktaskValidator(s.controller)
		mockValidator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).DoAndReturn(
			func(task *persistencespb.AllocatedTaskInfo, _ enumspb.TaskQueueType) bool {
				return task.TaskId != 1 // task 1 invalid; others valid
			},
		).AnyTimes()

		tm := s.newTestMatcher(ctx, testMatcherOpts{
			validator: mockValidator,
			config: func(cfg *taskQueueConfig) {
				cfg.ValidatorBatchSize = func() int { return 2 }
			},
		})
		defer tm.Stop()

		done := make(chan taskResponse, 4)
		for id := int64(1); id <= 2; id++ {
			require.NoError(t, tm.AddTask(s.newBacklogTask(id, done)))
		}
		tm.Start()
		// Drain first batch.
		for range 2 {
			await.Rcv(t, done)
		}

		require.NoError(t, tm.AddTask(s.newBacklogTask(3, done)))
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("validator did not continue immediately after mixed batch")
		}
	})
}

func (s *PriMatcherSuite) TestValidatorBatch_ValidatesConcurrently() {
	for _, tc := range []struct {
		name      string
		batchSize int
		taskCount int
		wantBatch int
	}{
		{name: "within cache capacity", batchSize: 2, taskCount: 2, wantBatch: 2},
		{name: "above cache capacity", batchSize: taskValidatorCacheMaxSize + 1, taskCount: taskValidatorCacheMaxSize + 1, wantBatch: taskValidatorCacheMaxSize},
	} {
		s.Run(tc.name, func() {
			synctest.Test(s.T(), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				started := make(chan int64, tc.taskCount)
				release := make(chan struct{})
				mockValidator := NewMocktaskValidator(gomock.NewController(t))
				mockValidator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).DoAndReturn(
					func(task *persistencespb.AllocatedTaskInfo, _ enumspb.TaskQueueType) bool {
						started <- task.TaskId
						select {
						case <-release:
						case <-ctx.Done():
						}
						return true
					},
				).AnyTimes()

				tm := s.newTestMatcher(ctx, testMatcherOpts{
					validator: mockValidator,
					config: func(cfg *taskQueueConfig) {
						cfg.ValidatorBatchSize = func() int { return tc.batchSize }
					},
				})
				defer tm.Stop()
				done := make(chan taskResponse, tc.taskCount)
				for id := int64(1); id <= int64(tc.taskCount); id++ {
					require.NoError(t, tm.AddTask(s.newBacklogTask(id, done)))
				}
				tm.Start()

				synctest.Wait()
				require.Len(t, started, tc.wantBatch)
				var wantIDs, startedIDs []int64
				for id := int64(1); id <= int64(tc.wantBatch); id++ {
					wantIDs = append(wantIDs, id)
					startedIDs = append(startedIDs, await.Rcv(t, started))
				}
				require.ElementsMatch(t, wantIDs, startedIDs)
				close(release)
				for range tc.wantBatch {
					require.ErrorIs(t, await.Rcv(t, done).err(), errReprocessTask)
				}
			})
		})
	}
}

func (s *PriMatcherSuite) TestValidatorRunsOnChildBehindForwardedHead() {
	synctest.Test(s.T(), func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tq := tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		childPartition := tq.NormalPartition(1)
		cfg := newTaskQueueConfig(tq, NewConfig(dynamicconfig.NewNoopCollection()), "nsname")
		cfg.ValidatorBatchSize = func() int { return 2 }
		cfg.ForwarderMaxOutstandingTasks = func() int { return 1 }
		cfg.ForwarderMaxRatePerSecond = func() float64 { return 1000 }

		mockClient := matchingservicemock.NewMockMatchingServiceClient(s.controller)
		forwardStarted := make(chan struct{})
		forwardRelease := make(chan struct{})
		mockClient.EXPECT().
			AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, *matchingservice.AddWorkflowTaskRequest, ...any) (*matchingservice.AddWorkflowTaskResponse, error) {
				close(forwardStarted)
				<-forwardRelease
				return &matchingservice.AddWorkflowTaskResponse{}, nil
			}).AnyTimes()

		// Return true so the forwarder actually forwards the head (false would
		// drop it in forwardTask and never call AddWorkflowTask). The batch
		// validator then reprocesses the tasks behind the head.
		mockValidator := NewMocktaskValidator(s.controller)
		mockValidator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).Return(true).AnyTimes()

		queue := UnversionedQueueKey(childPartition)
		fwdr, err := newPriForwarder(&cfg.forwarderConfig, queue, mockClient, testhooks.TestHooks{})
		require.NoError(t, err)

		rateLimitManager := newRateLimitManager(&mockUserDataManager{}, cfg, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		rateLimitManager.Start()
		tm := newPriTaskMatcher(
			ctx, cfg, childPartition, fwdr, mockClient, mockValidator,
			s.logger, metrics.NoopMetricsHandler, rateLimitManager, func() {}, func() {},
		)
		tm.Start()
		defer tm.Stop()

		// Wait until both the parentTaskForwarder and validator pollers are queued
		// so poller-list order applies when the tasks are added.
		await.RequireTrue(t, func() bool {
			tm.data.lock.Lock()
			defer tm.data.lock.Unlock()
			return tm.data.pollers.Len() >= 2
		}, time.Second, time.Millisecond)

		done := make(chan taskResponse, 3)
		for id := int64(1); id <= 3; id++ {
			require.NoError(t, tm.AddTask(s.newBacklogTask(id, done)))
		}

		await.Rcv(t, forwardStarted)

		for range 2 {
			require.ErrorIs(t, await.Rcv(t, done).err(), errReprocessTask)
		}

		close(forwardRelease)
	})
}

func (s *PriMatcherSuite) TestForwardTasksIndependentBackoff() {
	synctest.Test(s.T(), func(t *testing.T) {
		const workers = 16
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tq := tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		child := tq.NormalPartition(1)
		cfg := newTaskQueueConfig(tq, NewConfig(dynamicconfig.NewNoopCollection()), "nsname")
		cfg.ForwarderMaxOutstandingTasks = func() int { return workers }
		cfg.ForwarderMaxOutstandingPolls = func() int { return 0 }
		cfg.ForwarderMaxRatePerSecond = func() float64 { return 1000 }

		started := make(chan struct{}, workers)
		releaseErrors := make(chan struct{})
		releaseSuccesses := make(chan struct{})
		var calls atomic.Int32
		rateLimitErr := serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, "rate limit exceeded")
		client := matchingservicemock.NewMockMatchingServiceClient(s.controller)
		client.EXPECT().AddWorkflowTask(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(rpcCtx context.Context, _ *matchingservice.AddWorkflowTaskRequest, _ ...any) (*matchingservice.AddWorkflowTaskResponse, error) {
				call := calls.Add(1)
				started <- struct{}{}
				release := releaseSuccesses
				var forwardErr error
				if call <= workers {
					release = releaseErrors
					forwardErr = rateLimitErr
				}
				select {
				case <-release:
					return &matchingservice.AddWorkflowTaskResponse{}, forwardErr
				case <-rpcCtx.Done():
					return nil, rpcCtx.Err()
				}
			},
		).Times(workers * 2)
		fwdr, err := newPriForwarder(&cfg.forwarderConfig, UnversionedQueueKey(child), client, testhooks.TestHooks{})
		require.NoError(t, err)
		manager := newRateLimitManager(&mockUserDataManager{}, cfg, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		manager.Start()
		tm := newPriTaskMatcher(ctx, cfg, child, fwdr, client, nil,
			s.logger, metrics.NoopMetricsHandler, manager, func() {}, func() {})
		defer tm.Stop()
		done := make(chan taskResponse, workers)
		addTasks := func(firstID int64) {
			for id := firstID; id < firstID+workers; id++ {
				task := s.newBacklogTask(id, done)
				task.forwardCtx = ctx
				require.NoError(t, tm.AddTask(task))
			}
		}
		addTasks(1)
		tm.Start()
		for range workers {
			await.Rcv(t, started)
		}
		close(releaseErrors)
		for range workers {
			require.ErrorIs(t, await.Rcv(t, done).forwardErr, rateLimitErr)
		}
		synctest.Wait()

		// Each worker's first retry is at most one second, including jitter.
		<-time.After(time.Second)
		synctest.Wait()
		tm.data.lock.Lock()
		waitingPollers := tm.data.pollers.Len()
		tm.data.lock.Unlock()
		require.Equal(t, workers+1, waitingPollers, "all forwarding workers and the validator should be waiting")

		addTasks(workers + 1)
		for range workers {
			await.Rcv(t, started)
		}
		close(releaseSuccesses)
		for range workers {
			require.NoError(t, await.Rcv(t, done).forwardErr)
		}
		synctest.Wait()
	})
}
