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
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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

func (s *PriMatcherSuite) newRootMatcher(
	ctx context.Context,
	validator taskValidator,
	batchSize int,
) *priTaskMatcher {
	cfg := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		NewConfig(dynamicconfig.NewNoopCollection()),
		"nsname",
	)
	if batchSize > 0 {
		cfg.ValidatorBatchSize = func() int { return batchSize }
	}
	partition := tqid.UnsafeTaskQueueFamily("nsid", "tq").
		TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).
		RootPartition()
	rateLimitManager := newRateLimitManager(&mockUserDataManager{}, cfg, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	rateLimitManager.Start()
	return newPriTaskMatcher(
		ctx,
		cfg,
		partition,
		nil,
		nil,
		validator,
		s.logger,
		metrics.NoopMetricsHandler,
		rateLimitManager,
		func() {},
		func() {},
	)
}

func newBacklogTask(id int64, done chan taskResponse) *internalTask {
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

	cfg := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		NewConfig(dynamicconfig.NewNoopCollection()),
		"nsname",
	)

	partition := tqid.UnsafeTaskQueueFamily("nsid", "tq").
		TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).
		RootPartition()

	var validatorValidatedTask atomic.Bool

	// record validator calls
	mockValidator := NewMocktaskValidator(s.controller)
	mockValidator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).DoAndReturn(func(task *persistencespb.AllocatedTaskInfo, taskType enumspb.TaskQueueType) bool {
		validatorValidatedTask.Store(true)
		return true // task is valid
	})

	rateLimitManager := newRateLimitManager(&mockUserDataManager{}, cfg, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	rateLimitManager.Start()

	tm := newPriTaskMatcher(
		ctx,
		cfg,
		partition,
		nil, // nil forwarder = root partition
		nil, // no client needed for this test
		mockValidator,
		s.logger,
		metrics.NoopMetricsHandler,
		rateLimitManager,
		func() {}, // onRateLimited
		func() {}, // markAlive
	)

	// start the matcher
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

		tq := tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		childPartition := tq.NormalPartition(1) // child partition /1

		cfg := newTaskQueueConfig(tq, NewConfig(dynamicconfig.NewNoopCollection()), "nsname")
		// Use a generous poll timeout so we can distinguish retry success from timeout.
		cfg.LongPollExpirationInterval = func() time.Duration { return 10 * time.Second }

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

		// Create a priForwarder for the child partition (non-nil fwdr triggers child behavior).
		queue := UnversionedQueueKey(childPartition)
		fwdr, err := newPriForwarder(
			&cfg.forwarderConfig,
			queue,
			mockClient,
			testhooks.TestHooks{},
		)
		require.NoError(t, err)

		rateLimitManager := newRateLimitManager(&mockUserDataManager{}, cfg, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		rateLimitManager.Start()

		tm := newPriTaskMatcher(
			ctx,
			cfg,
			childPartition,
			fwdr,
			mockClient,
			nil, // no validator needed on child
			s.logger,
			metrics.NoopMetricsHandler,
			rateLimitManager,
			func() {},
			func() {},
		)

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

			cfg := newTaskQueueConfig(
				tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
				NewConfig(dynamicconfig.NewNoopCollection()),
				"nsname",
			)
			partition := tqid.UnsafeTaskQueueFamily("nsid", "tq").
				TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).
				RootPartition()

			// Validator rejects every task it sees, forcing the drop path.
			mockValidator := NewMocktaskValidator(s.controller)
			mockValidator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).Return(false).AnyTimes()

			rateLimitManager := newRateLimitManager(&mockUserDataManager{}, cfg, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
			rateLimitManager.Start()
			tm := newPriTaskMatcher(
				ctx,
				cfg,
				partition,
				nil, // nil forwarder = root partition -> validateTasks path
				nil,
				mockValidator,
				s.logger,
				metrics.NoopMetricsHandler,
				rateLimitManager,
				func() {},
				func() {},
			)
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

func (s *PriMatcherSuite) TestValidatorBatchSizeDefault() {
	cfg := newTaskQueueConfig(
		tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		NewConfig(dynamicconfig.NewNoopCollection()),
		"nsname",
	)
	s.Equal(10, cfg.ValidatorBatchSize())
}

func (s *PriMatcherSuite) TestValidatorBatch_AllInvalidDropsAll() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockValidator := NewMocktaskValidator(s.controller)
	mockValidator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).Return(false).Times(3)

	tm := s.newRootMatcher(ctx, mockValidator, 3)
	defer tm.Stop()

	done := make(chan taskResponse, 3)
	for id := int64(1); id <= 3; id++ {
		s.Require().NoError(tm.AddTask(newBacklogTask(id, done)))
	}
	tm.Start()

	for range 3 {
		select {
		case res := <-done:
			s.Require().NoError(res.err())
			s.Equal(dropReasonInvalid, res.dropReason)
		case <-time.After(2 * time.Second):
			s.Fail("timed out waiting for validator to drop batch")
		}
	}
}

func (s *PriMatcherSuite) TestValidatorBatch_AllValidReprocessesAll() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockValidator := NewMocktaskValidator(s.controller)
	mockValidator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).Return(true).Times(3)

	tm := s.newRootMatcher(ctx, mockValidator, 3)
	defer tm.Stop()

	done := make(chan taskResponse, 3)
	for id := int64(1); id <= 3; id++ {
		s.Require().NoError(tm.AddTask(newBacklogTask(id, done)))
	}
	tm.Start()

	for range 3 {
		select {
		case res := <-done:
			s.Require().ErrorIs(res.err(), errReprocessTask)
		case <-time.After(2 * time.Second):
			s.Fail("timed out waiting for validator to reprocess batch")
		}
	}
}

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

		tm := s.newRootMatcher(ctx, mockValidator, 2)
		defer tm.Stop()

		done := make(chan taskResponse, 4)
		for id := int64(1); id <= 2; id++ {
			require.NoError(t, tm.AddTask(newBacklogTask(id, done)))
		}
		tm.Start()
		// Drain first batch.
		for range 2 {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for first batch")
			}
		}

		require.NoError(t, tm.AddTask(newBacklogTask(3, done)))
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("validator did not continue immediately after mixed batch")
		}
	})
}

func (s *PriMatcherSuite) TestValidatorBatch_ValidatesConcurrently() {
	synctest.Test(s.T(), func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		started := make(chan int64, 2)
		release := make(chan struct{})
		mockValidator := NewMocktaskValidator(s.controller)
		mockValidator.EXPECT().maybeValidate(gomock.Any(), gomock.Any()).DoAndReturn(
			func(task *persistencespb.AllocatedTaskInfo, _ enumspb.TaskQueueType) bool {
				started <- task.TaskId
				select {
				case <-release:
				case <-ctx.Done():
				}
				return true
			},
		).Times(2)

		tm := s.newRootMatcher(ctx, mockValidator, 2)
		defer tm.Stop()
		done := make(chan taskResponse, 2)
		for id := int64(1); id <= 2; id++ {
			require.NoError(t, tm.AddTask(newBacklogTask(id, done)))
		}
		tm.Start()

		require.ElementsMatch(t, []int64{1, 2}, []int64{await.Rcv(t, started), await.Rcv(t, started)})
		close(release)
		for range 2 {
			require.ErrorIs(t, await.Rcv(t, done).err(), errReprocessTask)
		}
	})
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
			require.NoError(t, tm.AddTask(newBacklogTask(id, done)))
		}

		select {
		case <-forwardStarted:
		case <-time.After(time.Second):
			t.Fatal("forwarder never took the head")
		}

		reprocessed := 0
		deadline := time.After(time.Second)
		for reprocessed < 2 {
			select {
			case res := <-done:
				require.ErrorIs(t, res.err(), errReprocessTask)
				reprocessed++
			case <-deadline:
				t.Fatalf("validator did not reprocess tasks behind the head, reprocessed=%d", reprocessed)
			}
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
				task := newBacklogTask(id, done)
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
