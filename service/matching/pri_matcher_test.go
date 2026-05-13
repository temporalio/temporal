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
		func() {},
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
