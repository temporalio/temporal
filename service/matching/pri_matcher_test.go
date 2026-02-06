package matching

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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
	tm.AddTask(task)

	// validator should pick up and check task
	select {
	case res := <-completionCalled:
		// error should be errReprocessTask
		s.ErrorIs(res.err(), errReprocessTask)
	case <-time.After(2 * time.Second):
		s.Fail("Timeout waiting for validator to process task")
	}

	s.True(validatorValidatedTask.Load(), "Validator should have called maybeValidate")
}
