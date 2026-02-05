package matching

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PriMatcherSuite struct {
	suite.Suite
	logger log.Logger
}

func TestPriMatcherSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PriMatcherSuite))
}

func (s *PriMatcherSuite) SetupTest() {
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
}

// TestValidatorWorksOnRoot tests that the validator goroutine can pick up tasks
// on a root partition (where there is no forwarder). Before the fix, this would fail
// because the validator used isTaskForwarder=true which caused it to be skipped
// when allowForwarding=false (which is always the case on root).
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
		NormalPartition(0) // partition 0 is root

	// Track whether validator received a task
	var validatorReceivedTask atomic.Bool
	var validatorValidatedTask atomic.Bool

	// Create a mock validator that tracks calls
	mockValidator := &mockTaskValidator{
		maybeValidateFn: func(task *persistencespb.AllocatedTaskInfo, taskType enumspb.TaskQueueType) bool {
			validatorValidatedTask.Store(true)
			return true // task is valid
		},
	}

	rateLimitManager := newRateLimitManager(&mockUserDataManager{}, cfg, enumspb.TASK_QUEUE_TYPE_WORKFLOW)

	// Create a root matcher (no forwarder - fwdr=nil means root)
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
		func() {}, // markAlive
	)

	// Start the matcher (this starts validateTasksOnRoot goroutine)
	tm.Start()
	defer tm.Stop()

	// Add a backlog task to the matcher
	taskInfo := &persistencespb.AllocatedTaskInfo{
		TaskId: 1,
		Data: &persistencespb.TaskInfo{
			CreateTime: timestamppb.Now(),
		},
	}

	completionCalled := make(chan struct{})
	task := newInternalTaskFromBacklog(taskInfo, func(t *internalTask, res taskResponse) {
		validatorReceivedTask.Store(true)
		close(completionCalled)
	})
	task.source = enumsspb.TASK_SOURCE_DB_BACKLOG

	// Must call resetMatcherState before adding to matcher (this is normally done by task reader)
	task.resetMatcherState()

	// Add task to matcher
	tm.AddTask(task)

	// Wait for the validator to pick up and process the task
	// The validator should call maybeValidate and then finish the task with errReprocessTask
	select {
	case <-completionCalled:
		// Task was processed
	case <-time.After(2 * time.Second):
		s.Fail("Timeout waiting for validator to process task")
	}

	s.True(validatorReceivedTask.Load(), "Validator should have received the task")
	s.True(validatorValidatedTask.Load(), "Validator should have called maybeValidate")
}

// mockTaskValidator is a test implementation of taskValidator
type mockTaskValidator struct {
	maybeValidateFn func(task *persistencespb.AllocatedTaskInfo, taskType enumspb.TaskQueueType) bool
}

func (m *mockTaskValidator) maybeValidate(task *persistencespb.AllocatedTaskInfo, taskType enumspb.TaskQueueType) bool {
	if m.maybeValidateFn != nil {
		return m.maybeValidateFn(task, taskType)
	}
	return true
}
