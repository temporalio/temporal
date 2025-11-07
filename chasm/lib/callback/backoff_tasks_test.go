package callback_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// backoffTasksSuite extends callbackSuite to test BackoffTask execution.
// These tests mirror HSM's TestProcessBackoffTask pattern.
type backoffTasksSuite struct {
	callbackSuite
}

func TestBackoffTasksSuite(t *testing.T) {
	suite.Run(t, &backoffTasksSuite{})
}

func (s *backoffTasksSuite) SetupTest() {
	s.callbackSuite.SetupTest()
}

// ========================================
// BackoffTask Execution Tests
// ========================================

// TestBackoffTask_ReschedulesCallback tests that BackoffTask transitions callback back to SCHEDULED.
// Mirrors HSM test: TestProcessBackoffTask
func (s *backoffTasksSuite) TestBackoffTask_ReschedulesCallback() {
	// Create callback in BACKING_OFF state
	s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_BACKING_OFF)

	// Execute BackoffTask within a single transaction (like scheduler tests)
	ctx := s.newMutableContext()
	callbackField := s.workflow.Callbacks["cb-1"]
	cb, err := callbackField.Get(ctx)
	s.NoError(err)

	// Modify callback state
	cb.Attempt = 1
	cb.NextAttemptScheduleTime = timestamppb.New(s.timeSource.Now().Add(time.Minute))

	// Execute BackoffTask in the SAME transaction (don't close between state modification and execution)
	err = s.backoffExecutor.Execute(ctx, cb, chasm.TaskAttributes{}, &callbackspb.BackoffTask{
		Attempt: 1,
	})
	s.NoError(err)

	// Close transaction
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Verify callback transitioned to SCHEDULED
	s.Equal(callbackspb.CALLBACK_STATUS_SCHEDULED, cb.Status)
	s.Equal(int32(1), cb.Attempt) // Attempt stays same
	s.Nil(cb.NextAttemptScheduleTime)
}

// TestBackoffTask_GeneratesInvocationTask tests that BackoffTask generates a new InvocationTask.
func (s *backoffTasksSuite) TestBackoffTask_GeneratesInvocationTask() {
	// Create callback in BACKING_OFF state
	s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_BACKING_OFF)

	// Execute BackoffTask within a single transaction (like scheduler tests)
	ctx := s.newMutableContext()
	callbackField := s.workflow.Callbacks["cb-1"]
	cb, err := callbackField.Get(ctx)
	s.NoError(err)

	// Modify callback state
	cb.Attempt = 2
	cb.NextAttemptScheduleTime = timestamppb.New(s.timeSource.Now().Add(time.Minute))

	// Execute BackoffTask in the SAME transaction (don't close between state modification and execution)
	err = s.backoffExecutor.Execute(ctx, cb, chasm.TaskAttributes{}, &callbackspb.BackoffTask{
		Attempt: 2,
	})
	s.NoError(err)

	// Close transaction to commit tasks to node backend
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// TODO: Verify InvocationTask was generated with correct attempt. Similar to scheduler's
	// backfiller_tasks_test.go (see TODO at line 36-40), we cannot currently verify task
	// generation in CHASM integration tests. The issue is that when Get() retrieves a callback
	// from the tree that was created in a previous transaction, the callback instance returned
	// is not properly tracked by the current transaction's mutation system. When AddTask() is
	// called during the transition, the tasks are added to the node but CloseTransaction()
	// returns empty mutations and MockNodeBackend.AddTasks() is never called. This affects
	// both pure tasks (BackoffTask) and side-effect tasks (InvocationTask). Fix this when
	// CHASM offers unit testing hooks for task generation.
	// s.True(s.hasInvocationTask(2), "Expected InvocationTask with attempt=2 to be generated")
}

// ========================================
// BackoffTask Validation Tests
// ========================================

// TestBackoffTask_ValidateAttemptMismatch tests that BackoffTask validation rejects mismatched attempts.
func (s *backoffTasksSuite) TestBackoffTask_ValidateAttemptMismatch() {
	// Create callback in BACKING_OFF state with attempt=3
	cb := s.createCallback("cb-1", "req-1", "http://localhost", callbackspb.CALLBACK_STATUS_BACKING_OFF)
	cb.Attempt = 3

	ctx := s.newContext()

	// Try to validate BackoffTask with different attempt number
	valid, err := s.backoffExecutor.Validate(ctx, cb, chasm.TaskAttributes{}, &callbackspb.BackoffTask{
		Attempt: 2,
	})
	s.NoError(err)
	s.False(valid, "task should be rejected when attempt doesn't match")

	// Validate with correct attempt
	valid, err = s.backoffExecutor.Validate(ctx, cb, chasm.TaskAttributes{}, &callbackspb.BackoffTask{
		Attempt: 3,
	})
	s.NoError(err)
	s.True(valid, "task should be accepted when attempt matches and state is BACKING_OFF")
}
