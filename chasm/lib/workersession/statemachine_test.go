package workersession

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	workersessionpb "go.temporal.io/server/chasm/lib/workersession/gen/workersessionpb/v1"
)

func TestRecordHeartbeat(t *testing.T) {
	session := NewWorkerSession()
	ctx := &chasm.MockMutableContext{}
	leaseDeadline := time.Now().Add(30 * time.Second)

	// Test successful heartbeat recording
	err := RecordHeartbeat(ctx, session, leaseDeadline)
	require.NoError(t, err)

	// Verify lease deadline was set
	require.NotNil(t, session.LeaseExpirationTime)
	require.Equal(t, leaseDeadline.Unix(), session.LeaseExpirationTime.AsTime().Unix())

	// Verify session is still active
	require.Equal(t, workersessionpb.WORKER_SESSION_STATUS_ACTIVE, session.Status)

	// Verify a task was scheduled
	require.Len(t, ctx.Tasks, 1)

	// Verify the task is a LeaseExpiryTask
	task, ok := ctx.Tasks[0].Payload.(*workersessionpb.LeaseExpiryTask)
	require.True(t, ok)
	require.Equal(t, leaseDeadline.Unix(), task.LeaseExpirationTime.AsTime().Unix())
}

func TestTransitionHeartbeatReceived(t *testing.T) {
	session := NewWorkerSession()
	ctx := &chasm.MockMutableContext{}

	heartbeatTime := time.Now()
	leaseDeadline := heartbeatTime.Add(30 * time.Second)

	event := EventHeartbeatReceived{
		Time:          heartbeatTime,
		LeaseDeadline: leaseDeadline,
	}

	// Apply the transition
	err := TransitionHeartbeatReceived.Apply(ctx, session, event)
	require.NoError(t, err)

	// Verify state was updated
	require.NotNil(t, session.LeaseExpirationTime)
	require.Equal(t, leaseDeadline.Unix(), session.LeaseExpirationTime.AsTime().Unix())
	require.Equal(t, workersessionpb.WORKER_SESSION_STATUS_ACTIVE, session.Status)

	// Verify task was scheduled
	require.Len(t, ctx.Tasks, 1)
	require.Equal(t, leaseDeadline, ctx.Tasks[0].Attributes.ScheduledTime)
}

func TestTransitionLeaseExpired(t *testing.T) {
	session := NewWorkerSession()
	session.Status = workersessionpb.WORKER_SESSION_STATUS_ACTIVE
	ctx := &chasm.MockMutableContext{}

	expiryTime := time.Now()
	event := EventLeaseExpired{
		Time: expiryTime,
	}

	// Apply the transition
	err := TransitionLeaseExpired.Apply(ctx, session, event)
	require.NoError(t, err)

	// Verify status changed to expired
	require.Equal(t, workersessionpb.WORKER_SESSION_STATUS_EXPIRED, session.Status)

	// Verify cleanup task was scheduled
	require.Len(t, ctx.Tasks, 1)

	// Verify cleanup task is scheduled for the right time
	expectedCleanupTime := expiryTime.Add(expiredSessionCleanupDelay)
	require.Equal(t, expectedCleanupTime, ctx.Tasks[0].Attributes.ScheduledTime)

	// Verify it's a SessionCleanupTask
	_, ok := ctx.Tasks[0].Payload.(*workersessionpb.SessionCleanupTask)
	require.True(t, ok)
}

func TestTransitionCleanupCompleted(t *testing.T) {
	session := NewWorkerSession()
	session.Status = workersessionpb.WORKER_SESSION_STATUS_EXPIRED
	ctx := &chasm.MockMutableContext{}

	cleanupTime := time.Now()
	event := EventCleanupCompleted{
		Time: cleanupTime,
	}

	// Apply the transition
	err := TransitionCleanupCompleted.Apply(ctx, session, event)
	require.NoError(t, err)

	// Verify status changed to terminated
	require.Equal(t, workersessionpb.WORKER_SESSION_STATUS_TERMINATED, session.Status)

	// Verify no additional tasks were scheduled
	require.Empty(t, ctx.Tasks)
}

func TestScheduleLeaseExpiry(t *testing.T) {
	session := NewWorkerSession()
	ctx := &chasm.MockMutableContext{}
	leaseDeadline := time.Now().Add(1 * time.Minute)

	// Schedule lease expiry
	scheduleLeaseExpiry(ctx, session, leaseDeadline)

	// Verify task was scheduled
	require.Len(t, ctx.Tasks, 1)

	// Verify task details
	task, ok := ctx.Tasks[0].Payload.(*workersessionpb.LeaseExpiryTask)
	require.True(t, ok)
	require.Equal(t, leaseDeadline.Unix(), task.LeaseExpirationTime.AsTime().Unix())
	require.Equal(t, leaseDeadline, ctx.Tasks[0].Attributes.ScheduledTime)
	require.Empty(t, ctx.Tasks[0].Attributes.Destination) // Local execution
}

func TestMultipleHeartbeats(t *testing.T) {
	session := NewWorkerSession()
	ctx := &chasm.MockMutableContext{}

	// First heartbeat
	firstDeadline := time.Now().Add(30 * time.Second)
	err := RecordHeartbeat(ctx, session, firstDeadline)
	require.NoError(t, err)
	require.Equal(t, firstDeadline.Unix(), session.LeaseExpirationTime.AsTime().Unix())

	// Second heartbeat extends the lease
	secondDeadline := time.Now().Add(60 * time.Second)
	err = RecordHeartbeat(ctx, session, secondDeadline)
	require.NoError(t, err)
	require.Equal(t, secondDeadline.Unix(), session.LeaseExpirationTime.AsTime().Unix())

	// Verify two tasks were scheduled (one for each heartbeat)
	require.Len(t, ctx.Tasks, 2)
}

func TestInvalidTransitions(t *testing.T) {
	ctx := &chasm.MockMutableContext{}

	t.Run("HeartbeatOnExpiredSession", func(t *testing.T) {
		session := NewWorkerSession()
		session.Status = workersessionpb.WORKER_SESSION_STATUS_EXPIRED

		leaseDeadline := time.Now().Add(30 * time.Second)
		err := RecordHeartbeat(ctx, session, leaseDeadline)

		// Should fail because session is not active
		require.Error(t, err)
	})

	t.Run("LeaseExpiryOnTerminatedSession", func(t *testing.T) {
		session := NewWorkerSession()
		session.Status = workersessionpb.WORKER_SESSION_STATUS_TERMINATED

		event := EventLeaseExpired{Time: time.Now()}
		err := TransitionLeaseExpired.Apply(ctx, session, event)

		// Should fail because session is not active
		require.Error(t, err)
	})

	t.Run("CleanupOnActiveSession", func(t *testing.T) {
		session := NewWorkerSession()
		session.Status = workersessionpb.WORKER_SESSION_STATUS_ACTIVE

		event := EventCleanupCompleted{Time: time.Now()}
		err := TransitionCleanupCompleted.Apply(ctx, session, event)

		// Should fail because session is not expired
		require.Error(t, err)
	})
}
