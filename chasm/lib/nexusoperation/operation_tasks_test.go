package nexusoperation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

func TestBackoffTaskHandler_Validate(t *testing.T) {
	testCases := []struct {
		name     string
		status   nexusoperationpb.OperationStatus
		attempt  int32
		task     *nexusoperationpb.InvocationBackoffTask
		expected bool
	}{
		{
			name:     "valid when backing off and attempt matches",
			status:   nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			attempt:  2,
			task:     &nexusoperationpb.InvocationBackoffTask{Attempt: 2},
			expected: true,
		},
		{
			name:     "invalid when backing off but attempt mismatches",
			status:   nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			attempt:  2,
			task:     &nexusoperationpb.InvocationBackoffTask{Attempt: 1},
			expected: false,
		},
		{
			name:     "invalid when scheduled",
			status:   nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			attempt:  1,
			task:     &nexusoperationpb.InvocationBackoffTask{Attempt: 1},
			expected: false,
		},
		{
			name:     "invalid when started",
			status:   nexusoperationpb.OPERATION_STATUS_STARTED,
			attempt:  1,
			task:     &nexusoperationpb.InvocationBackoffTask{Attempt: 1},
			expected: false,
		},
		{
			name:     "invalid when timed out",
			status:   nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			attempt:  1,
			task:     &nexusoperationpb.InvocationBackoffTask{Attempt: 1},
			expected: false,
		},
	}

	handler := &OperationBackoffTaskHandler{}
	ctx := &chasm.MockContext{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op := newTestOperation()
			op.Status = tc.status
			op.Attempt = tc.attempt

			valid, err := handler.Validate(ctx, op, chasm.TaskAttributes{}, tc.task)
			require.NoError(t, err)
			require.Equal(t, tc.expected, valid)
		})
	}
}

func TestBackoffTaskHandler_Execute(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	op := newTestOperation()
	op.Status = nexusoperationpb.OPERATION_STATUS_BACKING_OFF
	op.Attempt = 2

	handler := &OperationBackoffTaskHandler{}
	err := handler.Execute(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.InvocationBackoffTask{Attempt: 2})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_SCHEDULED, op.Status)
	// Verify invocation task was emitted
	require.Len(t, ctx.Tasks, 1)
	_, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.InvocationTask)
	require.True(t, ok, "expected InvocationTask")
}

func TestScheduleToStartTimeoutTaskHandler_Validate(t *testing.T) {
	testCases := []struct {
		name     string
		status   nexusoperationpb.OperationStatus
		expected bool
	}{
		{
			name:     "valid when scheduled",
			status:   nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			expected: true,
		},
		{
			name:     "valid when backing off",
			status:   nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			expected: true,
		},
		{
			name:     "invalid when started",
			status:   nexusoperationpb.OPERATION_STATUS_STARTED,
			expected: false,
		},
		{
			name:     "invalid when succeeded",
			status:   nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
			expected: false,
		},
		{
			name:     "invalid when timed out",
			status:   nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			expected: false,
		},
	}

	handler := &OperationScheduleToStartTimeoutTaskHandler{}
	ctx := &chasm.MockContext{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op := newTestOperation()
			op.Status = tc.status

			valid, err := handler.Validate(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.ScheduleToStartTimeoutTask{})
			require.NoError(t, err)
			require.Equal(t, tc.expected, valid)
		})
	}
}

func TestScheduleToStartTimeoutTaskHandler_Execute(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	op := newTestOperation()
	op.Status = nexusoperationpb.OPERATION_STATUS_SCHEDULED

	handler := &OperationScheduleToStartTimeoutTaskHandler{}
	err := handler.Execute(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.ScheduleToStartTimeoutTask{})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_TIMED_OUT, op.Status)
	require.Empty(t, ctx.Tasks)
}

func TestStartToCloseTimeoutTaskHandler_Validate(t *testing.T) {
	testCases := []struct {
		name     string
		status   nexusoperationpb.OperationStatus
		expected bool
	}{
		{
			name:     "valid when started",
			status:   nexusoperationpb.OPERATION_STATUS_STARTED,
			expected: true,
		},
		{
			name:     "invalid when scheduled",
			status:   nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			expected: false,
		},
		{
			name:     "invalid when backing off",
			status:   nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			expected: false,
		},
		{
			name:     "invalid when succeeded",
			status:   nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
			expected: false,
		},
		{
			name:     "invalid when timed out",
			status:   nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			expected: false,
		},
	}

	handler := &OperationStartToCloseTimeoutTaskHandler{}
	ctx := &chasm.MockContext{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op := newTestOperation()
			op.Status = tc.status

			valid, err := handler.Validate(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.StartToCloseTimeoutTask{})
			require.NoError(t, err)
			require.Equal(t, tc.expected, valid)
		})
	}
}

func TestStartToCloseTimeoutTaskHandler_Execute(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	op := newTestOperation()
	op.Status = nexusoperationpb.OPERATION_STATUS_STARTED

	handler := &OperationStartToCloseTimeoutTaskHandler{}
	err := handler.Execute(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.StartToCloseTimeoutTask{})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_TIMED_OUT, op.Status)
	require.Empty(t, ctx.Tasks)
}

func TestScheduleToCloseTimeoutTaskHandler_Validate(t *testing.T) {
	testCases := []struct {
		name     string
		status   nexusoperationpb.OperationStatus
		expected bool
	}{
		{
			name:     "valid when scheduled",
			status:   nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			expected: true,
		},
		{
			name:     "valid when started",
			status:   nexusoperationpb.OPERATION_STATUS_STARTED,
			expected: true,
		},
		{
			name:     "valid when backing off",
			status:   nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			expected: true,
		},
		{
			name:     "invalid when succeeded",
			status:   nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
			expected: false,
		},
		{
			name:     "invalid when failed",
			status:   nexusoperationpb.OPERATION_STATUS_FAILED,
			expected: false,
		},
		{
			name:     "invalid when canceled",
			status:   nexusoperationpb.OPERATION_STATUS_CANCELED,
			expected: false,
		},
		{
			name:     "invalid when timed out",
			status:   nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			expected: false,
		},
	}

	handler := &OperationScheduleToCloseTimeoutTaskHandler{}
	ctx := &chasm.MockContext{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			op := newTestOperation()
			op.Status = tc.status

			valid, err := handler.Validate(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.ScheduleToCloseTimeoutTask{})
			require.NoError(t, err)
			require.Equal(t, tc.expected, valid)
		})
	}
}

func TestScheduleToCloseTimeoutTaskHandler_Execute(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	op := newTestOperation()
	op.Status = nexusoperationpb.OPERATION_STATUS_SCHEDULED

	handler := &OperationScheduleToCloseTimeoutTaskHandler{}
	err := handler.Execute(ctx, op, chasm.TaskAttributes{}, &nexusoperationpb.ScheduleToCloseTimeoutTask{})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_TIMED_OUT, op.Status)
	require.Empty(t, ctx.Tasks)
}
