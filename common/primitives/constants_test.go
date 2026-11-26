package primitives

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsInternalWorkflowID(t *testing.T) {
	require.True(t, IsInternalWorkflowID(ScheduleWorkflowIDPrefix+"my-schedule"))
	require.True(t, IsInternalWorkflowID("temporal-sys-worker-deployment:my-deployment"))
	require.False(t, IsInternalWorkflowID("my-workflow"))
	require.False(t, IsInternalWorkflowID("my-temporal-sys-workflow"))
	require.False(t, IsInternalWorkflowID(""))
}
