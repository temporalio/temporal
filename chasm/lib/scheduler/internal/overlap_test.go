package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestTracksCompletionResult(t *testing.T) {
	require.False(t, TracksCompletionResult(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))
	require.True(t, TracksCompletionResult(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP))
}
