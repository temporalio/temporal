package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestTaskValidator(t *testing.T) {
	ctx := &MockMutableContext{}
	visibility := NewVisibility(ctx)
	task := &persistencespb.ChasmVisibilityTaskData{
		TransitionCount: 3,
	}

	visibility.Data.TransitionCount = 1
	valid, err := defaultVisibilityTaskHandler.Validate(ctx, visibility, TaskAttributes{}, task)
	require.NoError(t, err)
	require.False(t, valid)

	visibility.Data.TransitionCount = task.TransitionCount
	valid, err = defaultVisibilityTaskHandler.Validate(ctx, visibility, TaskAttributes{}, task)
	require.NoError(t, err)
	require.True(t, valid)
}
