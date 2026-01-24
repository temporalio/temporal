package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestTaskValidator(t *testing.T) {
	ctx := &MockMutableContext{}
	visibility := NewVisibility(ctx)
	task := persistencespb.ChasmVisibilityTaskData_builder{
		TransitionCount: 3,
	}.Build()

	visibility.Data.SetTransitionCount(1)
	valid, err := defaultVisibilityTaskHandler.Validate(ctx, visibility, TaskAttributes{}, task)
	require.NoError(t, err)
	require.False(t, valid)

	visibility.Data.SetTransitionCount(task.GetTransitionCount())
	valid, err = defaultVisibilityTaskHandler.Validate(ctx, visibility, TaskAttributes{}, task)
	require.NoError(t, err)
	require.True(t, valid)
}
