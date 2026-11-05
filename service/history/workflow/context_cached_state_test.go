package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/locks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
)

func TestHasCachedState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		context func(*testing.T) *ContextImpl
		want    bool
	}{
		{"empty", func(*testing.T) *ContextImpl { return &ContextImpl{} }, false},
		{"mutable_state", func(t *testing.T) *ContextImpl {
			return &ContextImpl{MutableState: historyi.NewMockMutableState(gomock.NewController(t))}
		}, true},
		{"update_registry", func(t *testing.T) *ContextImpl {
			state := historyi.NewMockMutableState(gomock.NewController(t))
			state.EXPECT().GetCurrentVersion().Return(int64(1))
			state.EXPECT().VisitUpdates(gomock.Any())
			return &ContextImpl{updateRegistry: update.NewRegistry(state)}
		}, true},
		{"completion_buffer", func(*testing.T) *ContextImpl {
			return &ContextImpl{taskCompletionBuffer: &TaskCompletionBuffer{}}
		}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := tc.context(t)
			ctx.lock = locks.NewPrioritySemaphore(1)
			require.True(t, ctx.TryLock(locks.PriorityLow))
			defer ctx.Unlock()
			require.Equal(t, tc.want, ctx.HasCachedState())
		})
	}
}
