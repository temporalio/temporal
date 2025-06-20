package effect_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/effect"
)

func TestBufferApplyOrder(t *testing.T) {
	var buf effect.Buffer
	state := make([]int, 0, 3)

	buf.OnAfterCommit(func(context.Context) { state = append(state, 0) })
	buf.OnAfterCommit(func(context.Context) { state = append(state, 1) })
	buf.OnAfterCommit(func(context.Context) { state = append(state, 2) })

	buf.Apply(context.TODO())

	require.ElementsMatch(t, []int{0, 1, 2}, state)
}

func TestBufferRollbackOrder(t *testing.T) {
	var buf effect.Buffer
	state := make([]int, 0, 3)

	buf.OnAfterRollback(func(context.Context) { state = append(state, 0) })
	buf.OnAfterRollback(func(context.Context) { state = append(state, 1) })
	buf.OnAfterRollback(func(context.Context) { state = append(state, 2) })

	buf.Cancel(context.TODO())

	require.ElementsMatch(t, []int{2, 1, 0}, state)
}

func TestBufferCancelAfterApply(t *testing.T) {
	var buf effect.Buffer
	var commit, rollback int
	buf.OnAfterCommit(func(context.Context) { commit++ })
	buf.OnAfterRollback(func(context.Context) { rollback++ })

	buf.Apply(context.TODO())
	buf.Cancel(context.TODO())
	buf.Apply(context.TODO())
	buf.Cancel(context.TODO())
	buf.Apply(context.TODO())
	buf.Cancel(context.TODO())

	require.Equal(t, commit, 1)
	require.Equal(t, rollback, 0)
}

func TestBufferApplyAfterCancel(t *testing.T) {
	var buf effect.Buffer
	var commit, rollback int
	buf.OnAfterCommit(func(context.Context) { commit++ })
	buf.OnAfterRollback(func(context.Context) { rollback++ })

	buf.Cancel(context.TODO())
	buf.Apply(context.TODO())
	buf.Apply(context.TODO())
	buf.Cancel(context.TODO())
	buf.Apply(context.TODO())
	buf.Cancel(context.TODO())

	require.Equal(t, commit, 0)
	require.Equal(t, rollback, 1)
}
