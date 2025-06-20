package effect_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/effect"
)

func TestImmediate(t *testing.T) {
	var i int
	immediate := effect.Immediate(context.TODO())
	immediate.OnAfterCommit(func(context.Context) { i = 1 })
	require.Equal(t, i, 1, "commit func should have run")

	immediate.OnAfterRollback(func(context.Context) { i = 2 })
	require.Equal(t, i, 1, "rollback func should not run")
}
