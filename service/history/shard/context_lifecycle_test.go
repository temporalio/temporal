package shard

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

func TestGetEngineWhenShardStops(t *testing.T) {
	t.Parallel()

	for _, state := range []struct {
		name      string
		state     contextState
		hasEngine bool
	}{
		{name: "initialized", state: contextStateInitialized},
		{name: "initial acquisition", state: contextStateAcquiring},
		{name: "acquired", state: contextStateAcquired, hasEngine: true},
		{name: "reacquiring", state: contextStateAcquiring, hasEngine: true},
	} {
		for _, finishStop := range []bool{false, true} {
			name := state.name + "/unload"
			if finishStop {
				name = state.name + "/finish stop"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
					defer lifecycleCancel()
					shard := &ContextImpl{
						shardID:             1,
						state:               state.state,
						engineFuture:        future.NewFuture[historyi.Engine](),
						lifecycleCtx:        lifecycleCtx,
						lifecycleCancel:     lifecycleCancel,
						contextTaggedLogger: log.NewNoopLogger(),
					}
					var expectedEngine historyi.Engine
					var expectedErr error = &persistence.ShardOwnershipLostError{ShardID: 1, Msg: "shard closed"}
					if state.hasEngine {
						engine := historyi.NewMockEngine(gomock.NewController(t))
						engine.EXPECT().Stop()
						expectedEngine, expectedErr = engine, nil
						shard.engineFuture.Set(engine, nil)
					}

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					type result struct {
						engine historyi.Engine
						err    error
					}
					results := make(chan result, 1)
					go func() {
						engine, err := shard.GetEngine(ctx)
						results <- result{engine, err}
					}()
					synctest.Wait()
					if !state.hasEngine {
						select {
						case <-results:
							t.Fatal("GetEngine returned before acquisition or stop")
						default:
						}
					}

					if finishStop {
						shard.FinishStop()
					} else {
						shard.UnloadForOwnershipLost()
					}
					synctest.Wait()
					select {
					case got := <-results:
						require.Equal(t, result{expectedEngine, expectedErr}, got)
					default:
						t.Fatal("GetEngine remained blocked after the shard stopped")
					}
					require.NoError(t, ctx.Err())

					// An acquisition finishing after stop cannot replace the result.
					require.ErrorIs(t, shard.transition(contextRequestAcquired{
						engine: historyi.NewMockEngine(gomock.NewController(t)),
					}), errInvalidTransition)
					if !finishStop {
						shard.FinishStop()
					}
					engine, err := shard.GetEngine(ctx)
					require.Equal(t, result{expectedEngine, expectedErr}, result{engine, err})
				})
			})
		}
	}
}
