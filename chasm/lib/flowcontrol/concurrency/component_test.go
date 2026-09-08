package concurrency

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTestComponent(limit int32) *Component {
	return &Component{
		ConcurrencyState: &fcpb.ConcurrencyState{
			Config: &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: limit},
		},
	}
}

func TestSlotLifecycle(t *testing.T) {
	now := time.Now().UTC()
	limiter := newTestComponent(1)

	require.True(t, limiter.reserve("slot-1", now))
	require.True(t, limiter.reserve("slot-1", now))
	require.Len(t, limiter.Slots, 1)
	require.False(t, limiter.reserve("slot-2", now))
	require.False(t, limiter.commit("slot-2"))

	require.True(t, limiter.commit("slot-1"))
	limiter.cancelReservation("slot-1")
	require.Len(t, limiter.Slots, 1)

	limiter.release("slot-2")
	require.Len(t, limiter.Slots, 1)
	limiter.release("slot-1")
	require.Empty(t, limiter.Slots)
	limiter.release("slot-1")
	require.Empty(t, limiter.Slots)
}

func TestExpiredSlotIDCanBeReplaced(t *testing.T) {
	now := time.Now().UTC()
	limiter := newTestComponent(1)

	require.True(t, limiter.reserve("expired-slot", now))
	limiter.expire(now.Add(reserveTimeout + time.Second))
	require.True(t, limiter.reserve("new-slot", now.Add(reserveTimeout+time.Second)))
	require.Len(t, limiter.Slots, 1)
	require.Equal(t, "new-slot", limiter.Slots[0].GetSlotId())
}

func TestPoll(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name              string
		component         *Component
		requestGeneration int64
		requestStartTime  int64
		requestTokens     int32
		wantGeneration    int64
		wantTokens        int32
		wantReady         bool
	}{
		{
			name: "old generation retries with current generation",
			component: &Component{ConcurrencyState: &fcpb.ConcurrencyState{
				Config:     &taskqueuepb.ConcurrencyLimit{},
				Generation: 2,
			}},
			requestGeneration: 1,
			requestTokens:     1,
			wantGeneration:    2,
			wantReady:         true,
		},
		{
			name: "future generation waits",
			component: &Component{ConcurrencyState: &fcpb.ConcurrencyState{
				Config:     &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 1},
				Generation: 2,
			}},
			requestGeneration: 3,
			requestTokens:     1,
		},
		{
			name: "selected waiter receives available tokens",
			component: &Component{ConcurrencyState: &fcpb.ConcurrencyState{
				Config:     &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 3},
				Generation: 2,
				WakeUpTo:   100,
				Slots: []*fcpb.ConcurrencyState_Slot{
					{SlotId: "committed", Committed: true},
				},
			}},
			requestGeneration: 2,
			requestStartTime:  50,
			requestTokens:     3,
			wantGeneration:    2,
			wantTokens:        2,
			wantReady:         true,
		},
		{
			name: "wake all selects later waiter",
			component: &Component{ConcurrencyState: &fcpb.ConcurrencyState{
				Config:     &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 2},
				Generation: 2,
				WakeAll:    true,
			}},
			requestGeneration: 2,
			requestStartTime:  200,
			requestTokens:     1,
			wantGeneration:    2,
			wantTokens:        1,
			wantReady:         true,
		},
		{
			name: "unselected waiter stays blocked with stored capacity",
			component: &Component{ConcurrencyState: &fcpb.ConcurrencyState{
				Config:     &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 1},
				Generation: 2,
				WakeUpTo:   100,
			}},
			requestGeneration: 2,
			requestStartTime:  200,
			requestTokens:     1,
		},
		{
			name: "expired reservation wakes waiter without transition",
			component: &Component{ConcurrencyState: &fcpb.ConcurrencyState{
				Config:     &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 1},
				Generation: 2,
				Slots: []*fcpb.ConcurrencyState_Slot{
					{SlotId: "expired", Expires: timestamppb.New(now.Add(-time.Second))},
				},
			}},
			requestGeneration: 2,
			requestStartTime:  200,
			requestTokens:     1,
			wantGeneration:    2,
			wantTokens:        1,
			wantReady:         true,
		},
		{
			name: "unexpired reservation keeps waiter blocked",
			component: &Component{ConcurrencyState: &fcpb.ConcurrencyState{
				Config:     &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 1},
				Generation: 2,
				Slots: []*fcpb.ConcurrencyState_Slot{
					{SlotId: "active", Expires: timestamppb.New(now.Add(time.Minute))},
				},
			}},
			requestGeneration: 2,
			requestStartTime:  200,
			requestTokens:     1,
		},
		{
			name: "expiration does not bypass staging when stored capacity exists",
			component: &Component{ConcurrencyState: &fcpb.ConcurrencyState{
				Config:     &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 2},
				Generation: 2,
				Slots: []*fcpb.ConcurrencyState_Slot{
					{SlotId: "expired", Expires: timestamppb.New(now.Add(-time.Second))},
				},
			}},
			requestGeneration: 2,
			requestStartTime:  200,
			requestTokens:     2,
		},
		{
			name: "selected waiter may receive no tokens",
			component: &Component{ConcurrencyState: &fcpb.ConcurrencyState{
				Config:     &taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 1},
				Generation: 2,
				WakeAll:    true,
				Slots: []*fcpb.ConcurrencyState_Slot{
					{SlotId: "committed", Committed: true},
				},
			}},
			requestGeneration: 2,
			requestStartTime:  200,
			requestTokens:     1,
			wantGeneration:    2,
			wantReady:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generation, tokens, ready := tt.component.poll(
				now,
				tt.requestGeneration,
				tt.requestStartTime,
				tt.requestTokens,
			)
			require.Equal(t, tt.wantGeneration, generation)
			require.Equal(t, tt.wantTokens, tokens)
			require.Equal(t, tt.wantReady, ready)
		})
	}
}

func TestIncrementGenerationResetsWakeState(t *testing.T) {
	c := newTestComponent(1)
	c.Generation = 3
	c.WakeUpTo = 100
	c.WakeAll = true
	c.WakeStage = 4

	c.incrementGeneration()

	require.Equal(t, int64(4), c.Generation)
	require.Zero(t, c.WakeUpTo)
	require.False(t, c.WakeAll)
	require.Zero(t, c.WakeStage)
}

func TestUpdateConfigRequiresNewerVersion(t *testing.T) {
	c := newTestComponent(1)
	c.ConfigVersion = 2

	c.updateConfig(&taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 2}, 2)
	require.Equal(t, int32(1), c.Config.ConcurrentTasks)

	c.updateConfig(&taskqueuepb.ConcurrencyLimit{ConcurrentTasks: 3}, 3)
	require.Equal(t, int32(3), c.Config.ConcurrentTasks)
	require.Equal(t, int64(3), c.ConfigVersion)
}
