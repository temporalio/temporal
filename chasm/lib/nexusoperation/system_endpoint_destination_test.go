package nexusoperation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTaskDestination(t *testing.T) {
	require.Equal(t, "test-endpoint", taskDestination("test-endpoint"))
	require.Empty(t, taskDestination(commonnexus.SystemEndpoint))
}

// System endpoint tasks must have no destination so that they are scheduled on the transfer queue rather than being
// grouped by destination on the outbound queue.
func TestSystemEndpointTasksHaveNoDestination(t *testing.T) {
	newCtx := func() *chasm.MockMutableContext {
		return &chasm.MockMutableContext{
			MockContext: chasm.MockContext{
				HandleNow: func(chasm.Component) time.Time { return defaultTime },
			},
		}
	}
	newSystemOperation := func() *Operation {
		op := newTestOperation()
		op.Endpoint = commonnexus.SystemEndpoint
		return op
	}

	t.Run("scheduled", func(t *testing.T) {
		ctx := newCtx()
		op := newSystemOperation()
		require.NoError(t, TransitionScheduled.Apply(op, ctx, EventScheduled{}))
		require.NotEmpty(t, ctx.Tasks)
		require.IsType(t, &nexusoperationpb.InvocationTask{}, ctx.Tasks[0].Payload)
		require.Empty(t, ctx.Tasks[0].Attributes.Destination)
		require.True(t, ctx.Tasks[0].Attributes.IsImmediate())
	})

	t.Run("rescheduled", func(t *testing.T) {
		ctx := newCtx()
		op := newSystemOperation()
		op.Status = nexusoperationpb.OPERATION_STATUS_BACKING_OFF
		require.NoError(t, transitionRescheduled.Apply(op, ctx, EventRescheduled{}))
		require.Len(t, ctx.Tasks, 1)
		require.IsType(t, &nexusoperationpb.InvocationTask{}, ctx.Tasks[0].Payload)
		require.Empty(t, ctx.Tasks[0].Attributes.Destination)
	})

	t.Run("cancellation scheduled", func(t *testing.T) {
		ctx := newCtx()
		cancellation := newCancellation(&nexusoperationpb.CancellationState{Status: nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED})
		require.NoError(t, TransitionCancellationScheduled.Apply(cancellation, ctx, EventCancellationScheduled{
			Destination: commonnexus.SystemEndpoint,
		}))
		require.Len(t, ctx.Tasks, 1)
		require.IsType(t, &nexusoperationpb.CancellationTask{}, ctx.Tasks[0].Payload)
		require.Empty(t, ctx.Tasks[0].Attributes.Destination)
	})

	t.Run("cancellation rescheduled", func(t *testing.T) {
		ctx := newCtx()
		op := newSystemOperation()
		cancellation := newCancellation(&nexusoperationpb.CancellationState{
			Status:                  nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
			Attempt:                 1,
			NextAttemptScheduleTime: timestamppb.New(defaultTime.Add(time.Minute)),
		})
		cancellation.Operation = chasm.NewMockParentPtr[*Operation](op)
		require.NoError(t, transitionCancellationRescheduled.Apply(cancellation, ctx, EventCancellationRescheduled{}))
		require.Len(t, ctx.Tasks, 1)
		require.IsType(t, &nexusoperationpb.CancellationTask{}, ctx.Tasks[0].Payload)
		require.Empty(t, ctx.Tasks[0].Attributes.Destination)
	})
}
