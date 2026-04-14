package nexusoperation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

func newScheduledTestOperation(t *testing.T, ctx *chasm.MockMutableContext) *Operation {
	t.Helper()
	op := newTestOperation()
	require.NoError(t, TransitionScheduled.Apply(op, ctx, EventScheduled{}))
	return op
}

func TestHandleNexusCompletion(t *testing.T) {
	newStartedOp := func(t *testing.T, ctx *chasm.MockMutableContext) *Operation {
		t.Helper()
		op := newScheduledTestOperation(t, ctx)
		require.NoError(t, TransitionStarted.Apply(op, ctx, EventStarted{OperationToken: "tok"}))
		return op
	}
	newCtx := func() *chasm.MockMutableContext {
		return &chasm.MockMutableContext{
			MockContext: chasm.MockContext{
				HandleNow: func(chasm.Component) time.Time { return defaultTime },
			},
		}
	}

	t.Run("Success", func(t *testing.T) {
		ctx := newCtx()
		op := newStartedOp(t, ctx)
		err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
			RequestId: op.GetRequestId(),
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: mustToPayload(t, "result"),
			},
		})
		require.NoError(t, err)
		require.Equal(t, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, op.GetStatus())
	})

	t.Run("Failure", func(t *testing.T) {
		ctx := newCtx()
		op := newStartedOp(t, ctx)
		err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
			RequestId: op.GetRequestId(),
			Outcome: &persistencespb.ChasmNexusCompletion_Failure{
				Failure: &failurepb.Failure{Message: "oops"},
			},
		})
		require.NoError(t, err)
		require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, op.GetStatus())
	})

	t.Run("Canceled", func(t *testing.T) {
		ctx := newCtx()
		op := newStartedOp(t, ctx)
		err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
			RequestId: op.GetRequestId(),
			Outcome: &persistencespb.ChasmNexusCompletion_Failure{
				Failure: &failurepb.Failure{
					Message: "canceled",
					FailureInfo: &failurepb.Failure_CanceledFailureInfo{
						CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, nexusoperationpb.OPERATION_STATUS_CANCELED, op.GetStatus())
	})

	t.Run("RequestIDMismatch", func(t *testing.T) {
		ctx := newCtx()
		op := newStartedOp(t, ctx)
		err := op.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
			RequestId: "wrong-request-id",
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: mustToPayload(t, "result"),
			},
		})
		require.Error(t, err)
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound)
		require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, op.GetStatus())
	})
}
