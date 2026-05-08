package callback

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/components/nexusoperations"
)

// Confirm that callback delivery failures due to the Nexus operation not having
// started will be retried and not trigger circuit breakers.
func TestCallbacksToUnstartedNexusOperations(t *testing.T) {
	cb := &Callback{
		CallbackState: &callbackspb.CallbackState{
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{
						Url: commonnexus.SystemCallbackURL,
					},
				},
			},
			Status: callbackspb.CALLBACK_STATUS_SCHEDULED,
		},
	}

	// Simulate the InvocationTask being executed, which ends with the invocation's
	// result being saved on the Callback.
	mctx := &chasm.MockMutableContext{}
	_, err := cb.saveResult(mctx, saveResultInput{
		result:      invocationResultRetry{err: nexusoperations.ErrOperationNotStarted},
		retryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
	})

	require.NoError(t, err)
	require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.StateMachineState())
	require.Equal(t, int32(1), cb.Attempt)
	require.Equal(t, "nexus operation not started", cb.LastAttemptFailure.Message)
	require.False(t, cb.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
	require.NotNil(t, cb.NextAttemptScheduleTime)

	_, ok := cb.TerminalFailure.TryGet(mctx)
	require.False(t, ok)

	// Confirm backoff task was generated.
	require.Len(t, mctx.Tasks, 1)
	require.IsType(t, &callbackspb.BackoffTask{}, mctx.Tasks[0].Payload)
}
