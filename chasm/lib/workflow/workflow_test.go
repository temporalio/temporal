package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestAddCallbacksToMapVariants pins which variants reach persisted state, now that the conversion is
// shared with the other libraries via callback.FromAPICallback.
func TestAddCallbacksToMapVariants(t *testing.T) {
	eventTime := timestamppb.New(time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC))

	add := func(t *testing.T, cbs ...*commonpb.Callback) (chasm.Map[string, *callback.Callback], chasm.MutableContext, error) {
		t.Helper()
		ctx := &chasm.MockMutableContext{}
		target := make(chasm.Map[string, *callback.Callback], len(cbs))
		return target, ctx, addCallbacksToMap(ctx, target, "req-id", eventTime, cbs)
	}

	t.Run("PersistsNexus", func(t *testing.T) {
		target, ctx, err := add(t, &commonpb.Callback{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: "http://localhost:8080/cb"},
			},
		})
		require.NoError(t, err)
		require.Len(t, target, 1)
		require.Equal(t, "http://localhost:8080/cb",
			target["req-id-0"].Get(ctx).GetCallback().GetNexus().GetUrl())
	})

	// Representable in persisted state but not deliverable. Attaching one is blocked upstream by
	// callback.Validator, not here; see TestValidateCallbacks/WorkerVariantNotYetSupported.
	t.Run("PersistsWorker", func(t *testing.T) {
		target, ctx, err := add(t, &commonpb.Callback{
			Variant: &commonpb.Callback_Worker_{
				Worker: &commonpb.Callback_Worker{TaskQueueName: "tq", Service: "svc", Operation: "op"},
			},
		})
		require.NoError(t, err)
		require.Len(t, target, 1)
		require.Equal(t, "svc", target["req-id-0"].Get(ctx).GetCallback().GetWorker().GetService())
	})

	t.Run("RejectsVariantsWithNoPersistedRepresentation", func(t *testing.T) {
		for name, cb := range map[string]*commonpb.Callback{
			"internal": {Variant: &commonpb.Callback_Internal_{Internal: &commonpb.Callback_Internal{}}},
			"unset":    {},
		} {
			t.Run(name, func(t *testing.T) {
				_, _, err := add(t, cb)
				var invalidArgErr *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgErr)
				require.Contains(t, err.Error(), "unsupported callback variant")
			})
		}
	})

	// The conversion pass runs to completion before anything is inserted, so one bad callback must not
	// leave the earlier ones attached.
	t.Run("RejectsTheWholeBatchIfAnyCallbackIsUnconvertible", func(t *testing.T) {
		target, _, err := add(t,
			&commonpb.Callback{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: "http://localhost:8080/cb"},
			}},
			&commonpb.Callback{Variant: &commonpb.Callback_Internal_{
				Internal: &commonpb.Callback_Internal{},
			}},
		)
		require.Error(t, err)
		require.Empty(t, target)
	})
}
