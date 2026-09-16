package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func nexusCallback(url string) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{Url: url},
		},
	}
}

// TestAddUpdateCompletionCallbacks verifies that:
//   - if per-update limit is exceeded while workflow-wide limit is not exceeded,
//     we reject attaching callbacks onto that update.
//   - if workflow-wide limit is exceeded while per-update limit is not exceeded,
//     we reject attaching callbacks onto that update.
func TestAddUpdateCompletionCallbacks_LimitsExceeded(t *testing.T) {
	tests := []struct {
		name                    string
		firstUpdateID           string
		firstCallbacks          []*commonpb.Callback
		secondUpdateID          string
		secondCallbacks         []*commonpb.Callback
		maxCallbacksPerWorkflow int
		maxCallbacksPerUpdateID int
		wantError               string
		wantCallbackCounts      map[string]int
	}{
		{
			name:          "per-update limit exceeded",
			firstUpdateID: "u1",
			firstCallbacks: []*commonpb.Callback{
				nexusCallback("http://cb-1"),
				nexusCallback("http://cb-2"),
			},
			secondUpdateID: "u1",
			secondCallbacks: []*commonpb.Callback{
				nexusCallback("http://cb-3"),
			},
			maxCallbacksPerWorkflow: 10,
			maxCallbacksPerUpdateID: 2,
			wantError:               `cannot attach more than 2 callbacks to update "u1"`,
			wantCallbackCounts:      map[string]int{"u1": 2},
		},
		{
			name:          "per-workflow limit exceeded",
			firstUpdateID: "u1",
			firstCallbacks: []*commonpb.Callback{
				nexusCallback("http://cb-1"),
				nexusCallback("http://cb-2"),
			},
			secondUpdateID: "u2",
			secondCallbacks: []*commonpb.Callback{
				nexusCallback("http://cb-3"),
				nexusCallback("http://cb-4"),
			},
			maxCallbacksPerWorkflow: 3,
			maxCallbacksPerUpdateID: 10,
			wantError:               "cannot attach more than 3 callbacks to a workflow",
			wantCallbackCounts:      map[string]int{"u1": 2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctx := &chasm.MockMutableContext{}
			wf := &Workflow{MSPointer: chasm.NewMSPointer(&chasm.MockNodeBackend{})}
			eventTime := timestamppb.Now()

			// Add the first set of callbacks, expect this to succeed.
			require.NoError(t, wf.AddUpdateCompletionCallbacks(
				ctx,
				eventTime,
				test.firstUpdateID,
				"req-1",
				test.firstCallbacks,
				test.maxCallbacksPerWorkflow,
				test.maxCallbacksPerUpdateID,
			))

			// Add the second set of callbacks, expect this to fail with the expected error.
			err := wf.AddUpdateCompletionCallbacks(
				ctx,
				eventTime,
				test.secondUpdateID,
				"req-2",
				test.secondCallbacks,
				test.maxCallbacksPerWorkflow,
				test.maxCallbacksPerUpdateID,
			)
			var failedPrecondition *serviceerror.FailedPrecondition
			require.ErrorAs(t, err, &failedPrecondition)
			require.ErrorContains(t, err, test.wantError)

			// Verify the update state for already-added callbacks is unchanged.
			require.Len(t, wf.Updates, len(test.wantCallbackCounts))
			for updateID, wantCount := range test.wantCallbackCounts {
				update, ok := wf.Updates[updateID]
				require.True(t, ok)
				require.Len(t, update.Get(ctx).Callbacks, wantCount)
			}
		})
	}
}
