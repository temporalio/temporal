package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	test "go.temporal.io/server/common/testing"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newCallbackTestContext() *chasm.MockMutableContext {
	testTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	return &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return testTime },
		},
	}
}

func newScheduledTestActivity() *Activity {
	return &Activity{
		ActivityState: &activitypb.ActivityState{Status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED},
	}
}

func newNexusCallback() *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: "https://nexus.ex.xxxxx.cluster.tmprl.cloud:7243/Namespaces/ex.xxxxx/nexus/callback",
				Header: map[string]string{
					"Nexus-Operation-State": "succeeded",
				},
			},
		},
	}
}

func TestAddCompletionCallbacks(t *testing.T) {
	t.Parallel()
	cbValidator := test.NewCallbacksValidator(t, test.NewCallbacksValidatorConfig())

	sumCallbackSizes := func(ctx chasm.MutableContext, a *Activity) int64 {
		var size int64
		for _, chasmCB := range a.Callbacks {
			size += int64(chasmCB.Get(ctx).GetCallback().Size())
		}
		return size
	}

	t.Run("AttachesCallbacksInStandby", func(t *testing.T) {
		ctx := newCallbackTestContext()
		a := newScheduledTestActivity()

		cb1 := newNexusCallback()
		cb1.GetNexus().Url = "https://example.com/callback-1"
		cb2 := newNexusCallback()
		cb2.GetNexus().Url = "https://example.com/callback-2"

		err := callback.ValidateAndAttach(ctx, a, "req-id", timestamppb.New(ctx.Now(a)), []*commonpb.Callback{cb1, cb2}, "ns-name", cbValidator)
		require.NoError(t, err)
		require.Len(t, a.Callbacks, 2)

		first, ok := a.Callbacks["req-id-0"]
		require.True(t, ok)
		require.Equal(t, "https://example.com/callback-1", first.Get(ctx).GetCallback().GetNexus().GetUrl())

		second, ok := a.Callbacks["req-id-1"]
		require.True(t, ok)
		require.Equal(t, "https://example.com/callback-2", second.Get(ctx).GetCallback().GetNexus().GetUrl())

		// Callbacks stay in STANDBY until the activity reaches a terminal state.
		for _, field := range a.Callbacks {
			require.Equal(t, callbackspb.CALLBACK_STATUS_STANDBY, field.Get(ctx).Status)
		}
	})

	t.Run("EmptyListIsNoOp", func(t *testing.T) {
		ctx := newCallbackTestContext()
		a := newScheduledTestActivity()

		require.NoError(t, callback.ValidateAndAttach(ctx, a, "req-id", timestamppb.New(ctx.Now(a)), nil, "ns-name", cbValidator))
		// The fields get initialized lazily. The noop should keep them as-is.
		require.Nil(t, a.Callbacks)
		require.Nil(t, a.CallbackMetadata)
	})

	t.Run("DistinctRequestsAccumulate", func(t *testing.T) {
		ctx := newCallbackTestContext()
		a := newScheduledTestActivity()
		cbs := []*commonpb.Callback{newNexusCallback()}

		require.NoError(t, callback.ValidateAndAttach(ctx, a, "req-1", timestamppb.New(ctx.Now(a)), cbs, "ns-name", cbValidator))
		require.NoError(t, callback.ValidateAndAttach(ctx, a, "req-2", timestamppb.New(ctx.Now(a)), cbs, "ns-name", cbValidator))
		require.Len(t, a.Callbacks, 2)
	})

	t.Run("RejectsExceedingTheLimit", func(t *testing.T) {
		ctx := newCallbackTestContext()
		a := newScheduledTestActivity()

		// callbacks.Validator that enforces a limit of only 2 callbacks per execution.
		cfg := test.NewCallbacksValidatorConfig()
		cfg.MaxCallbacksPerExecution = func(string) int { return 2 }
		max2CallbacksValidator := test.NewCallbacksValidator(t, cfg)

		var (
			err                   error
			failedPreconditionErr *serviceerror.FailedPrecondition
		)
		// Try to exceed the limit initially.
		err = callback.ValidateAndAttach(ctx, a, "req-1", timestamppb.New(ctx.Now(a)), []*commonpb.Callback{
			newNexusCallback(),
			newNexusCallback(),
			newNexusCallback(),
		}, "ns-name", max2CallbacksValidator)

		require.ErrorAs(t, err, &failedPreconditionErr)
		require.ErrorContains(t, err, "cannot attach more than 2 callbacks to an execution")
		require.ErrorContains(t, err, "0 callbacks already attached")
		require.Empty(t, a.Callbacks)

		// Add one callback, and then try to add 2 more.
		require.NoError(t, callback.ValidateAndAttach(ctx, a, "req-1", timestamppb.New(ctx.Now(a)), []*commonpb.Callback{
			newNexusCallback(),
		}, "ns-name", max2CallbacksValidator))

		err = callback.ValidateAndAttach(ctx, a, "req-2", timestamppb.New(ctx.Now(a)), []*commonpb.Callback{
			newNexusCallback(),
			newNexusCallback(),
		}, "ns-name", max2CallbacksValidator)

		require.ErrorAs(t, err, &failedPreconditionErr)
		require.ErrorContains(t, err, "1 callbacks already attached")
		require.Len(t, a.Callbacks, 1)
	})

	t.Run("RejectsAClosedActivity", func(t *testing.T) {
		ctx := newCallbackTestContext()
		a := &Activity{
			ActivityState: &activitypb.ActivityState{Status: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
		}

		err := callback.ValidateAndAttach(ctx, a, "req-id", timestamppb.New(ctx.Now(a)), []*commonpb.Callback{
			newNexusCallback(),
		}, "ns-name", cbValidator)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.ErrorContains(t, err, "cannot attach callbacks to a closed execution")
		require.Empty(t, a.Callbacks)
	})

	t.Run("TracksTotalCallbacksSize", func(t *testing.T) {
		ctx := newCallbackTestContext()
		a := newScheduledTestActivity()
		require.Nil(t, a.GetCallbackMetadata())

		require.NoError(t, callback.ValidateAndAttach(ctx, a, "req-1", timestamppb.New(ctx.Now(a)), []*commonpb.Callback{
			newNexusCallback(),
			newNexusCallback(),
		}, "ns-name", cbValidator))
		require.Positive(t, a.GetCallbackMetadata().TotalCallbacksSize)
		require.Equal(t, sumCallbackSizes(ctx, a), a.GetCallbackMetadata().TotalCallbacksSize)
		sizeAfterFirstRequest := a.GetCallbackMetadata().TotalCallbacksSize

		require.NoError(t, callback.ValidateAndAttach(ctx, a, "req-2", timestamppb.New(ctx.Now(a)), []*commonpb.Callback{
			newNexusCallback(),
		}, "ns-name", cbValidator))
		require.Greater(t, a.GetCallbackMetadata().TotalCallbacksSize, sizeAfterFirstRequest)
		require.Equal(t, sumCallbackSizes(ctx, a), a.GetCallbackMetadata().TotalCallbacksSize)
	})

	t.Run("RejectsExceedingTheTotalSizeLimit", func(t *testing.T) {
		ctx := newCallbackTestContext()
		a := newScheduledTestActivity()
		require.NoError(t, callback.ValidateAndAttach(ctx, a, "req-1", timestamppb.New(ctx.Now(a)), []*commonpb.Callback{
			newNexusCallback(),
		}, "ns-name", cbValidator))

		// A budget with no room left for a second callback of the same size.
		cfg := test.NewCallbacksValidatorConfig()
		cfg.TotalCallbacksMaxSize = func(string) int { return int(a.GetCallbackMetadata().TotalCallbacksSize + 1) }
		validator := test.NewCallbacksValidator(t, cfg)

		err := callback.ValidateAndAttach(ctx, a, "req-2", timestamppb.New(ctx.Now(a)), []*commonpb.Callback{
			newNexusCallback(),
		}, "ns-name", validator)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.ErrorContains(t, err, "bytes already attached")
		require.Len(t, a.Callbacks, 1)
		require.Equal(t, sumCallbackSizes(ctx, a), a.GetCallbackMetadata().TotalCallbacksSize)
	})
}
