package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common/callbacks"
	test "go.temporal.io/server/common/testing"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testMaxCallbacksPerUpdateID = 1000

func newTestWorkflow() *Workflow {
	return &Workflow{
		WorkflowState: &chasmworkflowpb.WorkflowState{},
		MSPointer:     chasm.NewMSPointer(&chasm.MockNodeBackend{}),
	}
}

func nexusCallback(url string) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{Url: url},
		},
	}
}

func nexusCallbacks(urls ...string) []*commonpb.Callback {
	cbs := make([]*commonpb.Callback, len(urls))
	for i, url := range urls {
		cbs[i] = nexusCallback(url)
	}
	return cbs
}

func TestAddCompletionCallbacks(t *testing.T) {
	t.Parallel()

	t.Run("AttachesAndTracksTotals", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()
		cbs := nexusCallbacks("http://cb-1", "http://cb-2")

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs))
		require.Len(t, wf.Callbacks, 2)
		require.Equal(t, "http://cb-1", wf.Callbacks["req-1-0"].Get(ctx).GetCallback().GetNexus().GetUrl())
		require.Equal(t, "http://cb-2", wf.Callbacks["req-1-1"].Get(ctx).GetCallback().GetNexus().GetUrl())

		require.Equal(t, int64(2), wf.GetTotalCallbacksCount())
		require.Equal(t, int64(cbs[0].Size()+cbs[1].Size()), wf.GetTotalCallbacksSize())
	})

	t.Run("EmptyListIsNoOp", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", nil))
		require.Nil(t, wf.Callbacks)
		require.Zero(t, wf.GetTotalCallbacksCount())
		require.Zero(t, wf.GetTotalCallbacksSize())
	})

	t.Run("ReattachingTheSameRequestIsANoOp", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()
		cbs := nexusCallbacks("http://cb-1", "http://cb-2")

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs))
		sizeAfterFirstAttach := wf.GetTotalCallbacksSize()

		// The admitted and accepted events of an update both carry the same callbacks; the
		// second attach must not be counted twice.
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs))
		require.Len(t, wf.Callbacks, 2)
		require.Equal(t, int64(2), wf.GetTotalCallbacksCount())
		require.Equal(t, sizeAfterFirstAttach, wf.GetTotalCallbacksSize())
	})

	t.Run("DistinctRequestsAccumulate", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", nexusCallbacks("http://cb-1")))
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-2", nexusCallbacks("http://cb-2")))
		require.Len(t, wf.Callbacks, 2)
		require.Equal(t, int64(2), wf.GetTotalCallbacksCount())
	})

	t.Run("CountsWorkflowAndUpdateCallbacksTogether", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", nexusCallbacks("http://cb-1")))
		require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, timestamppb.Now(), "u1", "req-2", nexusCallbacks("http://cb-2")))
		require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, timestamppb.Now(), "u2", "req-3", nexusCallbacks("http://cb-3", "http://cb-4")))

		require.Len(t, wf.Callbacks, 1)
		require.Len(t, wf.Updates["u1"].Get(ctx).Callbacks, 1)
		require.Len(t, wf.Updates["u2"].Get(ctx).Callbacks, 2)
		require.Equal(t, int64(4), wf.GetTotalCallbacksCount())
	})
}

func TestValidateCallbackAddition(t *testing.T) {
	t.Parallel()

	validatorWithMaxCount := func(t *testing.T, maxCount int) callbacks.Validator {
		t.Helper()
		cfg := test.NewCallbacksValidatorConfig()
		cfg.MaxCallbacksPerExecution = func(string) int { return maxCount }
		return test.NewCallbacksValidator(t, cfg)
	}
	var failedPrecondition *serviceerror.FailedPrecondition

	t.Run("AllowsAdditionsWithinTheLimits", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()

		require.NoError(t, wf.ValidateCallbackAddition(
			ctx,
			nil,
			CallbackAddition{RequestID: "req-1", Callbacks: nexusCallbacks("http://cb-1", "http://cb-2")},
			"ns-name",
			validatorWithMaxCount(t, 3),
			testMaxCallbacksPerUpdateID,
		))
	})

	t.Run("RejectsExceedingTheExecutionCount", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", nexusCallbacks("http://cb-1", "http://cb-2")))

		err := wf.ValidateCallbackAddition(
			ctx,
			nil,
			CallbackAddition{RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3", "http://cb-4")},
			"ns-name",
			validatorWithMaxCount(t, 3),
			testMaxCallbacksPerUpdateID,
		)
		require.ErrorAs(t, err, &failedPrecondition)
		require.ErrorContains(t, err, "cannot attach more than 3 callbacks to an execution (2 callbacks already attached)")
	})

	t.Run("CountsUpdateCallbacksAgainstTheExecutionLimit", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()
		require.NoError(t, wf.AddUpdateCompletionCallbacks(
			ctx, timestamppb.Now(), "u1", "req-1", nexusCallbacks("http://cb-1", "http://cb-2"),
		))

		err := wf.ValidateCallbackAddition(
			ctx,
			nil,
			CallbackAddition{RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3", "http://cb-4")},
			"ns-name",
			validatorWithMaxCount(t, 3),
			testMaxCallbacksPerUpdateID,
		)
		require.ErrorContains(t, err, "cannot attach more than 3 callbacks to an execution (2 callbacks already attached)")
	})

	t.Run("RejectsExceedingThePerUpdateLimit", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()
		validator := validatorWithMaxCount(t, 100)
		require.NoError(t, wf.AddUpdateCompletionCallbacks(
			ctx, timestamppb.Now(), "u1", "req-1", nexusCallbacks("http://cb-1", "http://cb-2"),
		))

		err := wf.ValidateCallbackAddition(
			ctx,
			nil,
			CallbackAddition{UpdateID: "u1", RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3")},
			"ns-name",
			validator,
			2,
		)
		require.ErrorAs(t, err, &failedPrecondition)
		require.ErrorContains(t, err, `cannot attach more than 2 callbacks to update "u1" (2 callbacks already attached)`)

		// The workflow's own callbacks are not subject to the per-update limit.
		require.NoError(t, wf.ValidateCallbackAddition(
			ctx,
			nil,
			CallbackAddition{RequestID: "req-3", Callbacks: nexusCallbacks("http://cb-4", "http://cb-5", "http://cb-6")},
			"ns-name",
			validator,
			2,
		))
	})

	t.Run("RejectsExceedingTheAggregateSize", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()
		cbs := nexusCallbacks("http://cb-1")

		cfg := test.NewCallbacksValidatorConfig()
		cfg.TotalCallbacksMaxSize = func(string) int { return cbs[0].Size() }
		validator := test.NewCallbacksValidator(t, cfg)

		require.NoError(t, wf.ValidateCallbackAddition(
			ctx, nil, CallbackAddition{RequestID: "req-1", Callbacks: cbs}, "ns-name", validator, testMaxCallbacksPerUpdateID,
		))
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs))

		err := wf.ValidateCallbackAddition(
			ctx,
			nil,
			CallbackAddition{RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-2")},
			"ns-name",
			validator,
			testMaxCallbacksPerUpdateID,
		)
		require.ErrorAs(t, err, &failedPrecondition)
		require.ErrorContains(t, err, "bytes of callbacks to an execution")
	})

	t.Run("DoesNotRecountAnAlreadyAttachedRequest", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()
		validator := validatorWithMaxCount(t, 3)
		wfCBs := nexusCallbacks("http://cb-1", "http://cb-2")
		updateCBs := nexusCallbacks("http://cb-3", "http://cb-4")
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", wfCBs))
		require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, timestamppb.Now(), "u1", "req-2", updateCBs))

		// A retried request whose callbacks are already attached is a no-op when attached, so it
		// is not counted a second time, even though the execution and the update are at their limit.
		require.NoError(t, wf.ValidateCallbackAddition(
			ctx, nil, CallbackAddition{RequestID: "req-1", Callbacks: wfCBs}, "ns-name", validator, 2,
		))
		require.NoError(t, wf.ValidateCallbackAddition(
			ctx, nil, CallbackAddition{UpdateID: "u1", RequestID: "req-2", Callbacks: updateCBs}, "ns-name", validator, 2,
		))
	})

	t.Run("ToleratesAnUpdateWithoutAValue", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()
		wf.Updates = chasm.Map[string, *WorkflowUpdate]{"u1": chasm.NewEmptyField[*WorkflowUpdate]()}

		require.NoError(t, wf.ValidateCallbackAddition(
			ctx,
			nil,
			CallbackAddition{UpdateID: "u1", RequestID: "req-1", Callbacks: nexusCallbacks("http://cb-1")},
			"ns-name",
			validatorWithMaxCount(t, 3),
			testMaxCallbacksPerUpdateID,
		))
	})

	t.Run("EmptyAdditionIsANoOp", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()

		require.NoError(t, wf.ValidateCallbackAddition(
			ctx, nil, CallbackAddition{RequestID: "req-1"}, "ns-name", validatorWithMaxCount(t, 0), 0,
		))
	})
}

// In-flight callbacks belong to Updates admitted but not yet accepted. They are reserved against
// the limits so that concurrent Updates cannot each pass admission and then jointly exceed them
// on acceptance, but they must never be counted twice.
func TestValidateCallbackAddition_InFlight(t *testing.T) {
	t.Parallel()

	validatorWithMaxCount := func(t *testing.T, maxCount int) callbacks.Validator {
		t.Helper()
		cfg := test.NewCallbacksValidatorConfig()
		cfg.MaxCallbacksPerExecution = func(string) int { return maxCount }
		return test.NewCallbacksValidator(t, cfg)
	}
	inFlightU1 := CallbackAddition{UpdateID: "u1", RequestID: "req-1", Callbacks: nexusCallbacks("http://cb-1", "http://cb-2")}

	t.Run("ReservesAgainstTheExecutionCount", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()

		err := wf.ValidateCallbackAddition(
			ctx,
			[]CallbackAddition{inFlightU1},
			CallbackAddition{UpdateID: "u2", RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3", "http://cb-4")},
			"ns-name",
			validatorWithMaxCount(t, 3),
			testMaxCallbacksPerUpdateID,
		)
		var failedPrecondition *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecondition)
		require.ErrorContains(t, err, "cannot attach more than 3 callbacks to an execution (2 callbacks already attached)")
	})

	t.Run("ReservesAgainstTheExecutionSize", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()
		addition := CallbackAddition{UpdateID: "u2", RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3")}

		cfg := test.NewCallbacksValidatorConfig()
		inFlightSize := inFlightU1.Callbacks[0].Size() + inFlightU1.Callbacks[1].Size()
		cfg.TotalCallbacksMaxSize = func(string) int { return inFlightSize + addition.Callbacks[0].Size() - 1 }

		err := wf.ValidateCallbackAddition(
			ctx,
			[]CallbackAddition{inFlightU1},
			addition,
			"ns-name",
			test.NewCallbacksValidator(t, cfg),
			testMaxCallbacksPerUpdateID,
		)
		var failedPrecondition *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecondition)
		require.ErrorContains(t, err, "bytes of callbacks to an execution")
	})

	t.Run("ReservesAgainstTheUpdateCount", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()

		err := wf.ValidateCallbackAddition(
			ctx,
			[]CallbackAddition{inFlightU1},
			CallbackAddition{UpdateID: "u1", RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3")},
			"ns-name",
			validatorWithMaxCount(t, 10),
			2,
		)
		var failedPrecondition *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecondition)
		require.ErrorContains(t, err, `cannot attach more than 2 callbacks to update "u1" (2 callbacks already attached)`)
	})

	t.Run("DoesNotRecountAReofferedInFlightRequest", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()

		// A retry of the request that admitted the Update carries the same callbacks.
		require.NoError(t, wf.ValidateCallbackAddition(
			ctx,
			[]CallbackAddition{inFlightU1},
			inFlightU1,
			"ns-name",
			validatorWithMaxCount(t, 2),
			2,
		))
	})

	t.Run("CountsARequestBufferedTwiceOnce", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()

		// A retry of the admitting request while the Update is with the worker is buffered
		// alongside the original request, so the Registry reports it twice.
		require.NoError(t, wf.ValidateCallbackAddition(
			ctx,
			[]CallbackAddition{inFlightU1, inFlightU1},
			CallbackAddition{UpdateID: "u2", RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3")},
			"ns-name",
			validatorWithMaxCount(t, 3),
			testMaxCallbacksPerUpdateID,
		))
	})

	t.Run("DoesNotRecountAnInFlightRequestAlreadyPersisted", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		wf := newTestWorkflow()
		require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, timestamppb.Now(), "u1", "req-1", inFlightU1.Callbacks))

		// The Update was admitted from an UpdateAdmitted event, which persisted its callbacks, and a
		// retry of that request was then buffered.
		require.NoError(t, wf.ValidateCallbackAddition(
			ctx,
			[]CallbackAddition{inFlightU1},
			CallbackAddition{UpdateID: "u2", RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3")},
			"ns-name",
			validatorWithMaxCount(t, 3),
			2,
		))
	})
}
