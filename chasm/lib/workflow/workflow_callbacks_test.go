package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/namespace"
	test "go.temporal.io/server/common/testing"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTestCHASMContext(t *testing.T) *chasm.MockMutableContext {
	ctx := &chasm.MockMutableContext{
		HandleNamespaceEntry: func() *namespace.Namespace {
			return test.NewNamespace(t)
		},
	}
	return ctx
}

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
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()
		cbs := nexusCallbacks("http://cb-1", "http://cb-2")

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs))
		require.Len(t, wf.Callbacks, 2)
		require.Equal(t, "http://cb-1", wf.Callbacks["req-1-0"].Get(ctx).GetCallback().GetNexus().GetUrl())
		require.Equal(t, "http://cb-2", wf.Callbacks["req-1-1"].Get(ctx).GetCallback().GetNexus().GetUrl())

		require.Equal(t, int64(2), wf.GetCallbackMetadata().GetTotalCallbacksCount())
		require.Equal(t, int64(cbs[0].Size()+cbs[1].Size()), wf.GetCallbackMetadata().GetTotalCallbacksSize())
	})

	t.Run("EmptyListIsNoOp", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", nil))
		require.Nil(t, wf.Callbacks)
		require.Zero(t, wf.GetCallbackMetadata().GetTotalCallbacksCount())
		require.Zero(t, wf.GetCallbackMetadata().GetTotalCallbacksSize())
	})

	t.Run("ReattachingTheSameRequestIsANoOp", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()
		cbs := nexusCallbacks("http://cb-1", "http://cb-2")

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs))
		sizeAfterFirstAttach := wf.GetCallbackMetadata().GetTotalCallbacksSize()

		// The admitted and accepted events of an update both carry the same callbacks; the
		// second attach must not be counted twice.
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs))
		require.Len(t, wf.Callbacks, 2)
		require.Equal(t, int64(2), wf.GetCallbackMetadata().GetTotalCallbacksCount())
		require.Equal(t, sizeAfterFirstAttach, wf.GetCallbackMetadata().GetTotalCallbacksSize())
	})

	t.Run("DistinctRequestsAccumulate", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", nexusCallbacks("http://cb-1")))
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-2", nexusCallbacks("http://cb-2")))
		require.Len(t, wf.Callbacks, 2)
		require.Equal(t, int64(2), wf.GetCallbackMetadata().GetTotalCallbacksCount())
	})

	t.Run("CountsWorkflowAndUpdateCallbacksTogether", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()

		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", nexusCallbacks("http://cb-1")))
		require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, timestamppb.Now(), "u1", "req-2", nexusCallbacks("http://cb-2")))
		require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, timestamppb.Now(), "u2", "req-3", nexusCallbacks("http://cb-3", "http://cb-4")))

		require.Len(t, wf.Callbacks, 1)
		require.Len(t, wf.Updates["u1"].Get(ctx).Callbacks, 1)
		require.Len(t, wf.Updates["u2"].Get(ctx).Callbacks, 2)
		require.Equal(t, int64(4), wf.GetCallbackMetadata().GetTotalCallbacksCount())
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
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()

		err := wf.ValidateCallbackAddition(
			ctx,
			nil,
			CallbackAddition{RequestID: "req-1", Callbacks: nexusCallbacks("http://cb-1", "http://cb-2")},
			validatorWithMaxCount(t, 3),
		)
		require.NoError(t, err)
	})

	t.Run("RejectsExceedingTheExecutionCount", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", nexusCallbacks("http://cb-1", "http://cb-2")))

		err := wf.ValidateCallbackAddition(
			ctx,
			nil,
			CallbackAddition{RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3", "http://cb-4")},
			validatorWithMaxCount(t, 3),
		)
		require.ErrorAs(t, err, &failedPrecondition)
		require.ErrorContains(t, err, "cannot attach more than 3 callbacks to an execution (2 callbacks already attached)")
	})

	t.Run("CountsUpdateCallbacksAgainstTheExecutionLimit", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()
		require.NoError(t, wf.AddUpdateCompletionCallbacks(
			ctx, timestamppb.Now(), "u1", "req-1", nexusCallbacks("http://cb-1", "http://cb-2"),
		))

		err := wf.ValidateCallbackAddition(
			ctx,
			nil,
			CallbackAddition{RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3", "http://cb-4")},
			validatorWithMaxCount(t, 3),
		)
		require.ErrorContains(t, err, "cannot attach more than 3 callbacks to an execution (2 callbacks already attached)")
	})

	t.Run("RejectsExceedingTheAggregateSize", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()
		cbs := nexusCallbacks("http://cb-1")

		cfg := test.NewCallbacksValidatorConfig()
		cfg.TotalCallbacksMaxSize = func(string) int { return cbs[0].Size() }
		validator := test.NewCallbacksValidator(t, cfg)

		err := wf.ValidateCallbackAddition(ctx, nil, CallbackAddition{RequestID: "req-1", Callbacks: cbs}, validator)
		require.NoError(t, err)
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", cbs))

		err = wf.ValidateCallbackAddition(
			ctx,
			nil,
			CallbackAddition{RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-2")},
			validator,
		)
		require.ErrorAs(t, err, &failedPrecondition)
		require.ErrorContains(t, err, "bytes of callbacks to an execution")
	})

	t.Run("DoesNotRecountAnAlreadyAttachedRequest", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()
		validator := validatorWithMaxCount(t, 3)
		wfCBs := nexusCallbacks("http://cb-1", "http://cb-2")
		updateCBs := nexusCallbacks("http://cb-3", "http://cb-4")
		require.NoError(t, wf.AddCompletionCallbacks(ctx, timestamppb.Now(), "req-1", wfCBs))
		require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, timestamppb.Now(), "u1", "req-2", updateCBs))

		// A retried request whose callbacks are already attached is a no-op when attached, so it
		// is not counted a second time, even though the execution and the update are at their limit.
		err := wf.ValidateCallbackAddition(ctx, nil, CallbackAddition{RequestID: "req-1", Callbacks: wfCBs}, validator)
		require.NoError(t, err)
		err = wf.ValidateCallbackAddition(ctx, nil, CallbackAddition{UpdateID: "u1", RequestID: "req-2", Callbacks: updateCBs}, validator)
		require.NoError(t, err)
	})

	t.Run("ToleratesAnUpdateWithoutAValue", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()
		wf.Updates = chasm.Map[string, *WorkflowUpdate]{"u1": chasm.NewEmptyField[*WorkflowUpdate]()}

		require.NoError(t, wf.ValidateCallbackAddition(
			ctx,
			nil,
			CallbackAddition{UpdateID: "u1", RequestID: "req-1", Callbacks: nexusCallbacks("http://cb-1")},
			validatorWithMaxCount(t, 3),
		))
	})

	t.Run("EmptyAdditionIsANoOp", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()

		require.NoError(t, wf.ValidateCallbackAddition(
			ctx, nil, CallbackAddition{RequestID: "req-1"}, validatorWithMaxCount(t, 0),
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
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()

		err := wf.ValidateCallbackAddition(
			ctx,
			[]CallbackAddition{inFlightU1},
			CallbackAddition{UpdateID: "u2", RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3", "http://cb-4")},
			validatorWithMaxCount(t, 3),
		)
		var failedPrecondition *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecondition)
		require.ErrorContains(t, err, "cannot attach more than 3 callbacks to an execution (2 callbacks already attached)")
	})

	t.Run("ReservesAgainstTheExecutionSize", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()
		addition := CallbackAddition{UpdateID: "u2", RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3")}

		cfg := test.NewCallbacksValidatorConfig()
		inFlightSize := inFlightU1.Callbacks[0].Size() + inFlightU1.Callbacks[1].Size()
		cfg.TotalCallbacksMaxSize = func(string) int { return inFlightSize + addition.Callbacks[0].Size() - 1 }

		err := wf.ValidateCallbackAddition(
			ctx,
			[]CallbackAddition{inFlightU1},
			addition,
			test.NewCallbacksValidator(t, cfg),
		)
		var failedPrecondition *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPrecondition)
		require.ErrorContains(t, err, "bytes of callbacks to an execution")
	})

	t.Run("DoesNotRecountAReofferedInFlightRequest", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()

		// A retry of the request that admitted the Update carries the same callbacks.
		require.NoError(t, wf.ValidateCallbackAddition(
			ctx,
			[]CallbackAddition{inFlightU1},
			inFlightU1,
			validatorWithMaxCount(t, 2),
		))
	})

	t.Run("CountsARequestBufferedTwiceOnce", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()

		// A retry of the admitting request while the Update is with the worker is buffered
		// alongside the original request, so the Registry reports it twice.
		require.NoError(t, wf.ValidateCallbackAddition(
			ctx,
			[]CallbackAddition{inFlightU1, inFlightU1},
			CallbackAddition{UpdateID: "u2", RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3")},
			validatorWithMaxCount(t, 3),
		))
	})

	t.Run("DoesNotRecountAnInFlightRequestAlreadyPersisted", func(t *testing.T) {
		ctx := newTestCHASMContext(t)
		wf := newTestWorkflow()
		require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, timestamppb.Now(), "u1", "req-1", inFlightU1.Callbacks))

		// The Update was admitted from an UpdateAdmitted event, which persisted its callbacks, and a
		// retry of that request was then buffered.
		require.NoError(t, wf.ValidateCallbackAddition(
			ctx,
			[]CallbackAddition{inFlightU1},
			CallbackAddition{UpdateID: "u2", RequestID: "req-2", Callbacks: nexusCallbacks("http://cb-3")},
			validatorWithMaxCount(t, 3),
		))
	})
}
