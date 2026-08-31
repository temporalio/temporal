package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTestWorkflowForCallbackLimits() *Workflow {
	backend := &chasm.MockNodeBackend{}
	return &Workflow{MSPointer: chasm.NewMSPointer(backend)}
}

func nexusCallback(url string) *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{Url: url},
		},
	}
}

func TestAddUpdateCompletionCallbacks_PerUpdateLimitExceeded(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	wf := newTestWorkflowForCallbackLimits()
	eventTime := timestamppb.Now()

	err := wf.AddUpdateCompletionCallbacks(
		ctx,
		eventTime,
		"u1",
		"req-1",
		[]*commonpb.Callback{nexusCallback("http://cb-1"), nexusCallback("http://cb-2")},
		10,
		2,
	)
	require.NoError(t, err)
	require.Len(t, wf.Updates["u1"].Get(ctx).Callbacks, 2)

	err = wf.AddUpdateCompletionCallbacks(ctx, eventTime, "u1", "req-2",
		[]*commonpb.Callback{nexusCallback("http://cb-3")},
		10, 2,
	)
	require.Error(t, err)
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
	require.ErrorContains(t, err, `cannot attach more than 2 callbacks to update "u1"`)
	require.Len(t, wf.Updates["u1"].Get(ctx).Callbacks, 2, "the already-attached callbacks must still be there")
}

func TestAddUpdateCompletionCallbacks_WorkflowWideLimitSharedAcrossUpdates(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	wf := newTestWorkflowForCallbackLimits()
	eventTime := timestamppb.Now()

	require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, eventTime, "u1", "req-1",
		[]*commonpb.Callback{nexusCallback("http://cb-1"), nexusCallback("http://cb-2")},
		3,
		10,
	))

	err := wf.AddUpdateCompletionCallbacks(ctx, eventTime, "u2", "req-2",
		[]*commonpb.Callback{nexusCallback("http://cb-3"), nexusCallback("http://cb-4")},
		3, 10,
	)
	require.Error(t, err)
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
	require.ErrorContains(t, err, "cannot attach more than 3 callbacks to a workflow")

	require.Len(t, wf.Updates["u1"].Get(ctx).Callbacks, 2, "u1's callbacks must be untouched")
	_, u2Exists := wf.Updates["u2"]
	require.False(t, u2Exists, "u2 should not have been created since its callbacks were rejected")
}

func TestAddUpdateCompletionCallbacks_IdempotentAtLimits(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	wf := newTestWorkflowForCallbackLimits()
	eventTime := timestamppb.Now()
	callbacks := []*commonpb.Callback{nexusCallback("http://cb-1"), nexusCallback("http://cb-2")}

	require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, eventTime, "u1", "req-1", callbacks, 2, 2))
	require.NoError(t, wf.AddUpdateCompletionCallbacks(ctx, eventTime, "u1", "req-1", callbacks, 2, 2))
	require.Len(t, wf.Updates["u1"].Get(ctx).Callbacks, 2)
}

func TestAddCompletionCallbacks_IdempotentAtLimit(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	wf := newTestWorkflowForCallbackLimits()
	eventTime := timestamppb.Now()
	callbacks := []*commonpb.Callback{nexusCallback("http://cb-1")}

	require.NoError(t, wf.AddCompletionCallbacks(ctx, eventTime, "req-1", callbacks, 1))
	require.NoError(t, wf.AddCompletionCallbacks(ctx, eventTime, "req-1", callbacks, 1))
	require.Len(t, wf.Callbacks, 1)
}

func TestAddUpdateCompletionCallbacks_WorkflowLimitIncludesWorkflowCallbacks(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	wf := newTestWorkflowForCallbackLimits()
	eventTime := timestamppb.Now()

	require.NoError(t, wf.AddCompletionCallbacks(
		ctx,
		eventTime,
		"workflow-request",
		[]*commonpb.Callback{nexusCallback("http://workflow-cb")},
		2,
	))
	require.NoError(t, wf.AddUpdateCompletionCallbacks(
		ctx,
		eventTime,
		"u1",
		"update-request-1",
		[]*commonpb.Callback{nexusCallback("http://update-cb-1")},
		2,
		2,
	))

	err := wf.AddUpdateCompletionCallbacks(
		ctx,
		eventTime,
		"u2",
		"update-request-2",
		[]*commonpb.Callback{nexusCallback("http://update-cb-2")},
		2,
		2,
	)
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
	require.Len(t, wf.Callbacks, 1)
	require.Len(t, wf.Updates["u1"].Get(ctx).Callbacks, 1)
	_, exists := wf.Updates["u2"]
	require.False(t, exists)
}

func TestAddUpdateCompletionCallbacks_FailureDoesNotCreateUpdate(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	wf := newTestWorkflowForCallbackLimits()
	eventTime := timestamppb.Now()

	err := wf.AddUpdateCompletionCallbacks(
		ctx,
		eventTime,
		"u1",
		"req-1",
		[]*commonpb.Callback{nexusCallback("http://cb-1"), nexusCallback("http://cb-2")},
		10,
		1,
	)
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
	_, exists := wf.Updates["u1"]
	require.False(t, exists)
}
