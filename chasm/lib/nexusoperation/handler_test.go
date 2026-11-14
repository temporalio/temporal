package nexusoperation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	apinexusoperationpb "go.temporal.io/api/nexusoperation/v1" //nolint:importas
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/callback"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	test "go.temporal.io/server/common/testing"
)

const (
	startTestNamespaceID = "namespace-id"
	startTestOperationID = "operation-id"
)

// newStartTestHandler returns a handler wired to the CHASM test engine and the context to call it with.
func newStartTestHandler(t *testing.T) (*handler, context.Context) {
	t.Helper()
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(callback.NewNilLibrary()))
	require.NoError(t, registry.Register(&Library{
		metricTagConfig: dynamicconfig.GetTypedPropertyFn(NexusMetricTagConfig{}),
	}))

	h := &handler{
		callbackValidator: test.NewCallbacksValidator(t, test.NewCallbacksValidatorConfig()),
		linkValidator:     newTestLinkValidator(10, 10),
		logger:            log.NewTestLogger(),
	}
	return h, chasm.NewEngineContext(context.Background(), chasmtest.NewEngine(t, registry))
}

// startWithAttach calls StartNexusOperation with a completion callback, a link, and on_conflict_options
// that attach both, which goes through UpdateWithStartExecution.
func startWithAttach(
	ctx context.Context,
	h *handler,
	requestID string,
	reusePolicy enumspb.NexusOperationIdReusePolicy,
	conflictPolicy enumspb.NexusOperationIdConflictPolicy,
) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	resp, err := h.StartNexusOperation(ctx, &nexusoperationpb.StartNexusOperationRequest{
		NamespaceId: startTestNamespaceID,
		EndpointId:  "endpoint-id",
		FrontendRequest: &workflowservice.StartNexusOperationExecutionRequest{
			Namespace:           "ns-name",
			OperationId:         startTestOperationID,
			Endpoint:            "test-endpoint",
			Service:             "test-service",
			Operation:           "test-operation",
			RequestId:           requestID,
			IdReusePolicy:       reusePolicy,
			IdConflictPolicy:    conflictPolicy,
			CompletionCallbacks: []*commonpb.Callback{newNexusCallback()},
			Links:               []*commonpb.Link{testLink("wf-" + requestID)},
			OnConflictOptions: &apinexusoperationpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
				AttachLinks:               true,
			},
		},
	})
	return resp.GetFrontendResponse(), err
}

// attachedCounts returns the number of completion callbacks and caller links on the operation run.
func attachedCounts(ctx context.Context, t *testing.T, runID string) (callbacks int, links int) {
	t.Helper()
	ref := chasm.NewComponentRef[*Operation](chasm.ExecutionKey{
		NamespaceID: startTestNamespaceID,
		BusinessID:  startTestOperationID,
		RunID:       runID,
	})
	counts, err := chasm.ReadComponent(ctx, ref, func(o *Operation, ctx chasm.Context, _ any) ([2]int, error) {
		return [2]int{len(o.Callbacks), len(ctx.Links(o))}, nil
	}, nil)
	require.NoError(t, err)
	return counts[0], counts[1]
}

func terminateOperation(ctx context.Context, t *testing.T, h *handler, runID string) {
	t.Helper()
	_, err := h.TerminateNexusOperation(ctx, &nexusoperationpb.TerminateNexusOperationRequest{
		NamespaceId: startTestNamespaceID,
		FrontendRequest: &workflowservice.TerminateNexusOperationExecutionRequest{
			OperationId: startTestOperationID,
			RunId:       runID,
			RequestId:   "terminate-request",
			Reason:      "test",
		},
	})
	require.NoError(t, err)
}

func TestStartNexusOperation_NewStartWithAttach(t *testing.T) {
	h, ctx := newStartTestHandler(t)

	resp, err := startWithAttach(ctx, h, "request-1",
		enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING)
	require.NoError(t, err)
	require.True(t, resp.GetStarted())

	cbs, links := attachedCounts(ctx, t, resp.GetRunId())
	require.Equal(t, 1, cbs, "the update must not attach the new operation's callback again")
	require.Equal(t, 1, links, "the update must not attach the new operation's link again")
}

func TestStartNexusOperation_UseExistingAttaches(t *testing.T) {
	h, ctx := newStartTestHandler(t)

	first, err := startWithAttach(ctx, h, "request-1",
		enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING)
	require.NoError(t, err)

	resp, err := startWithAttach(ctx, h, "request-2",
		enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING)
	require.NoError(t, err)
	require.False(t, resp.GetStarted())
	require.Equal(t, first.GetRunId(), resp.GetRunId())
	cbs, links := attachedCounts(ctx, t, first.GetRunId())
	require.Equal(t, 2, cbs)
	require.Equal(t, 2, links)

	// A retry of an attach that already succeeded does not attach again.
	resp, err = startWithAttach(ctx, h, "request-2",
		enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING)
	require.NoError(t, err)
	require.False(t, resp.GetStarted())
	require.Equal(t, first.GetRunId(), resp.GetRunId())
	cbs, links = attachedCounts(ctx, t, first.GetRunId())
	require.Equal(t, 2, cbs)
	require.Equal(t, 2, links)
}

func TestStartNexusOperation_FailRejectsAttach(t *testing.T) {
	h, ctx := newStartTestHandler(t)

	first, err := startWithAttach(ctx, h, "request-1",
		enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_FAIL)
	require.NoError(t, err)

	_, err = startWithAttach(ctx, h, "request-2",
		enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_FAIL)
	var alreadyStarted *serviceerror.NexusOperationExecutionAlreadyStarted
	require.ErrorAs(t, err, &alreadyStarted)
	require.Equal(t, first.GetRunId(), alreadyStarted.RunId)
	require.Equal(t, "request-1", alreadyStarted.StartRequestId)
	cbs, links := attachedCounts(ctx, t, first.GetRunId())
	require.Equal(t, 1, cbs)
	require.Equal(t, 1, links)

	// A retry of the original start succeeds under FAIL and returns the same run.
	resp, err := startWithAttach(ctx, h, "request-1",
		enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_FAIL)
	require.NoError(t, err)
	require.False(t, resp.GetStarted())
	require.Equal(t, first.GetRunId(), resp.GetRunId())
}

func TestStartNexusOperation_AttachAfterClosed(t *testing.T) {
	t.Run("AllowDuplicateStartsNewRun", func(t *testing.T) {
		h, ctx := newStartTestHandler(t)

		first, err := startWithAttach(ctx, h, "request-1",
			enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING)
		require.NoError(t, err)
		terminateOperation(ctx, t, h, first.GetRunId())

		resp, err := startWithAttach(ctx, h, "request-2",
			enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING)
		require.NoError(t, err)
		require.True(t, resp.GetStarted())

		// The new run holds only this request's callback and link.
		cbs, links := attachedCounts(ctx, t, resp.GetRunId())
		require.Equal(t, 1, cbs)
		require.Equal(t, 1, links)
	})

	t.Run("RejectDuplicateFails", func(t *testing.T) {
		h, ctx := newStartTestHandler(t)

		first, err := startWithAttach(ctx, h, "request-1",
			enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE,
			enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING)
		require.NoError(t, err)
		terminateOperation(ctx, t, h, first.GetRunId())

		_, err = startWithAttach(ctx, h, "request-2",
			enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE,
			enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING)
		var alreadyStarted *serviceerror.NexusOperationExecutionAlreadyStarted
		require.ErrorAs(t, err, &alreadyStarted)
		require.Equal(t, first.GetRunId(), alreadyStarted.RunId)
		cbs, links := attachedCounts(ctx, t, first.GetRunId())
		require.Equal(t, 1, cbs)
		require.Equal(t, 1, links)
	})
}
