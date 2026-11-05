package nexusoperation

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	test "go.temporal.io/server/common/testing"
	"go.temporal.io/server/common/testing/protorequire"
)

// newLinkTestContext returns a mock context backed by stored, standing in for the framework's
// per-request link storage: writes land in ctx.LinksByRequest, while reads (Links/RequestLinks) are
// served from stored. Tests copy a write into stored to simulate it having been persisted.
func newLinkTestContext(stored map[string][]*commonpb.Link) *chasm.MockMutableContext {
	ctx := newCallbackTestContext()
	ctx.HandleLinks = func(chasm.Component) []*commonpb.Link {
		var all []*commonpb.Link
		for _, links := range stored {
			all = append(all, links...)
		}
		return all
	}
	ctx.HandleRequestLinks = func(_ chasm.Component, requestID string) ([]*commonpb.Link, error) {
		return stored[requestID], nil
	}
	return ctx
}

func testLink(workflowID string) *commonpb.Link {
	return &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{
			Namespace:  "ns-name",
			WorkflowId: workflowID,
			RunId:      "wf-run-id",
		},
	}}
}

func TestNewStandaloneOperationAttachesLinks(t *testing.T) {
	t.Parallel()
	cbValidator := test.NewCallbacksValidator(t, test.NewCallbacksValidatorConfig())

	newStartReq := func(links ...*commonpb.Link) *nexusoperationpb.StartNexusOperationRequest {
		return &nexusoperationpb.StartNexusOperationRequest{
			EndpointId: "endpoint-id",
			FrontendRequest: &workflowservice.StartNexusOperationExecutionRequest{
				Namespace:   "ns-name",
				OperationId: "op-id",
				RequestId:   "req-id",
				Endpoint:    "test-endpoint",
				Service:     "test-service",
				Operation:   "test-operation",
				Links:       links,
			},
		}
	}

	t.Run("WithLinks", func(t *testing.T) {
		ctx := newLinkTestContext(map[string][]*commonpb.Link{})
		link := testLink("wf-id")

		op, err := newStandaloneOperation(ctx, newStartReq(link), cbValidator, test.NewLinksValidator(t, test.NewLinksValidatorConfig()))
		require.NoError(t, err)
		require.Equal(t, nexusoperationpb.OPERATION_STATUS_SCHEDULED, op.Status)

		// Links are keyed by the request that contributed them.
		protorequire.ProtoSliceEqual(t, []*commonpb.Link{link}, ctx.LinksByRequest[op]["req-id"])
	})

	t.Run("WithoutLinks", func(t *testing.T) {
		ctx := newLinkTestContext(map[string][]*commonpb.Link{})

		op, err := newStandaloneOperation(ctx, newStartReq(), cbValidator, test.NewLinksValidator(t, test.NewLinksValidatorConfig()))
		require.NoError(t, err)
		require.Empty(t, ctx.LinksByRequest[op])
	})

	t.Run("EnforcesThePerComponentCap", func(t *testing.T) {
		ctx := newLinkTestContext(map[string][]*commonpb.Link{})
		cfg := test.NewLinksValidatorConfig()
		cfg.MaxLinksPerExecution = func(string) int { return 1 }

		_, err := newStandaloneOperation(
			ctx,
			newStartReq(testLink("wf-1"), testLink("wf-2")),
			cbValidator,
			test.NewLinksValidator(t, cfg),
		)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.ErrorContains(t, err, "cannot attach more than 1 links to an execution")
	})

	t.Run("EnforcesTheTotalSizeCap", func(t *testing.T) {
		// Each link fits on its own; together they are one byte over the cap.
		linkA, linkB := testLink("wf-1"), testLink("wf-2")
		ctx := newLinkTestContext(map[string][]*commonpb.Link{})
		cfg := test.NewLinksValidatorConfig()
		cfg.TotalLinksMaxSize = func(string) int { return linkA.Size() + linkB.Size() - 1 }

		_, err := newStandaloneOperation(
			ctx,
			newStartReq(linkA, linkB),
			cbValidator,
			test.NewLinksValidator(t, cfg),
		)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.ErrorContains(t, err, "bytes of links to an execution")
	})
}

func TestOperationAttachLinks(t *testing.T) {
	t.Parallel()

	t.Run("EmptyListIsANoOp", func(t *testing.T) {
		stored := map[string][]*commonpb.Link{}
		ctx := newLinkTestContext(stored)
		op := newScheduledTestOperation(t, ctx)

		require.NoError(t, op.attachLinks(ctx, nil, "req-id", test.NewLinksValidator(t, test.NewLinksValidatorConfig()), "ns-name"))
		require.Empty(t, ctx.LinksByRequest[op])
	})

	t.Run("SameRequestIDIsNoOp", func(t *testing.T) {
		// A retried start (or a retried on_conflict_options attach) must not duplicate links.
		stored := map[string][]*commonpb.Link{}
		ctx := newLinkTestContext(stored)
		op := newScheduledTestOperation(t, ctx)
		validator := test.NewLinksValidator(t, test.NewLinksValidatorConfig())
		linkA, linkB := testLink("wf-a"), testLink("wf-b")

		// The first call records the request's links verbatim, without intra-batch dedup, matching
		// the workflow and standalone activity start paths.
		require.NoError(t, op.attachLinks(ctx, []*commonpb.Link{linkA, linkA}, "req-1", validator, "ns-name"))
		stored["req-1"] = ctx.LinksByRequest[op]["req-1"]
		protorequire.ProtoSliceEqual(t, []*commonpb.Link{linkA, linkA}, stored["req-1"])

		// A retry under the same requestID is a no-op, even with different links.
		require.NoError(t, op.attachLinks(ctx, []*commonpb.Link{linkB}, "req-1", validator, "ns-name"))
		protorequire.ProtoSliceEqual(t, []*commonpb.Link{linkA, linkA}, ctx.LinksByRequest[op]["req-1"])
	})

	t.Run("DistinctRequestsAccumulate", func(t *testing.T) {
		stored := map[string][]*commonpb.Link{}
		ctx := newLinkTestContext(stored)
		op := newScheduledTestOperation(t, ctx)
		validator := test.NewLinksValidator(t, test.NewLinksValidatorConfig())

		require.NoError(t, op.attachLinks(ctx, []*commonpb.Link{testLink("wf-1")}, "req-1", validator, "ns-name"))
		stored["req-1"] = ctx.LinksByRequest[op]["req-1"]
		require.NoError(t, op.attachLinks(ctx, []*commonpb.Link{testLink("wf-2")}, "req-2", validator, "ns-name"))

		require.Len(t, ctx.LinksByRequest[op], 2)
	})

	t.Run("RejectsAClosedOperation", func(t *testing.T) {
		ctx := newLinkTestContext(map[string][]*commonpb.Link{})
		op := newScheduledTestOperation(t, ctx)
		require.NoError(t, TransitionSucceeded.Apply(op, ctx, EventSucceeded{}))

		err := op.attachLinks(ctx, []*commonpb.Link{testLink("wf-id")}, "req-new", test.NewLinksValidator(t, test.NewLinksValidatorConfig()), "ns-name")
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.ErrorContains(t, err, "cannot attach links to a closed nexus operation")
	})

	t.Run("IsIdempotentAfterClose", func(t *testing.T) {
		// The original attach succeeded but the response was lost; by the time the client retries the
		// operation has closed. The retry must still report success for links already persisted.
		link := testLink("wf-id")
		ctx := newLinkTestContext(map[string][]*commonpb.Link{"req-1": {link}})
		op := newScheduledTestOperation(t, ctx)
		require.NoError(t, TransitionSucceeded.Apply(op, ctx, EventSucceeded{}))

		require.NoError(t, op.attachLinks(ctx, []*commonpb.Link{link}, "req-1", test.NewLinksValidator(t, test.NewLinksValidatorConfig()), "ns-name"))
	})

	t.Run("OnlyEnforcesAggregateLimits", func(t *testing.T) {
		// Per-request checks (count, size, shape) are the frontend's job; attachLinks only enforces
		// the per-execution aggregates, mirroring callback.ValidateAndAttach.
		ctx := newLinkTestContext(map[string][]*commonpb.Link{})
		op := newScheduledTestOperation(t, ctx)
		cfg := test.NewLinksValidatorConfig()
		cfg.MaxLinksPerRequest = func(string) int { return 1 }

		err := op.attachLinks(
			ctx,
			[]*commonpb.Link{testLink("wf-1"), testLink("wf-2")},
			"req-id",
			test.NewLinksValidator(t, cfg),
			"ns-name",
		)
		require.NoError(t, err)
		require.Len(t, ctx.LinksByRequest[op]["req-id"], 2)
	})

	t.Run("RejectsExceedingThePerComponentCapWithAlreadyAttachedLinks", func(t *testing.T) {
		ctx := newLinkTestContext(map[string][]*commonpb.Link{"req-1": {testLink("wf-existing")}})
		op := newScheduledTestOperation(t, ctx)
		cfg := test.NewLinksValidatorConfig()
		cfg.MaxLinksPerExecution = func(string) int { return 1 }

		err := op.attachLinks(
			ctx,
			[]*commonpb.Link{testLink("wf-new")},
			"req-2",
			test.NewLinksValidator(t, cfg),
			"ns-name",
		)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.ErrorContains(t, err, "1 links already attached")
	})

	t.Run("RejectsExceedingTheTotalSizeCapWithAlreadyAttachedLinks", func(t *testing.T) {
		existing := testLink("wf-existing")
		ctx := newLinkTestContext(map[string][]*commonpb.Link{"req-1": {existing}})
		op := newScheduledTestOperation(t, ctx)
		cfg := test.NewLinksValidatorConfig()
		// Room for the existing link plus one byte: any new link must be rejected.
		cfg.TotalLinksMaxSize = func(string) int { return existing.Size() + 1 }

		err := op.attachLinks(
			ctx,
			[]*commonpb.Link{testLink("wf-new")},
			"req-2",
			test.NewLinksValidator(t, cfg),
			"ns-name",
		)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.ErrorContains(t, err, "bytes of links to an execution")
		require.Empty(t, ctx.LinksByRequest[op])
	})
}

// TestDescribeResponseIncludesLinks covers the plumbing from both link sources into the
// DescribeNexusOperationExecution response.
func TestDescribeResponseIncludesLinks(t *testing.T) {
	t.Parallel()

	req := &nexusoperationpb.DescribeNexusOperationRequest{
		FrontendRequest: &workflowservice.DescribeNexusOperationExecutionRequest{},
	}
	newOp := func(ctx chasm.MutableContext) *Operation {
		op := newTestOperation()
		op.RequestData = chasm.NewDataField(ctx, &nexusoperationpb.OperationRequestData{})
		op.Visibility = chasm.NewComponentField(ctx, chasm.NewVisibility(ctx))
		return op
	}

	t.Run("UnionsCallerAndHandlerLinks", func(t *testing.T) {
		callerLink, handlerLink := testLink("caller-wf"), testLink("handler-wf")
		ctx := newLinkTestContext(map[string][]*commonpb.Link{"req-id": {callerLink}})
		op := newOp(ctx)
		// Links returned by the Nexus handler on its start/completion response.
		op.Links = []*commonpb.Link{handlerLink}

		resp, err := op.buildDescribeResponse(ctx, req)
		require.NoError(t, err)
		protorequire.ProtoElementsMatch(t,
			[]*commonpb.Link{callerLink, handlerLink},
			resp.GetFrontendResponse().GetInfo().GetLinks())
	})

	t.Run("CallerLinksOnly", func(t *testing.T) {
		callerLink := testLink("caller-wf")
		ctx := newLinkTestContext(map[string][]*commonpb.Link{"req-id": {callerLink}})

		resp, err := newOp(ctx).buildDescribeResponse(ctx, req)
		require.NoError(t, err)
		protorequire.ProtoSliceEqual(t,
			[]*commonpb.Link{callerLink},
			resp.GetFrontendResponse().GetInfo().GetLinks())
	})

	t.Run("WithoutLinks", func(t *testing.T) {
		ctx := newLinkTestContext(map[string][]*commonpb.Link{})

		resp, err := newOp(ctx).buildDescribeResponse(ctx, req)
		require.NoError(t, err)
		require.Empty(t, resp.GetFrontendResponse().GetInfo().GetLinks())
	})
}
