package links_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/links"
	"google.golang.org/protobuf/proto"
)

func TestValidate(t *testing.T) {
	const maxLinks = 3
	const maxSize = 1024

	validWorkflowEvent := &commonpb.Link{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: "wf",
				RunId:      "run",
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   1,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
		},
	}
	validBatchJob := &commonpb.Link{
		Variant: &commonpb.Link_BatchJob_{
			BatchJob: &commonpb.Link_BatchJob{JobId: "job"},
		},
	}
	validNexusOperation := &commonpb.Link{
		Variant: &commonpb.Link_NexusOperation_{
			NexusOperation: &commonpb.Link_NexusOperation{
				Namespace:   "ns",
				OperationId: "op",
				RunId:       "run",
			},
		},
	}
	validActivity := &commonpb.Link{
		Variant: &commonpb.Link_Activity_{
			Activity: &commonpb.Link_Activity{
				Namespace:  "ns",
				ActivityId: "act",
				RunId:      "run",
			},
		},
	}
	validWorkflow := &commonpb.Link{
		Variant: &commonpb.Link_Workflow_{
			Workflow: &commonpb.Link_Workflow{
				Namespace:  "ns",
				WorkflowId: "wid",
				RunId:      "rid",
			},
		},
	}

	t.Run("HappyPath", func(t *testing.T) {
		err := links.Validate([]*commonpb.Link{
			validWorkflowEvent,
			validBatchJob,
			validNexusOperation,
			validActivity,
			validWorkflow,
		}, maxLinks+2, maxSize)
		require.NoError(t, err)
	})

	t.Run("ExceedsCountLimit", func(t *testing.T) {
		err := links.Validate([]*commonpb.Link{validWorkflowEvent, validBatchJob, validNexusOperation, validActivity}, maxLinks, maxSize)
		require.ErrorContains(t, err, "cannot attach more than 3 links per request")
	})

	t.Run("ExceedsSizeLimit", func(t *testing.T) {
		err := links.Validate([]*commonpb.Link{validWorkflowEvent}, maxLinks, 1)
		require.ErrorContains(t, err, "link exceeds allowed size")
	})

	t.Run("WorkflowEvent/EmptyNamespace", func(t *testing.T) {
		l := proto.Clone(validWorkflowEvent).(*commonpb.Link)
		l.GetWorkflowEvent().Namespace = ""
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "workflow event link must not have an empty namespace field")
	})

	t.Run("WorkflowEvent/EmptyWorkflowID", func(t *testing.T) {
		l := proto.Clone(validWorkflowEvent).(*commonpb.Link)
		l.GetWorkflowEvent().WorkflowId = ""
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "workflow event link must not have an empty workflow ID field")
	})

	t.Run("WorkflowEvent/EmptyRunID", func(t *testing.T) {
		l := proto.Clone(validWorkflowEvent).(*commonpb.Link)
		l.GetWorkflowEvent().RunId = ""
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "workflow event link must not have an empty run ID field")
	})

	t.Run("WorkflowEvent/UnspecifiedTypeWithNonZeroEventID", func(t *testing.T) {
		l := proto.Clone(validWorkflowEvent).(*commonpb.Link)
		l.GetWorkflowEvent().GetEventRef().EventType = enumspb.EVENT_TYPE_UNSPECIFIED
		l.GetWorkflowEvent().GetEventRef().EventId = 42
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "workflow event link ref cannot have an unspecified event type and a non-zero event ID")
	})

	t.Run("BatchJob/EmptyJobID", func(t *testing.T) {
		l := proto.Clone(validBatchJob).(*commonpb.Link)
		l.GetBatchJob().JobId = ""
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "batch job link must not have an empty job ID")
	})

	t.Run("NexusOperation/EmptyNamespace", func(t *testing.T) {
		l := proto.Clone(validNexusOperation).(*commonpb.Link)
		l.GetNexusOperation().Namespace = ""
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "nexus operation link must not have an empty namespace field")
	})

	t.Run("NexusOperation/EmptyOperationID", func(t *testing.T) {
		l := proto.Clone(validNexusOperation).(*commonpb.Link)
		l.GetNexusOperation().OperationId = ""
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "nexus operation link must not have an empty operation ID field")
	})

	t.Run("NexusOperation/EmptyRunID", func(t *testing.T) {
		l := proto.Clone(validNexusOperation).(*commonpb.Link)
		l.GetNexusOperation().RunId = ""
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "nexus operation link must not have an empty run ID field")
	})

	t.Run("Activity/EmptyNamespace", func(t *testing.T) {
		l := proto.Clone(validActivity).(*commonpb.Link)
		l.GetActivity().Namespace = ""
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "activity link must not have an empty namespace field")
	})

	t.Run("Activity/EmptyActivityID", func(t *testing.T) {
		l := proto.Clone(validActivity).(*commonpb.Link)
		l.GetActivity().ActivityId = ""
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "activity link must not have an empty activity ID field")
	})

	t.Run("Activity/EmptyRunID", func(t *testing.T) {
		l := proto.Clone(validActivity).(*commonpb.Link)
		l.GetActivity().RunId = ""
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "activity link must not have an empty run ID field")
	})

	t.Run("UnsupportedVariant", func(t *testing.T) {
		err := links.Validate([]*commonpb.Link{{}}, maxLinks, maxSize)
		require.ErrorContains(t, err, "unsupported link variant")
	})

	t.Run("Workflow/EmptyNamespace", func(t *testing.T) {
		l := proto.Clone(validWorkflow).(*commonpb.Link)
		l.GetWorkflow().Namespace = ""
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.EqualError(t, err, "workflow link must not have an empty namespace field")
	})

	t.Run("Workflow/EmptyWorkflowID", func(t *testing.T) {
		l := proto.Clone(validWorkflow).(*commonpb.Link)
		l.GetWorkflow().WorkflowId = ""
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.EqualError(t, err, "workflow link must not have an empty workflow ID field")
	})

	t.Run("Workflow/EmptyRunID", func(t *testing.T) {
		l := proto.Clone(validWorkflow).(*commonpb.Link)
		l.GetWorkflow().RunId = ""
		err := links.Validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.EqualError(t, err, "workflow link must not have an empty run ID field")
	})
}

func batchJobLink(jobID string) *commonpb.Link {
	return &commonpb.Link{
		Variant: &commonpb.Link_BatchJob_{
			BatchJob: &commonpb.Link_BatchJob{JobId: jobID},
		},
	}
}

func TestValidateWithCallbacks(t *testing.T) {
	const maxSize = 1024

	t.Run("HappyPath", func(t *testing.T) {
		err := links.ValidateWithCallbacks(
			[]*commonpb.Link{batchJobLink("request-job")},
			[]*commonpb.Callback{{Links: []*commonpb.Link{batchJobLink("callback-job")}}},
			2,
			maxSize,
		)
		require.NoError(t, err)
	})

	t.Run("CountsRequestAndCallbackLinksTogether", func(t *testing.T) {
		err := links.ValidateWithCallbacks(
			[]*commonpb.Link{batchJobLink("request-job")},
			[]*commonpb.Callback{{Links: []*commonpb.Link{batchJobLink("callback-job")}}},
			1,
			maxSize,
		)
		require.ErrorContains(t, err, "cannot attach more than 1 links per request, got 2")
	})

	t.Run("InvalidCallbackLink", func(t *testing.T) {
		err := links.ValidateWithCallbacks(
			nil,
			[]*commonpb.Callback{{Links: []*commonpb.Link{batchJobLink("")}}},
			2,
			maxSize,
		)
		require.ErrorContains(t, err, "batch job link must not have an empty job ID")
	})
}

func TestValidator(t *testing.T) {
	limit := func(n int) func(string) int {
		return func(string) int { return n }
	}

	t.Run("ValidateRequest", func(t *testing.T) {
		v := links.NewValidator("an activity", limit(1), limit(10), limit(1024))
		require.NoError(t, v.ValidateRequest("ns", []*commonpb.Link{batchJobLink("a")}))
		require.ErrorContains(t,
			v.ValidateRequest("ns", []*commonpb.Link{batchJobLink("a"), batchJobLink("b")}),
			"cannot attach more than 1 links per request, got 2")
	})

	t.Run("ValidateRequestWithCallbacks", func(t *testing.T) {
		v := links.NewValidator("an activity", limit(1), limit(10), limit(1024))
		err := v.ValidateRequestWithCallbacks(
			"ns",
			[]*commonpb.Link{batchJobLink("request-job")},
			[]*commonpb.Callback{{Links: []*commonpb.Link{batchJobLink("callback-job")}}},
		)
		require.ErrorContains(t, err, "cannot attach more than 1 links per request, got 2")
	})

	t.Run("ValidateTotal", func(t *testing.T) {
		v := links.NewValidator("an activity", limit(10), limit(3), limit(1024))
		require.NoError(t, v.ValidateTotal("ns", 1, 2))

		err := v.ValidateTotal("ns", 1, 3)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.EqualError(t, err, "cannot attach more than 3 links to an activity (1 links already attached)")
	})

	t.Run("LimitsAreNamespaceScoped", func(t *testing.T) {
		perNamespace := func(limits map[string]int) func(string) int {
			return func(ns string) int { return limits[ns] }
		}
		v := links.NewValidator(
			"an activity",
			perNamespace(map[string]int{"strict": 1, "loose": 5}),
			perNamespace(map[string]int{"strict": 1, "loose": 5}),
			limit(1024),
		)
		twoLinks := []*commonpb.Link{batchJobLink("a"), batchJobLink("b")}

		require.NoError(t, v.ValidateRequest("loose", twoLinks))
		require.ErrorContains(t, v.ValidateRequest("strict", twoLinks),
			"cannot attach more than 1 links per request, got 2")

		require.NoError(t, v.ValidateTotal("loose", 2, 2))
		require.ErrorContains(t, v.ValidateTotal("strict", 2, 2),
			"cannot attach more than 1 links to an activity")
	})
}
