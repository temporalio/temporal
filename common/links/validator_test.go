package links_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/links"
	test "go.temporal.io/server/common/testing"
	"google.golang.org/protobuf/proto"
)

func limit(n int) func(string) int {
	return func(string) int { return n }
}

func newValidator(t *testing.T, maxPerRequest, maxSize, maxPerExecution int) links.Validator {
	cfg := test.NewLinksValidatorConfig()
	cfg.MaxLinksPerRequest = limit(maxPerRequest)
	cfg.LinkMaxSize = limit(maxSize)
	cfg.MaxLinksPerExecution = limit(maxPerExecution)
	return test.NewLinksValidator(t, cfg)
}

func TestValidate(t *testing.T) {
	const maxLinks = 3
	const maxSize = 1024
	validate := func(ls []*commonpb.Link, maxLinks, maxSize int) error {
		return newValidator(t, maxLinks, maxSize, 2000).Validate("ns", ls, nil)
	}

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
	validWorkflowEventRequestIDRef := &commonpb.Link{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: "wf",
				RunId:      "run",
				Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
					RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
						RequestId: "request-id",
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
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
	validCallback := &commonpb.Link{
		Variant: &commonpb.Link_Callback_{
			Callback: &commonpb.Link_Callback{
				Namespace: "ns",
				Execution: &commonpb.Execution{
					Type:       enumspb.EXECUTION_TYPE_NEXUS_OPERATION,
					BusinessId: "op",
					RunId:      "run",
				},
				ComponentPath: []string{"Update", "wf-update-id"},
				RequestId:     "req",
			},
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
		err := validate([]*commonpb.Link{
			validWorkflowEvent,
			validBatchJob,
			validCallback,
			validNexusOperation,
			validActivity,
			validWorkflow,
		}, maxLinks+3, maxSize)
		require.NoError(t, err)
	})

	t.Run("ExceedsCountLimit", func(t *testing.T) {
		err := validate([]*commonpb.Link{validWorkflowEvent, validBatchJob, validNexusOperation, validActivity}, maxLinks, maxSize)
		require.ErrorContains(t, err, "cannot attach more than 3 links per request")
	})

	t.Run("ExceedsSizeLimit", func(t *testing.T) {
		err := validate([]*commonpb.Link{validWorkflowEvent}, maxLinks, 1)
		require.ErrorContains(t, err, "link exceeds allowed size")
	})

	t.Run("WorkflowEvent/EmptyNamespace", func(t *testing.T) {
		l := proto.Clone(validWorkflowEvent).(*commonpb.Link)
		l.GetWorkflowEvent().Namespace = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "workflow event link must not have an empty namespace field")
	})

	t.Run("WorkflowEvent/EmptyWorkflowID", func(t *testing.T) {
		l := proto.Clone(validWorkflowEvent).(*commonpb.Link)
		l.GetWorkflowEvent().WorkflowId = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "workflow event link must not have an empty workflow ID field")
	})

	t.Run("WorkflowEvent/EmptyRunID", func(t *testing.T) {
		l := proto.Clone(validWorkflowEvent).(*commonpb.Link)
		l.GetWorkflowEvent().RunId = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "workflow event link must not have an empty run ID field")
	})

	t.Run("WorkflowEvent/UnspecifiedTypeWithNonZeroEventID", func(t *testing.T) {
		l := proto.Clone(validWorkflowEvent).(*commonpb.Link)
		l.GetWorkflowEvent().GetEventRef().EventType = enumspb.EVENT_TYPE_UNSPECIFIED
		l.GetWorkflowEvent().GetEventRef().EventId = 42
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "workflow event link ref cannot have an unspecified event type and a non-zero event ID")
	})

	t.Run("WorkflowEvent/RequestIDRefEmptyRequestID", func(t *testing.T) {
		l := proto.Clone(validWorkflowEventRequestIDRef).(*commonpb.Link)
		l.GetWorkflowEvent().GetRequestIdRef().RequestId = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "workflow event request ID ref must not have an empty request ID")
	})

	t.Run("WorkflowEvent/RequestIDRefUnspecifiedEventType", func(t *testing.T) {
		l := proto.Clone(validWorkflowEventRequestIDRef).(*commonpb.Link)
		l.GetWorkflowEvent().GetRequestIdRef().EventType = enumspb.EVENT_TYPE_UNSPECIFIED
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "workflow event request ID ref must not have an unspecified event type")
	})

	t.Run("BatchJob/EmptyJobID", func(t *testing.T) {
		l := proto.Clone(validBatchJob).(*commonpb.Link)
		l.GetBatchJob().JobId = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "batch job link must not have an empty job ID")
	})

	t.Run("Callback/EmptyExecution", func(t *testing.T) {
		l := proto.Clone(validCallback).(*commonpb.Link)
		l.GetCallback().Execution = nil
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "callback link must have an execution")
	})

	t.Run("Callback/RequestID", func(t *testing.T) {
		l := proto.Clone(validCallback).(*commonpb.Link)
		l.GetCallback().RequestId = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "callback link must have a request ID")
	})

	t.Run("NexusOperation/EmptyNamespace", func(t *testing.T) {
		l := proto.Clone(validNexusOperation).(*commonpb.Link)
		l.GetNexusOperation().Namespace = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "nexus operation link must not have an empty namespace field")
	})

	t.Run("NexusOperation/EmptyOperationID", func(t *testing.T) {
		l := proto.Clone(validNexusOperation).(*commonpb.Link)
		l.GetNexusOperation().OperationId = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "nexus operation link must not have an empty operation ID field")
	})

	t.Run("NexusOperation/EmptyRunID", func(t *testing.T) {
		l := proto.Clone(validNexusOperation).(*commonpb.Link)
		l.GetNexusOperation().RunId = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "nexus operation link must not have an empty run ID field")
	})

	t.Run("Activity/EmptyNamespace", func(t *testing.T) {
		l := proto.Clone(validActivity).(*commonpb.Link)
		l.GetActivity().Namespace = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "activity link must not have an empty namespace field")
	})

	t.Run("Activity/EmptyActivityID", func(t *testing.T) {
		l := proto.Clone(validActivity).(*commonpb.Link)
		l.GetActivity().ActivityId = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "activity link must not have an empty activity ID field")
	})

	t.Run("Activity/EmptyRunID", func(t *testing.T) {
		l := proto.Clone(validActivity).(*commonpb.Link)
		l.GetActivity().RunId = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.ErrorContains(t, err, "activity link must not have an empty run ID field")
	})

	t.Run("UnsupportedVariant", func(t *testing.T) {
		err := validate([]*commonpb.Link{{}}, maxLinks, maxSize)
		require.ErrorContains(t, err, "unsupported link variant")
	})

	t.Run("Workflow/EmptyNamespace", func(t *testing.T) {
		l := proto.Clone(validWorkflow).(*commonpb.Link)
		l.GetWorkflow().Namespace = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.EqualError(t, err, "workflow link must not have an empty namespace field")
	})

	t.Run("Workflow/EmptyWorkflowID", func(t *testing.T) {
		l := proto.Clone(validWorkflow).(*commonpb.Link)
		l.GetWorkflow().WorkflowId = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
		require.EqualError(t, err, "workflow link must not have an empty workflow ID field")
	})

	t.Run("Workflow/EmptyRunID", func(t *testing.T) {
		l := proto.Clone(validWorkflow).(*commonpb.Link)
		l.GetWorkflow().RunId = ""
		err := validate([]*commonpb.Link{l}, maxLinks, maxSize)
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

func TestValidate_Callbacks(t *testing.T) {
	t.Run("HappyPath", func(t *testing.T) {
		err := newValidator(t, 2, 1024, 2000).Validate(
			"ns",
			[]*commonpb.Link{batchJobLink("request-job")},
			[]*commonpb.Callback{{Links: []*commonpb.Link{batchJobLink("callback-job")}}},
		)
		require.NoError(t, err)
	})

	t.Run("CountsRequestAndCallbackLinksTogether", func(t *testing.T) {
		err := newValidator(t, 1, 1024, 2000).Validate(
			"ns",
			[]*commonpb.Link{batchJobLink("request-job")},
			[]*commonpb.Callback{{Links: []*commonpb.Link{batchJobLink("callback-job")}}},
		)
		require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		require.ErrorContains(t, err, "cannot attach more than 1 links per request, got 2")
	})

	t.Run("InvalidCallbackLink", func(t *testing.T) {
		err := newValidator(t, 2, 1024, 2000).Validate(
			"ns",
			nil,
			[]*commonpb.Callback{{Links: []*commonpb.Link{batchJobLink("")}}},
		)
		require.ErrorContains(t, err, "batch job link must not have an empty job ID")
	})

	t.Run("OversizedCallbackLink", func(t *testing.T) {
		err := newValidator(t, 2, 1, 2000).Validate(
			"ns",
			nil,
			[]*commonpb.Callback{{Links: []*commonpb.Link{batchJobLink("callback-job")}}},
		)
		require.ErrorContains(t, err, "link exceeds allowed size")
	})
}

func TestValidateAdditions(t *testing.T) {
	t.Run("WithinLimit", func(t *testing.T) {
		v := newValidator(t, 10, 1024, 3)
		existing := links.UsageOf([]*commonpb.Link{batchJobLink("a")})
		require.NoError(t, v.ValidateAdditions("ns", []*commonpb.Link{batchJobLink("b"), batchJobLink("c")}, existing))
	})

	t.Run("ExceedsCountLimit", func(t *testing.T) {
		v := newValidator(t, 10, 1024, 3)
		err := v.ValidateAdditions(
			"ns",
			[]*commonpb.Link{batchJobLink("b"), batchJobLink("c"), batchJobLink("d")},
			links.Usage{Count: 1},
		)
		require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
		require.EqualError(t, err, "cannot attach more than 3 links to an execution (1 links already attached)")
	})

	t.Run("NoAdditionsAlwaysPasses", func(t *testing.T) {
		// An execution already over a since-lowered limit must not fail a request that adds nothing.
		v := newValidator(t, 10, 1024, 1)
		require.NoError(t, v.ValidateAdditions("ns", nil, links.Usage{Count: 5}))
	})
}

func TestValidatorConfigValidate(t *testing.T) {
	t.Run("MissingFields", func(t *testing.T) {
		cfg := test.NewLinksValidatorConfig()
		cfg.MaxLinksPerRequest = nil
		cfg.MaxLinksPerExecution = nil

		_, err := links.NewValidator(cfg)
		require.EqualError(t, err, "missing required fields: [MaxLinksPerRequest MaxLinksPerExecution]")
	})

	// Verify that all fields will be reported if unset. (And adding a new field
	// will not silently be considered valid.)
	t.Run("MissingAllFields", func(t *testing.T) {
		var fields []string
		for field := range reflect.TypeFor[links.ValidatorConfig]().Fields() {
			fields = append(fields, field.Name)
		}

		_, err := links.NewValidator(links.ValidatorConfig{})
		require.EqualError(t, err, fmt.Sprintf("missing required fields: %v", fields))
	})
}

func TestValidator_LimitsAreNamespaceScoped(t *testing.T) {
	perNamespace := func(limits map[string]int) func(string) int {
		return func(ns string) int { return limits[ns] }
	}
	cfg := test.NewLinksValidatorConfig()
	cfg.MaxLinksPerRequest = perNamespace(map[string]int{"strict": 1, "loose": 5})
	cfg.MaxLinksPerExecution = perNamespace(map[string]int{"strict": 1, "loose": 5})
	v := test.NewLinksValidator(t, cfg)
	twoLinks := []*commonpb.Link{batchJobLink("a"), batchJobLink("b")}

	require.NoError(t, v.Validate("loose", twoLinks, nil))
	require.ErrorContains(t, v.Validate("strict", twoLinks, nil),
		"cannot attach more than 1 links per request, got 2")

	require.NoError(t, v.ValidateAdditions("loose", twoLinks, links.Usage{Count: 2}))
	require.ErrorContains(t, v.ValidateAdditions("strict", twoLinks, links.Usage{Count: 2}),
		"cannot attach more than 1 links to an execution")
}
