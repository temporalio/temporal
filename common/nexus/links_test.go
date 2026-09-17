package nexus_test

import (
	"net/url"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/testing/protorequire"
)

// TestConvertNexusLinksToProtoLinks verifies that the converter handles the
// Workflow, WorkflowEvent, Activity, and NexusOperation link variants, drops unsupported types,
// and skips malformed entries.
func TestConvertNexusLinksToProtoLinks(t *testing.T) {
	logger := log.NewTestLogger()

	workflowEvent := nexus.Link{
		URL: &url.URL{
			Scheme:   "temporal",
			Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
			RawQuery: "eventID=1&eventType=WorkflowExecutionStarted&referenceType=EventReference",
		},
		Type: "temporal.api.common.v1.Link.WorkflowEvent",
	}
	activity := nexus.Link{
		URL: &url.URL{
			Scheme: "temporal",
			Path:   "/namespaces/ns/activities/act-id/run-id/details",
		},
		Type: "temporal.api.common.v1.Link.Activity",
	}
	workflow := nexus.Link{
		URL: &url.URL{
			Scheme: "temporal",
			Path:   "/namespaces/ns/workflows/wf-id/run-id",
		},
		Type: "temporal.api.common.v1.Link.Workflow",
	}
	nexusOperation := nexus.Link{
		URL: &url.URL{
			Scheme: "temporal",
			Path:   "/namespaces/ns/nexus-operations/op-id/run-id/details",
		},
		Type: "temporal.api.common.v1.Link.NexusOperation",
	}
	unsupported := nexus.Link{
		URL:  &url.URL{Scheme: "temporal", Path: "/foo"},
		Type: "unknown.Type",
	}
	malformedActivity := nexus.Link{
		URL:  &url.URL{Scheme: "temporal", Path: "/namespaces/ns/foo/act-id"},
		Type: "temporal.api.common.v1.Link.Activity",
	}
	malformedWorkflows := []nexus.Link{
		{
			URL:  &url.URL{Scheme: "temporal", Path: "/namespaces//workflows/wid/rid"}, // missing ns
			Type: "temporal.api.common.v1.Link.Workflow",
		},
		{
			URL:  &url.URL{Scheme: "temporal", Path: "/namespaces/ns/workflows//rid"}, // missing wid
			Type: "temporal.api.common.v1.Link.Workflow",
		},
		{
			URL:  &url.URL{Scheme: "temporal", Path: "/namespaces/ns/workflows/wid/"}, // missing rid
			Type: "temporal.api.common.v1.Link.Workflow",
		},
		{
			URL:  &url.URL{Scheme: "temporal", Path: "/foo"}, // incorrect path
			Type: "temporal.api.common.v1.Link.Workflow",
		},
	}
	nexusLinks := []nexus.Link{workflowEvent, activity, workflow, nexusOperation, unsupported, malformedActivity}
	nexusLinks = append(nexusLinks, malformedWorkflows...)

	out := commonnexus.ConvertNexusLinksToProtoLinks(nexusLinks, logger)
	require.Len(t, out, 4, "workflow, workflow-event, activity, and Nexus-operation links must round-trip; unsupported and malformed entries must be dropped")

	expected := []*commonpb.Link{
		{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  "ns",
					WorkflowId: "wf-id",
					RunId:      "run-id",
					Reference: &commonpb.Link_WorkflowEvent_EventRef{
						EventRef: &commonpb.Link_WorkflowEvent_EventReference{
							EventId:   1,
							EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
						},
					},
				},
			},
		},
		{
			Variant: &commonpb.Link_Activity_{
				Activity: &commonpb.Link_Activity{
					Namespace:  "ns",
					ActivityId: "act-id",
					RunId:      "run-id",
				},
			},
		},
		{
			Variant: &commonpb.Link_Workflow_{
				Workflow: &commonpb.Link_Workflow{
					Namespace:  "ns",
					WorkflowId: "wf-id",
					RunId:      "run-id",
				},
			},
		},
		{
			Variant: &commonpb.Link_NexusOperation_{
				NexusOperation: &commonpb.Link_NexusOperation{
					Namespace:   "ns",
					OperationId: "op-id",
					RunId:       "run-id",
				},
			},
		},
	}
	protorequire.ProtoSliceEqual(t, expected, out)
}
