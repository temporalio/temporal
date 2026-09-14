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
// Workflow, WorkflowEvent, and Activity link variants, drops unsupported types,
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
	nexusLinks := []nexus.Link{workflowEvent, activity, workflow, unsupported, malformedActivity}
	nexusLinks = append(nexusLinks, malformedWorkflows...)

	out := commonnexus.ConvertNexusLinksToProtoLinks(nexusLinks, logger)
	require.Len(t, out, 3, "workflow, workflow-event, and activity links must round-trip; unsupported and malformed entries must be dropped")

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
	}
	protorequire.ProtoSliceEqual(t, expected, out)
}

// A Callback link is one of the variants a Nexus handler can hand back, so the batch converter has
// to recognize it.
func TestConvertNexusLinksToProtoLinks_CallbackAndNilURL(t *testing.T) {
	logger := log.NewTestLogger()

	callbackLink := nexus.Link{
		URL: &url.URL{
			Scheme:   "temporal",
			Path:     "/namespaces/ns/nexus-operations/op-id/run-id/callbacks/request-id",
			RawQuery: "componentPath=c-path1&componentPath=c-path2",
		},
		Type: "temporal.api.common.v1.Link.Callback",
	}
	malformedCallback := nexus.Link{
		URL:  &url.URL{Scheme: "temporal", Path: "/namespaces/ns/nexus-operations/op-id/run-id/callbacks"},
		Type: "temporal.api.common.v1.Link.Callback",
	}
	noURL := nexus.Link{Type: "temporal.api.common.v1.Link.Callback"}

	out := commonnexus.ConvertNexusLinksToProtoLinks(
		[]nexus.Link{callbackLink, malformedCallback, noURL},
		logger,
	)

	// The malformed and missing-URL links are dropped with a warning.
	protorequire.ProtoSliceEqual(t, []*commonpb.Link{
		{
			Variant: &commonpb.Link_Callback_{
				Callback: &commonpb.Link_Callback{
					Namespace: "ns",
					Execution: &commonpb.Execution{
						Type:       enumspb.EXECUTION_TYPE_NEXUS_OPERATION,
						BusinessId: "op-id",
						RunId:      "run-id",
					},
					ComponentPath: []string{"c-path1", "c-path2"},
					RequestId:     "request-id",
				},
			},
		},
	}, out)
}
