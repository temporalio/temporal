package nexusoperation

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
	unsupported := nexus.Link{
		URL:  &url.URL{Scheme: "temporal", Path: "/foo"},
		Type: "unknown.Type",
	}
	malformedActivity := nexus.Link{
		URL:  &url.URL{Scheme: "temporal", Path: "/namespaces/ns/foo/act-id"},
		Type: "temporal.api.common.v1.Link.Activity",
	}

	out := commonnexus.ConvertNexusLinksToProtoLinks([]nexus.Link{workflowEvent, activity, unsupported, malformedActivity}, logger)
	require.Len(t, out, 2)

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
	}
	protorequire.ProtoSliceEqual(t, expected, out)
}
