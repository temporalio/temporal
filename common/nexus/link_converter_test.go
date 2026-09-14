package nexus_test

import (
	"net/url"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestConvertLinkNexusOperationToNexusLink(t *testing.T) {
	input := &commonpb.Link_NexusOperation{
		Namespace:   "ns",
		OperationId: "op-id",
		RunId:       "run-id",
	}

	output := commonnexus.ConvertLinkNexusOperationToNexusLink(input)
	require.Equal(t, nexus.Link{
		URL: &url.URL{
			Scheme:  "temporal",
			Path:    "/namespaces/ns/nexus-operations/op-id/run-id/details",
			RawPath: "/namespaces/ns/nexus-operations/op-id/run-id/details",
		},
		Type: "temporal.api.common.v1.Link.NexusOperation",
	}, output)
	require.Equal(t, "temporal:///namespaces/ns/nexus-operations/op-id/run-id/details", output.URL.String())
}

func TestConvertLinkActivityToNexusLink(t *testing.T) {
	input := &commonpb.Link_Activity{
		Namespace:  "ns",
		ActivityId: "act-id",
		RunId:      "run-id",
	}

	output := commonnexus.ConvertLinkActivityToNexusLink(input)
	require.Equal(t, nexus.Link{
		URL: &url.URL{
			Scheme:  "temporal",
			Path:    "/namespaces/ns/activities/act-id/run-id/details",
			RawPath: "/namespaces/ns/activities/act-id/run-id/details",
		},
		Type: "temporal.api.common.v1.Link.Activity",
	}, output)
	require.Equal(t, "temporal:///namespaces/ns/activities/act-id/run-id/details", output.URL.String())
}

func TestConvertNexusLinkToLinkActivity(t *testing.T) {
	type testcase struct {
		name     string
		input    nexus.Link
		expected *commonpb.Link_Activity
		errMsg   string
	}

	cases := []testcase{
		{
			name: "valid",
			input: nexus.Link{
				URL: &url.URL{
					Scheme: "temporal",
					Path:   "/namespaces/ns/activities/act-id/run-id/details",
				},
				Type: "temporal.api.common.v1.Link.Activity",
			},
			expected: &commonpb.Link_Activity{
				Namespace:  "ns",
				ActivityId: "act-id",
				RunId:      "run-id",
			},
		},
		{
			name: "round-trip with escaped path",
			input: commonnexus.ConvertLinkActivityToNexusLink(&commonpb.Link_Activity{
				Namespace:  "ns/with/slash",
				ActivityId: "act id with space",
				RunId:      "run-id",
			}),
			expected: &commonpb.Link_Activity{
				Namespace:  "ns/with/slash",
				ActivityId: "act id with space",
				RunId:      "run-id",
			},
		},
		{
			name: "wrong type",
			input: nexus.Link{
				URL:  &url.URL{Scheme: "temporal", Path: "/namespaces/ns/activities/act-id"},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			errMsg: "cannot parse link type",
		},
		{
			name: "invalid scheme",
			input: nexus.Link{
				URL:  &url.URL{Scheme: "http", Path: "/namespaces/ns/activities/act-id"},
				Type: "temporal.api.common.v1.Link.Activity",
			},
			errMsg: "invalid scheme",
		},
		{
			name: "malformed path",
			input: nexus.Link{
				URL:  &url.URL{Scheme: "temporal", Path: "/namespaces/ns/foo/act-id"},
				Type: "temporal.api.common.v1.Link.Activity",
			},
			errMsg: "malformed URL path",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := commonnexus.ConvertNexusLinkToLinkActivity(tc.input)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
				return
			}
			require.NoError(t, err)
			protorequire.ProtoEqual(t, tc.expected, out)
		})
	}
}

func TestConvertLinkWorkflowEventToNexusLink(t *testing.T) {
	type testcase struct {
		name      string
		input     *commonpb.Link_WorkflowEvent
		output    nexus.Link
		outputURL string
	}

	cases := []testcase{
		{
			name: "valid",
			input: &commonpb.Link_WorkflowEvent{
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
			output: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id/run-id/history",
					RawQuery: "eventID=1&eventType=WorkflowExecutionStarted&referenceType=EventReference",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			outputURL: "temporal:///namespaces/ns/workflows/wf-id/run-id/history?eventID=1&eventType=WorkflowExecutionStarted&referenceType=EventReference",
		},
		{
			name: "valid with angle bracket",
			input: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: "wf-id>",
				RunId:      "run-id",
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   1,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
			output: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id>/run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id%3E/run-id/history",
					RawQuery: "eventID=1&eventType=WorkflowExecutionStarted&referenceType=EventReference",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			outputURL: "temporal:///namespaces/ns/workflows/wf-id%3E/run-id/history?eventID=1&eventType=WorkflowExecutionStarted&referenceType=EventReference",
		},
		{
			name: "valid with slash",
			input: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: "wf-id/",
				RunId:      "run-id",
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   1,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
			output: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id//run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id%2F/run-id/history",
					RawQuery: "eventID=1&eventType=WorkflowExecutionStarted&referenceType=EventReference",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			outputURL: "temporal:///namespaces/ns/workflows/wf-id%2F/run-id/history?eventID=1&eventType=WorkflowExecutionStarted&referenceType=EventReference",
		},
		{
			name: "valid event id missing",
			input: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: "wf-id",
				RunId:      "run-id",
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
			output: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id/run-id/history",
					RawQuery: "eventType=WorkflowExecutionStarted&referenceType=EventReference",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			outputURL: "temporal:///namespaces/ns/workflows/wf-id/run-id/history?eventType=WorkflowExecutionStarted&referenceType=EventReference",
		},
		{
			name: "valid request id",
			input: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: "wf-id",
				RunId:      "run-id",
				Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
					RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
						RequestId: "request-id",
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
					},
				},
			},
			output: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id/run-id/history",
					RawQuery: "eventType=WorkflowExecutionOptionsUpdated&referenceType=RequestIdReference&requestID=request-id",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			outputURL: "temporal:///namespaces/ns/workflows/wf-id/run-id/history?eventType=WorkflowExecutionOptionsUpdated&referenceType=RequestIdReference&requestID=request-id",
		},
		{
			name: "valid request id empty",
			input: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: "wf-id",
				RunId:      "run-id",
				Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
					RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
						RequestId: "",
					},
				},
			},
			output: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id/run-id/history",
					RawQuery: "eventType=Unspecified&referenceType=RequestIdReference&requestID=",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			outputURL: "temporal:///namespaces/ns/workflows/wf-id/run-id/history?eventType=Unspecified&referenceType=RequestIdReference&requestID=",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output := commonnexus.ConvertLinkWorkflowEventToNexusLink(tc.input)
			require.Equal(t, tc.output, output)
			require.Equal(t, tc.outputURL, output.URL.String())
		})
	}
}

func TestConvertNexusLinkToLinkWorkflowEvent(t *testing.T) {
	type testcase struct {
		name   string
		input  nexus.Link
		output *commonpb.Link_WorkflowEvent
		errMsg string
	}

	cases := []testcase{
		{
			name: "valid long event type",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
					RawQuery: "referenceType=EventReference&eventID=1&eventType=EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			output: &commonpb.Link_WorkflowEvent{
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
		{
			name: "valid short event type",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
					RawQuery: "referenceType=EventReference&eventID=1&eventType=WorkflowExecutionStarted",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			output: &commonpb.Link_WorkflowEvent{
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
		{
			name: "valid with angle bracket",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id>/run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id%2E/run-id/history",
					RawQuery: "referenceType=EventReference&eventID=1&eventType=EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			output: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: "wf-id>",
				RunId:      "run-id",
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   1,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
		},
		{
			name: "valid with slash",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id//run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id%2F/run-id/history",
					RawQuery: "referenceType=EventReference&eventID=1&eventType=EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			output: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: "wf-id/",
				RunId:      "run-id",
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   1,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
		},
		{
			name: "valid event id missing",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id/run-id/history",
					RawQuery: "referenceType=EventReference&eventID=&eventType=EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			output: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: "wf-id",
				RunId:      "run-id",
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
		},
		{
			name: "invalid scheme",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "random",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id/run-id/history",
					RawQuery: "referenceType=EventReference&eventID=1&eventType=EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			errMsg: "failed to parse link to Link_WorkflowEvent",
		},
		{
			name: "invalid path missing history",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/",
					RawPath:  "/namespaces/ns/workflows/wf-id/run-id/",
					RawQuery: "referenceType=EventReference&eventID=1&eventType=EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			errMsg: "failed to parse link to Link_WorkflowEvent",
		},
		{
			name: "invalid path missing namespace",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces//workflows/wf-id/run-id/history",
					RawPath:  "/namespaces//workflows/wf-id/run-id/history",
					RawQuery: "referenceType=EventReference&eventID=1&eventType=EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			errMsg: "failed to parse link to Link_WorkflowEvent",
		},
		{
			name: "invalid event type",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id/run-id/history",
					RawQuery: "referenceType=EventReference&eventID=1&eventType=EVENT_TYPE_INVALID",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			errMsg: "failed to parse link to Link_WorkflowEvent",
		},
		{
			name: "valid request id long event type",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id/run-id/history",
					RawQuery: "referenceType=RequestIdReference&requestID=request-id&eventType=EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			output: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: "wf-id",
				RunId:      "run-id",
				Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
					RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
						RequestId: "request-id",
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
					},
				},
			},
		},
		{
			name: "valid request id short event type",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id/run-id/history",
					RawQuery: "referenceType=RequestIdReference&requestID=request-id&eventType=WorkflowExecutionOptionsUpdated",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			output: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: "wf-id",
				RunId:      "run-id",
				Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
					RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
						RequestId: "request-id",
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
					},
				},
			},
		},
		{
			name: "valid request id empty",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id/run-id/history",
					RawQuery: "referenceType=RequestIdReference&requestID=&eventType=EVENT_TYPE_UNSPECIFIED",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			output: &commonpb.Link_WorkflowEvent{
				Namespace:  "ns",
				WorkflowId: "wf-id",
				RunId:      "run-id",
				Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
					RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
						RequestId: "",
						EventType: enumspb.EVENT_TYPE_UNSPECIFIED,
					},
				},
			},
		},
		{
			name: "invalid request id reference missing event type",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id/run-id/history",
					RawPath:  "/namespaces/ns/workflows/wf-id/run-id/history",
					RawQuery: "referenceType=RequestIdReference&requestID=",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
			errMsg: "failed to parse link to Link_WorkflowEvent",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(tc.input)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
			} else {
				require.NoError(t, err)
				if diff := cmp.Diff(tc.output, output, protocmp.Transform()); diff != "" {
					assert.Fail(t, "Proto mismatch (-want +got):\n", diff)
				}
			}
		})
	}
}

// Every execution type a callback can be attached to must survive the round trip through a URL,
// since the path segment naming the execution is the only thing that carries it.
func TestConvertLinkCallback_RoundTripsEveryExecutionType(t *testing.T) {
	for _, tc := range []struct {
		executionType enumspb.ExecutionType
		wantURL       string
	}{
		{
			executionType: enumspb.EXECUTION_TYPE_WORKFLOW,
			wantURL:       "temporal:///namespaces/ns/workflows/wf-id/run-id/callbacks/request-id",
		},
		{
			executionType: enumspb.EXECUTION_TYPE_ACTIVITY,
			wantURL:       "temporal:///namespaces/ns/activities/act-id/run-id/callbacks/request-id",
		},
		{
			executionType: enumspb.EXECUTION_TYPE_NEXUS_OPERATION,
			wantURL:       "temporal:///namespaces/ns/nexus-operations/op-id/run-id/callbacks/request-id",
		},
	} {
		t.Run(tc.executionType.String(), func(t *testing.T) {
			businessID := map[enumspb.ExecutionType]string{
				enumspb.EXECUTION_TYPE_WORKFLOW:        "wf-id",
				enumspb.EXECUTION_TYPE_ACTIVITY:        "act-id",
				enumspb.EXECUTION_TYPE_NEXUS_OPERATION: "op-id",
			}[tc.executionType]

			input := &commonpb.Link_Callback{
				Namespace: "ns",
				Execution: &commonpb.Execution{
					Type:       tc.executionType,
					BusinessId: businessID,
					RunId:      "run-id",
				},
				RequestId: "request-id",
			}

			link, err := commonnexus.ConvertLinkCallbackToNexusLink(input)
			require.NoError(t, err)
			require.Equal(t, "temporal.api.common.v1.Link.Callback", link.Type)
			require.Equal(t, tc.wantURL, link.URL.String())

			output, err := commonnexus.ConvertNexusLinkToLinkCallback(link)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, input, output)
		})
	}
}

// A callback attached to a child component (e.g. a workflow update) carries a component path, whose
// segments and their order have to survive the round trip.
func TestConvertLinkCallback_RoundTripsComponentPath(t *testing.T) {
	// Return a Link_Callback, but without any ComponentPath.
	getInput := func() *commonpb.Link_Callback {
		return &commonpb.Link_Callback{
			Namespace: "ns",
			Execution: &commonpb.Execution{
				Type:       enumspb.EXECUTION_TYPE_WORKFLOW,
				BusinessId: "wf-id",
				RunId:      "run-id",
			},
			RequestId: "request-id",
		}
	}

	t.Run("NoComponentPath", func(t *testing.T) {
		// All of these result in no URL query parameters being added in the
		// converted Nexus link URL.
		cases := [][]string{
			nil,
			[]string{},
		}
		for _, tc := range cases {
			input := getInput()
			input.ComponentPath = tc

			link, err := commonnexus.ConvertLinkCallbackToNexusLink(input)
			require.NoError(t, err)
			require.Equal(
				t,
				"temporal:///namespaces/ns/workflows/wf-id/run-id/callbacks/request-id",
				link.URL.String(),
			)

			output, err := commonnexus.ConvertNexusLinkToLinkCallback(link)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, input, output)
		}
	})

	t.Run("WithComponentPath", func(t *testing.T) {
		input := getInput()
		input.ComponentPath = []string{"A:B", "C/D", "E_F"}

		link, err := commonnexus.ConvertLinkCallbackToNexusLink(input)
		require.NoError(t, err)
		require.Equal(
			t,
			"temporal:///namespaces/ns/workflows/wf-id/run-id/callbacks/request-id"+
				// Note the component path is in-order and URI encoded.
				"?componentPath=A%3AB&componentPath=C%2FD&componentPath=E_F",
			link.URL.String(),
		)

		output, err := commonnexus.ConvertNexusLinkToLinkCallback(link)
		require.NoError(t, err)
		protorequire.ProtoEqual(t, input, output)
	})

	// Each segment is its own query parameter rather than being joined into one value, so a
	// delimiter occurring inside a segment cannot be mistaken for a separator. Path segments end in
	// a user-supplied ID (an update ID is only length-validated), so a comma is reachable and a
	// comma-joined encoding would need its own escaping layer on top of the URL's.
	t.Run("SegmentContainingDelimiters", func(t *testing.T) {
		input := getInput()
		input.ComponentPath = []string{"Updates", "id,with,commas"}

		link, err := commonnexus.ConvertLinkCallbackToNexusLink(input)
		require.NoError(t, err)
		require.Equal(
			t,
			"temporal:///namespaces/ns/workflows/wf-id/run-id/callbacks/request-id"+
				"?componentPath=Updates&componentPath=id%2Cwith%2Ccommas",
			link.URL.String(),
		)

		output, err := commonnexus.ConvertNexusLinkToLinkCallback(link)
		require.NoError(t, err)
		protorequire.ProtoEqual(t, input, output)
	})

	// url.Values.Encode sorts by key but keeps each key's values in insertion order, which is what
	// the path relies on. Only a path whose order differs from its sorted order proves that; the
	// cases above happen to already be in sorted order.
	t.Run("PreservesUnsortedOrder", func(t *testing.T) {
		input := getInput()
		input.ComponentPath = []string{"zulu", "alpha"}

		link, err := commonnexus.ConvertLinkCallbackToNexusLink(input)
		require.NoError(t, err)
		require.Equal(
			t,
			"temporal:///namespaces/ns/workflows/wf-id/run-id/callbacks/request-id"+
				"?componentPath=zulu&componentPath=alpha",
			link.URL.String(),
		)

		output, err := commonnexus.ConvertNexusLinkToLinkCallback(link)
		require.NoError(t, err)
		protorequire.ProtoEqual(t, input, output)
	})
}

// IDs are user-supplied and may contain characters that would otherwise change the shape of the
// path, so they have to be escaped on the way out and unescaped on the way back.
func TestConvertLinkCallback_RoundTripsEscapedIDs(t *testing.T) {
	input := &commonpb.Link_Callback{
		Namespace: "ns/with-slash",
		Execution: &commonpb.Execution{
			Type:       enumspb.EXECUTION_TYPE_WORKFLOW,
			BusinessId: "wf-id/callbacks/fake",
			RunId:      "run-id",
		},
		RequestId: "request id?x=1",
	}

	link, err := commonnexus.ConvertLinkCallbackToNexusLink(input)
	require.NoError(t, err)
	require.Equal(
		t,
		"temporal:///namespaces/ns%2Fwith-slash/workflows/wf-id%2Fcallbacks%2Ffake/run-id"+
			"/callbacks/request%20id%3Fx=1",
		link.URL.String(),
	)

	output, err := commonnexus.ConvertNexusLinkToLinkCallback(link)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, input, output)
}

func TestConvertLinkCallbackToNexusLink_UnsupportedExecutionType(t *testing.T) {
	_, err := commonnexus.ConvertLinkCallbackToNexusLink(&commonpb.Link_Callback{
		Namespace: "ns",
		Execution: &commonpb.Execution{
			Type:       enumspb.EXECUTION_TYPE_UNSPECIFIED,
			BusinessId: "wf-id",
			RunId:      "run-id",
		},
		RequestId: "request-id",
	})
	require.ErrorContains(t, err, "unsupported execution type")
}

func TestConvertNexusLinkToLinkCallback_Invalid(t *testing.T) {
	callbackLinkType := "temporal.api.common.v1.Link.Callback"

	for _, tc := range []struct {
		name      string
		link      nexus.Link
		wantError string
	}{
		{
			name: "wrong-type",
			link: nexus.Link{
				URL:  &url.URL{Scheme: "temporal", Path: "/namespaces/ns/workflows/wf-id/run-id/callbacks/req-id"},
				Type: "temporal.api.common.v1.Link.Activity",
			},
			wantError: "cannot parse link type",
		},
		{
			name: "wrong-scheme",
			link: nexus.Link{
				URL:  &url.URL{Scheme: "https", Path: "/namespaces/ns/workflows/wf-id/run-id/callbacks/req-id"},
				Type: callbackLinkType,
			},
			wantError: "invalid scheme",
		},
		{
			name: "unknown-execution-type",
			link: nexus.Link{
				URL:  &url.URL{Scheme: "temporal", Path: "/namespaces/ns/schedules/sched-id/run-id/callbacks/req-id"},
				Type: callbackLinkType,
			},
			wantError: "malformed URL path",
		},
		{
			name: "missing-request-id-segment",
			link: nexus.Link{
				URL:  &url.URL{Scheme: "temporal", Path: "/namespaces/ns/workflows/wf-id/run-id/callbacks"},
				Type: callbackLinkType,
			},
			wantError: "malformed URL path",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := commonnexus.ConvertNexusLinkToLinkCallback(tc.link)
			require.ErrorContains(t, err, tc.wantError)
		})
	}
}
