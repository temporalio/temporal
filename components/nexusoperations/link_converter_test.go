// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package nexusoperations_test

import (
	"net/url"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/components/nexusoperations"
	"google.golang.org/protobuf/testing/protocmp"
)

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
			output := nexusoperations.ConvertLinkWorkflowEventToNexusLink(tc.input)
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
			output, err := nexusoperations.ConvertNexusLinkToLinkWorkflowEvent(tc.input)
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
