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
		name   string
		input  *commonpb.Link_WorkflowEvent
		output nexus.Link
		errMsg string
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
					RawQuery: "eventID=1&eventType=WorkflowExecutionStarted&referenceType=EventReference",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
		},
		{
			name: "valid with percent-encoding",
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
					RawQuery: "eventID=1&eventType=WorkflowExecutionStarted&referenceType=EventReference",
				},
				Type: "temporal.api.common.v1.Link.WorkflowEvent",
			},
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
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := nexusoperations.ConvertLinkWorkflowEventToNexusLink(tc.input)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.output, output)
			}
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
			name: "valid",
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
			name: "valid with percent-encoding",
			input: nexus.Link{
				URL: &url.URL{
					Scheme:   "temporal",
					Path:     "/namespaces/ns/workflows/wf-id>/run-id/history",
					RawQuery: "referenceType=EventReference&eventID=1&eventType=WorkflowExecutionStarted",
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
