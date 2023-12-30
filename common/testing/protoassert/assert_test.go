// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package protoassert_test

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/common/testing/protoassert"
)

const myUUID = "deb7b204-b384-4fde-85c6-e5a56c42336a"

type noProtoNoCry struct {
	A float64
	B string
	C []bool
}

type canHazProto struct {
	A float64
	B *commonpb.WorkflowExecution
}

func TestProtoElementsMatch(t *testing.T) {
	assert := protoassert.New(t)
	for _, tc := range []struct {
		Name string
		A    any
		B    any
	}{{
		Name: "Shallow proto object - in order",
		A: []*commonpb.WorkflowExecution{{
			WorkflowId: "some random workflow ID",
			RunId:      myUUID,
		}, {
			WorkflowId: "second workflow",
			RunId:      myUUID,
		}},
		B: []*commonpb.WorkflowExecution{{
			WorkflowId: "some random workflow ID",
			RunId:      myUUID,
		}, {
			WorkflowId: "second workflow",
			RunId:      myUUID,
		}},
	}, {
		Name: "Shallow proto object - out of order",
		A: []*commonpb.WorkflowExecution{{
			WorkflowId: "some random workflow ID",
			RunId:      myUUID,
		}, {
			WorkflowId: "second workflow",
			RunId:      myUUID,
		}},
		B: []*commonpb.WorkflowExecution{{
			WorkflowId: "second workflow",
			RunId:      myUUID,
		}, {
			WorkflowId: "some random workflow ID",
			RunId:      myUUID,
		}},
	}, {
		Name: "Structs containing proto",
		A: []canHazProto{{
			A: 13,
			B: &commonpb.WorkflowExecution{
				WorkflowId: "some random workflow ID",
				RunId:      myUUID,
			},
		}, {
			A: 12,
			B: &commonpb.WorkflowExecution{
				WorkflowId: "second random workflow ID",
				RunId:      myUUID,
			},
		}},
		B: []canHazProto{{
			A: 12,
			B: &commonpb.WorkflowExecution{
				WorkflowId: "second random workflow ID",
				RunId:      myUUID,
			},
		}, {
			A: 13,
			B: &commonpb.WorkflowExecution{
				WorkflowId: "some random workflow ID",
				RunId:      myUUID,
			},
		}},
	}, {
		Name: "Nested proto struct",
		A: []*workflowpb.WorkflowExecutionInfo{
			{
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "some random workflow ID",
					RunId:      myUUID,
				},
			}, {
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "second random workflow ID",
					RunId:      myUUID,
				},
			}},
		B: []*workflowpb.WorkflowExecutionInfo{
			{
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "second random workflow ID",
					RunId:      myUUID,
				},
			}, {
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "some random workflow ID",
					RunId:      myUUID,
				},
			}},
	}} {
		if !assert.ProtoElementsMatch(tc.A, tc.B) {
			t.Errorf("%s: expected equality", tc.Name)
		}
	}
}
