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

package scheduler

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"google.golang.org/protobuf/proto"

	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
)

type (
	ProcessWorkflowCompletionEvent struct{}
	Describe                       struct{}
	ListMatchingTimes              struct{}
	PatchSchedule                  struct{}
	UpdateSchedule                 struct{}
)

func (p ProcessWorkflowCompletionEvent) Name() string {
	return "scheduler.process_workflow_completion_event"
}

func (p ProcessWorkflowCompletionEvent) SerializeOutput(_ any) ([]byte, error) {
	// ProcessWorkflowCompletionEvent outputs void and therefore does nothing for serialization.
	return nil, nil
}

func (p ProcessWorkflowCompletionEvent) DeserializeInput(data []byte) (any, error) {
	input := &persistencepb.HSMCompletionCallbackArg{}
	if err := proto.Unmarshal(data, input); err != nil {
		return nil, serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return input, nil
}

func (d Describe) Name() string { return "scheduler.describe" }

func (d Describe) SerializeOutput(response any) ([]byte, error) {
	if output, ok := response.(*schedspb.DescribeResponse); ok {
		return proto.Marshal(output)
	}
	return nil, fmt.Errorf("invalid describe output provided: %v", response) // nolint:goerr113
}

func (d Describe) DeserializeInput(_ []byte) (any, error) {
	// No input arguments
	return nil, nil
}

func (l ListMatchingTimes) Name() string { return "scheduler.list_matching_times" }

func (l ListMatchingTimes) SerializeOutput(response any) ([]byte, error) {
	if output, ok := response.(*workflowservice.ListScheduleMatchingTimesResponse); ok {
		return proto.Marshal(output)
	}
	return nil, fmt.Errorf("invalid listMatchingTimes output provided: %v", response) // nolint:goerr113
}

func (l ListMatchingTimes) DeserializeInput(data []byte) (any, error) {
	input := &workflowservice.ListScheduleMatchingTimesRequest{}
	if err := proto.Unmarshal(data, input); err != nil {
		return nil, serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return input, nil
}

func (p PatchSchedule) Name() string { return "scheduler.patch_schedule" }

func (p PatchSchedule) SerializeOutput(_ any) ([]byte, error) {
	return nil, nil
}

func (p PatchSchedule) DeserializeInput(data []byte) (any, error) {
	input := &schedpb.SchedulePatch{}
	if err := proto.Unmarshal(data, input); err != nil {
		return nil, serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return input, nil
}

func (u UpdateSchedule) Name() string { return "scheduler.update_schedule" }

func (u UpdateSchedule) SerializeOutput(_ any) ([]byte, error) {
	return nil, nil
}

func (u UpdateSchedule) DeserializeInput(data []byte) (any, error) {
	input := &schedspb.FullUpdateRequest{}
	if err := proto.Unmarshal(data, input); err != nil {
		return nil, serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return input, nil
}
