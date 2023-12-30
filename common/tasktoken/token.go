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

package tasktoken

import (
	v11 "go.temporal.io/server/api/clock/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewWorkflowTaskToken(
	namespaceID string,
	workflowID string,
	runID string,
	scheduledEventID int64,
	startedEventId int64,
	startedTime *timestamppb.Timestamp,
	attempt int32,
	clock *v11.VectorClock,
	version int64,
) *tokenspb.Task {
	return &tokenspb.Task{
		NamespaceId:      namespaceID,
		WorkflowId:       workflowID,
		RunId:            runID,
		ScheduledEventId: scheduledEventID,
		StartedEventId:   startedEventId,
		StartedTime:      startedTime,
		Attempt:          attempt,
		Clock:            clock,
		Version:          version,
	}
}

func NewActivityTaskToken(
	namespaceID string,
	workflowID string,
	runID string,
	scheduledEventID int64,
	activityId string,
	activityType string,
	attempt int32,
	clock *v11.VectorClock,
	version int64,
) *tokenspb.Task {
	return &tokenspb.Task{
		NamespaceId:      namespaceID,
		WorkflowId:       workflowID,
		RunId:            runID,
		ScheduledEventId: scheduledEventID,
		ActivityType:     activityType,
		Attempt:          attempt,
		ActivityId:       activityId,
		Clock:            clock,
		Version:          version,
	}
}
