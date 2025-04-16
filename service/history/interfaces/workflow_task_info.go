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

package interfaces

import (
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
)

// TODO: This should be part of persistence layer
type WorkflowTaskInfo struct {
	Version             int64
	ScheduledEventID    int64
	StartedEventID      int64
	RequestID           string
	WorkflowTaskTimeout time.Duration
	// This is only needed to communicate task queue used after AddWorkflowTaskScheduledEvent.
	TaskQueue *taskqueuepb.TaskQueue
	Attempt   int32
	// Scheduled and Started timestamps are useful for transient workflow task: when transient workflow task finally completes,
	// use these Timestamp to create scheduled/started events.
	// Also used for recording latency metrics
	ScheduledTime time.Time
	StartedTime   time.Time
	// OriginalScheduledTime is to record the first scheduled workflow task during workflow task heartbeat.
	// Client may to heartbeat workflow task by RespondWorkflowTaskComplete with ForceCreateNewWorkflowTask == true
	// In this case, OriginalScheduledTime won't change. Then when time.Now().UTC()-OriginalScheduledTime exceeds
	// some threshold, server can interrupt the heartbeat by enforcing to time out the workflow task.
	OriginalScheduledTime time.Time

	// Indicate type of the current workflow task (normal, transient, or speculative).
	Type enumsspb.WorkflowTaskType

	// These two fields are sent to workers in the WorkflowTaskStarted event. We need to save a
	// copy in mutable state to know the last values we sent (which might have been in a
	// transient event), otherwise a dynamic config change of the suggestion threshold could
	// cause the WorkflowTaskStarted event that the worker used to not match the event we saved
	// in history.
	SuggestContinueAsNew bool
	HistorySizeBytes     int64
	// BuildIdRedirectCounter tracks the started build ID redirect counter for transient/speculative WFT. This
	// info is to make sure the right redirect counter is used in the WFT started event created later
	// for a transient/speculative WFT.
	// Deprecated.
	BuildIdRedirectCounter int64
	// BuildId tracks the started build ID for transient/speculative WFT. This info is used for two purposes:
	// - verify WFT completes by the same Build ID that started in the latest attempt
	// - when persisting transient/speculative WFT, the right Build ID is used in the WFT started event
	// Deprecated.
	BuildId string
}

type WorkflowTaskCompletionLimits struct {
	MaxResetPoints              int
	MaxSearchAttributeValueSize int
}
