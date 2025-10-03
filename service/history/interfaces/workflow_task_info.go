package interfaces

import (
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/tasks"
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

	// Persisted timeout tasks for this workflow task. These are set when the timeout tasks are generated
	// and used to delete them when the workflow task completes.
	ScheduleToStartTimeoutTask *tasks.WorkflowTaskTimeoutTask
	StartToCloseTimeoutTask    *tasks.WorkflowTaskTimeoutTask
}

type WorkflowTaskCompletionLimits struct {
	MaxResetPoints              int
	MaxSearchAttributeValueSize int
}
