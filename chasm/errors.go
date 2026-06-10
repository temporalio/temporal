package chasm

import (
	"fmt"
	"strings"

	"go.temporal.io/server/common/log/tag"
)

type ExecutionAlreadyStartedError struct {
	Message          string
	CurrentRequestID string
	CurrentRunID     string
}

func NewExecutionAlreadyStartedErr(
	message, currentRequestID, currentRunID string,
) *ExecutionAlreadyStartedError {
	return &ExecutionAlreadyStartedError{
		Message:          message,
		CurrentRequestID: currentRequestID,
		CurrentRunID:     currentRunID,
	}
}

func (e *ExecutionAlreadyStartedError) Error() string {
	return e.Message
}

// TaskNotInvalidatedError indicates a CHASM task executed successfully but its
// validator still reports the task as valid. Retrying such a task can loop
// forever, so queue executors should treat it as terminal and send it to DLQ
// when task DLQ is enabled.
type TaskNotInvalidatedError struct {
	TaskKind string
	TaskInfo string

	TaskNotInvalidatedDetails
}

func NewTaskNotInvalidatedError(
	taskKind string,
	taskInfo string,
) *TaskNotInvalidatedError {
	return &TaskNotInvalidatedError{
		TaskKind: taskKind,
		TaskInfo: taskInfo,
	}
}

// TaskNotInvalidatedDetails identifies the logical CHASM task that remained
// valid after successful handler execution.
type TaskNotInvalidatedDetails struct {
	TaskType             string
	TaskTypeID           uint32
	Archetype            string
	ArchetypeID          ArchetypeID
	ComponentPath        []string
	EncodedComponentPath string
	TaskAttributes       TaskAttributes
}

// NewTaskNotInvalidatedErrorWithDetails returns an error with logical task
// context that can be used to locate and repair a stuck CHASM execution.
func NewTaskNotInvalidatedErrorWithDetails(
	taskKind string,
	details TaskNotInvalidatedDetails,
) *TaskNotInvalidatedError {
	return &TaskNotInvalidatedError{
		TaskKind:                  taskKind,
		TaskNotInvalidatedDetails: details,
	}
}

func (e *TaskNotInvalidatedError) Error() string {
	msg := fmt.Sprintf("CHASM %s task remained valid after successful execution", e.TaskKind)
	if e.TaskInfo != "" {
		return fmt.Sprintf("%s: %s", msg, e.TaskInfo)
	}
	return msg
}

func (e *TaskNotInvalidatedError) LogTags() []tag.Tag {
	tags := []tag.Tag{
		tag.String("chasm-task-kind", e.TaskKind),
	}

	if e.TaskType != "" {
		tags = append(tags, tag.String("chasm-task-type", e.TaskType))
	}
	if e.TaskTypeID != 0 {
		tags = append(tags, tag.UInt32("chasm-task-type-id", e.TaskTypeID))
	}
	if e.Archetype != "" {
		tags = append(tags, tag.Archetype(e.Archetype))
	}
	if e.ArchetypeID != 0 {
		tags = append(tags, tag.ArchetypeID(uint32(e.ArchetypeID)))
	}
	if e.ComponentPath != nil {
		tags = append(tags, tag.String("chasm-component-path", formatTaskNotInvalidatedPath(e.ComponentPath)))
	}
	if e.EncodedComponentPath != "" {
		tags = append(tags, tag.String("chasm-encoded-component-path", e.EncodedComponentPath))
	}
	if !e.TaskAttributes.ScheduledTime.IsZero() {
		tags = append(tags, tag.Time("chasm-task-scheduled-time", e.TaskAttributes.ScheduledTime))
	}
	if e.TaskAttributes.Destination != "" {
		tags = append(tags, tag.String("chasm-task-destination", e.TaskAttributes.Destination))
	}
	if e.TaskAttributes.IsImmediate() {
		tags = append(tags, tag.Bool("chasm-task-immediate", true))
	}
	return tags
}

func formatTaskNotInvalidatedPath(
	path []string,
) string {
	return strings.Join(path, "/")
}

func (e *TaskNotInvalidatedError) IsTerminalTaskError() bool {
	return true
}
