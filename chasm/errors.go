package chasm

import (
	"fmt"
	"strings"
	"time"
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

	TaskType             string
	TaskTypeID           uint32
	Archetype            string
	ArchetypeID          ArchetypeID
	ComponentPath        []string
	EncodedComponentPath string
	ScheduledTime        time.Time
	Destination          string
	Immediate            bool
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
	ScheduledTime        time.Time
	Destination          string
	Immediate            bool
}

// NewTaskNotInvalidatedErrorWithDetails returns an error with logical task
// context that can be used to locate and repair a stuck CHASM execution.
func NewTaskNotInvalidatedErrorWithDetails(
	taskKind string,
	details TaskNotInvalidatedDetails,
) *TaskNotInvalidatedError {
	return &TaskNotInvalidatedError{
		TaskKind:             taskKind,
		TaskType:             details.TaskType,
		TaskTypeID:           details.TaskTypeID,
		Archetype:            details.Archetype,
		ArchetypeID:          details.ArchetypeID,
		ComponentPath:        details.ComponentPath,
		EncodedComponentPath: details.EncodedComponentPath,
		ScheduledTime:        details.ScheduledTime,
		Destination:          details.Destination,
		Immediate:            details.Immediate,
	}
}

func (e *TaskNotInvalidatedError) Error() string {
	return fmt.Sprintf(
		"CHASM %s task remained valid after successful execution: %s",
		e.TaskKind,
		e.buildLogicalTaskReport(),
	)
}

func (e *TaskNotInvalidatedError) buildLogicalTaskReport() string {
	if e.TaskInfo != "" && e.TaskType == "" {
		return e.TaskInfo
	}

	var b strings.Builder
	b.WriteString("LogicalTask{")
	wroteField := false
	writeField := func(name string, value any) {
		if wroteField {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s: %v", name, value)
		wroteField = true
	}

	if e.TaskType != "" {
		writeField("Type", e.TaskType)
	}
	if e.TaskTypeID != 0 {
		writeField("TypeID", e.TaskTypeID)
	}
	if e.Archetype != "" {
		writeField("Archetype", e.Archetype)
	}
	if e.ArchetypeID != 0 {
		writeField("ArchetypeID", e.ArchetypeID)
	}
	if e.ComponentPath != nil {
		writeField("Path", formatTaskNotInvalidatedPath(e.ComponentPath))
	}
	if e.EncodedComponentPath != "" {
		writeField("EncodedPath", e.EncodedComponentPath)
	}
	if !e.ScheduledTime.IsZero() {
		writeField("ScheduledTime", e.ScheduledTime.Format(time.RFC3339Nano))
	}
	if e.Destination != "" {
		writeField("Destination", e.Destination)
	}
	if e.Immediate {
		writeField("Immediate", true)
	}
	b.WriteString("}")
	return b.String()
}

func formatTaskNotInvalidatedPath(
	path []string,
) string {
	return strings.Join(path, "/")
}

func (e *TaskNotInvalidatedError) IsTerminalTaskError() bool {
	return true
}
