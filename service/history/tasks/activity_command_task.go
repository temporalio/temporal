package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*ActivityCommandTask)(nil)
var _ HasDestination = (*ActivityCommandTask)(nil)

type (
	// ActivityCommandTask sends commands to activities via Nexus.
	ActivityCommandTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64

		// CommandType specifies the type of command.
		CommandType enumsspb.ActivityCommandType
		// TaskTokens of activities to send command to (batched by worker).
		TaskTokens [][]byte
		// Destination is the worker control task queue for outbound queue grouping.
		Destination string
	}
)

func (t *ActivityCommandTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

func (t *ActivityCommandTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *ActivityCommandTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *ActivityCommandTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *ActivityCommandTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *ActivityCommandTask) GetCategory() Category {
	return CategoryOutbound
}

func (t *ActivityCommandTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_ACTIVITY_COMMAND
}

// GetDestination implements HasDestination for outbound queue grouping.
func (t *ActivityCommandTask) GetDestination() string {
	return t.Destination
}

func (t *ActivityCommandTask) String() string {
	return fmt.Sprintf("ActivityCommandTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, CommandType: %v, TaskTokens: %d, Destination: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
		t.CommandType,
		len(t.TaskTokens),
		t.Destination,
	)
}
