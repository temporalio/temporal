package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*NotifyActivityTask)(nil)
var _ HasDestination = (*NotifyActivityTask)(nil)

type (
	// NotifyActivityTask is an task that notifies activities via Nexus.
	NotifyActivityTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64

		// NotificationType specifies the type of notification.
		NotificationType enumsspb.ActivityNotificationType
		// ScheduledEventIDs of activities to notify (batched by worker).
		ScheduledEventIDs []int64
		// Destination is the worker control task queue for outbound queue grouping.
		Destination string
	}
)

func (t *NotifyActivityTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

func (t *NotifyActivityTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *NotifyActivityTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *NotifyActivityTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *NotifyActivityTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *NotifyActivityTask) GetCategory() Category {
	return CategoryOutbound
}

func (t *NotifyActivityTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_NOTIFY_ACTIVITY
}

// GetDestination implements HasDestination for outbound queue grouping.
func (t *NotifyActivityTask) GetDestination() string {
	return t.Destination
}

func (t *NotifyActivityTask) String() string {
	return fmt.Sprintf("NotifyActivityTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, NotificationType: %v, ScheduledEventIDs: %v, Destination: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
		t.NotificationType,
		t.ScheduledEventIDs,
		t.Destination,
	)
}
