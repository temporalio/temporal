package queues

import (
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/tasks"
)

type (
	// PriorityAssigner assigns priority to task executables
	PriorityAssigner interface {
		Assign(Executable) tasks.Priority
	}

	priorityAssignerImpl struct{}

	staticPriorityAssigner struct {
		priority tasks.Priority
	}
)

func NewPriorityAssigner() PriorityAssigner {
	return &priorityAssignerImpl{}
}

func (a *priorityAssignerImpl) Assign(executable Executable) tasks.Priority {
	taskType := executable.GetType()
	switch taskType {
	case enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
		enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT,
		enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
		enumsspb.TASK_TYPE_WORKFLOW_EXECUTION_TIMEOUT:
		return tasks.PriorityLow
	case enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION,
		enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
		enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION,
		enumsspb.TASK_TYPE_UNSPECIFIED:
		// add more task types here if we believe it's ok to delay those tasks
		// and assign them the same priority as throttled tasks
		return tasks.PriorityPreemptable
	}

	if _, ok := enumsspb.TaskType_name[int32(taskType)]; !ok {
		// low priority for unknown task types
		return tasks.PriorityPreemptable
	}

	return tasks.PriorityHigh
}

func NewNoopPriorityAssigner() PriorityAssigner {
	return NewStaticPriorityAssigner(tasks.PriorityHigh)
}

func NewStaticPriorityAssigner(priority tasks.Priority) PriorityAssigner {
	return staticPriorityAssigner{priority: priority}
}

func (a staticPriorityAssigner) Assign(_ Executable) tasks.Priority {
	return a.priority
}
