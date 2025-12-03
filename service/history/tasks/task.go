//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination task_mock.go

package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
)

type (
	// Task is the generic task interface
	Task interface {
		GetKey() Key
		GetNamespaceID() string
		GetWorkflowID() string
		GetRunID() string
		GetTaskID() int64
		GetVisibilityTime() time.Time
		GetCategory() Category
		GetType() enumsspb.TaskType

		SetTaskID(id int64)
		SetVisibilityTime(timestamp time.Time)

		// TODO: All tasks should have a method returning the versioned transition
		// in which the task is generated.
		// This versioned transition can be used to determine if the task or
		// the mutable state is stale.
	}

	// HasVersion is a legacy method for tasks that uses only version to determine
	// if the task should be executed or not.
	HasVersion interface {
		GetVersion() int64
	}

	// HasStateMachineTaskType must be implemented by all HSM state machine tasks.
	HasStateMachineTaskType interface {
		StateMachineTaskType() string
	}
	// HasDestination must be implemented by all tasks used in the outbound queue.
	HasDestination interface {
		GetDestination() string
	}

	HasArchetypeID interface {
		GetArchetypeID() uint32
	}
)

// GetShardIDForTask computes the shardID for a given task using the task's namespace, workflow ID and the number of
// history shards in the cluster.
func GetShardIDForTask(task Task, numShards int) int {
	return int(common.WorkflowIDToHistoryShard(task.GetNamespaceID(), task.GetWorkflowID(), int32(numShards)))
}
