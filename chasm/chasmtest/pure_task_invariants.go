package chasmtest

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/server/chasm"
)

// PureTaskAssumptionViolation reports a pure task whose validator still accepted
// it after it had executed.
type PureTaskAssumptionViolation struct {
	TaskType      string
	ScheduledTime time.Time
}

func (v PureTaskAssumptionViolation) String() string {
	return fmt.Sprintf(
		"%s (scheduled %s) is still valid after executing, so it executed a second time",
		v.TaskType, v.ScheduledTime.Format(time.RFC3339Nano))
}

// FirePureTasksStrict behaves like [Engine.FirePureTasks], and additionally
// enforces a load-bearing framework assumption:
//
//	A pure task MUST become invalid once it has executed.
//
// Why it is load-bearing: a validator returning false is the only mechanism that
// removes a task from ComponentAttributes.PureTasks - see
// closeTransactionCleanupInvalidTasks in chasm/tree.go. A task that stays valid
// is never pruned. It is now past-due, so it remains the tree's earliest pure
// task, and because its PhysicalTaskStatus is already physicalTaskStatusCreated,
// closeTransactionGeneratePhysicalPureTask declines to create another physical
// timer. Only the earliest pure task in the tree is ever considered for a timer,
// so the whole execution - every component, not just the offending one - stops
// receiving pure task timers. There is no ordinary self-healing path: recovery
// requires RefreshTasks, replication recreating the node, or a task backdated
// earlier than the stranded one.
//
// Detection re-runs each task immediately after it executes. A compliant task
// fails its own validator on the second call and returns ran=false having done
// nothing. A non-compliant task executes again, which both flags the violation
// and demonstrates the re-execution loop it would cause in production.
//
// This is the check described by the TODO in [chasm.Node.EachPureTask] ("Add a
// validation for that and return an internal error if tasks is still valid after
// processing"), applied from the test side. Once the framework enforces the
// invariant itself, callers of this helper can fall back to FirePureTasks.
//
// Note that only persisted, timer-driven pure tasks are observed. Immediate pure
// tasks (chasm.TaskScheduledTimeImmediate) execute inline during CloseTransaction
// and are never persisted, so they are structurally incapable of stranding a
// timer and never reach this code path.
func (e *Engine) FirePureTasksStrict(
	ref chasm.ComponentRef,
	referenceTime time.Time,
) (executedTypes []string, violations []PureTaskAssumptionViolation, err error) {
	exec, err := e.executionForRef(ref)
	if err != nil {
		return nil, nil, err
	}

	engineCtx := chasm.NewEngineContext(context.Background(), e)
	if err := exec.node.EachPureTask(
		referenceTime,
		func(handler chasm.NodePureTask, taskAttributes chasm.TaskAttributes, taskInstance any) (bool, error) {
			ran, err := handler.ExecutePureTask(engineCtx, taskAttributes, taskInstance)
			if err != nil || !ran {
				return ran, err
			}
			executedTypes = append(executedTypes, fmt.Sprintf("%T", taskInstance))

			reran, rerunErr := handler.ExecutePureTask(engineCtx, taskAttributes, taskInstance)
			if rerunErr != nil {
				return ran, rerunErr
			}
			if reran {
				violations = append(violations, PureTaskAssumptionViolation{
					TaskType:      fmt.Sprintf("%T", taskInstance),
					ScheduledTime: taskAttributes.ScheduledTime,
				})
			}
			return ran, nil
		},
	); err != nil {
		return executedTypes, violations, err
	}

	if err := exec.closeTransaction(); err != nil {
		return executedTypes, violations, err
	}
	return executedTypes, violations, nil
}
