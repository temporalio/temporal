package migration

import (
	"errors"
	"fmt"
)

// ValidateLegacyState validates legacy scheduler state before migration to CHASM.
func ValidateLegacyState(legacy *LegacyState) error {
	if legacy == nil {
		return errors.New("legacy state is nil")
	}

	if legacy.Schedule == nil {
		return errors.New("schedule is required")
	}

	if legacy.State == nil {
		return errors.New("internal state is required")
	}

	state := legacy.State
	if state.Namespace == "" {
		return errors.New("namespace is required")
	}

	if state.NamespaceId == "" {
		return errors.New("namespace_id is required")
	}

	if state.ScheduleId == "" {
		return errors.New("schedule_id is required")
	}

	if state.ConflictToken <= 0 {
		return fmt.Errorf("conflict_token must be positive, got %d", state.ConflictToken)
	}

	// Validate buffered starts have required timestamps
	for i, start := range state.BufferedStarts {
		if start.NominalTime == nil {
			return fmt.Errorf("buffered_starts[%d]: nominal_time is required", i)
		}
		if start.ActualTime == nil {
			return fmt.Errorf("buffered_starts[%d]: actual_time is required", i)
		}
	}

	return nil
}

// ValidateCHASMState validates CHASM scheduler state before migration to legacy.
func ValidateCHASMState(chasm *CHASMState) error {
	if chasm == nil {
		return errors.New("CHASM state is nil")
	}

	if chasm.Scheduler == nil {
		return errors.New("scheduler state is required")
	}

	if chasm.Generator == nil {
		return errors.New("generator state is required")
	}

	if chasm.Invoker == nil {
		return errors.New("invoker state is required")
	}

	scheduler := chasm.Scheduler
	if scheduler.Schedule == nil {
		return errors.New("scheduler.schedule is required")
	}

	if scheduler.Namespace == "" {
		return errors.New("namespace is required")
	}

	if scheduler.NamespaceId == "" {
		return errors.New("namespace_id is required")
	}

	if scheduler.ScheduleId == "" {
		return errors.New("schedule_id is required")
	}

	if scheduler.ConflictToken <= 0 {
		return fmt.Errorf("conflict_token must be positive, got %d", scheduler.ConflictToken)
	}

	// CHASM buffered starts must have request_id and workflow_id
	for i, start := range chasm.Invoker.GetBufferedStarts() {
		if start.NominalTime == nil {
			return fmt.Errorf("invoker.buffered_starts[%d]: nominal_time is required", i)
		}
		if start.ActualTime == nil {
			return fmt.Errorf("invoker.buffered_starts[%d]: actual_time is required", i)
		}
		if start.RequestId == "" {
			return fmt.Errorf("invoker.buffered_starts[%d]: request_id is required in CHASM", i)
		}
		if start.WorkflowId == "" {
			return fmt.Errorf("invoker.buffered_starts[%d]: workflow_id is required in CHASM", i)
		}
	}

	return nil
}
