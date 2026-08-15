package umpiretest

import (
	"errors"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2"
)

// PlanTarget identifies a structural lifecycle destination without selecting a runtime entity.
type PlanTarget struct {
	Entity umpire.EntityType
	State  string
}

// PlanRequest contains the explicit structural planning policy.
type PlanRequest struct {
	Target      PlanTarget
	Mode        umpire.RouteMode
	Constraints umpire.Constraints
	Seed        int64
}

// PlanResult retains the planning defaults and compiled lifecycle route plan.
type PlanResult struct {
	Target      PlanTarget
	Mode        umpire.RouteMode
	Constraints umpire.Constraints
	Seed        int64
	Plan        *umpire.Plan
}

// Plan validates structural intent against one compiled Temporal protocol.
func Plan(protocol *umpire2.Protocol, request PlanRequest) (PlanResult, error) {
	if protocol == nil {
		return PlanResult{}, errors.New("umpiretest planning: protocol is nil")
	}
	if request.Target.Entity == "" {
		return PlanResult{}, errors.New("umpiretest planning: target entity is empty")
	}
	if request.Target.State == "" {
		return PlanResult{}, errors.New("umpiretest planning: target state is empty")
	}
	seed := request.Seed
	if seed == 0 {
		seed = 1
	}
	plan, err := protocol.PlanTo(
		request.Target.Entity,
		request.Target.State,
		request.Mode,
		request.Constraints,
		umpire.WithSeed(seed),
	)
	result := PlanResult{
		Target: request.Target, Mode: request.Mode, Constraints: request.Constraints, Seed: seed, Plan: plan,
	}
	if err != nil {
		return result, err
	}
	return result, nil
}
