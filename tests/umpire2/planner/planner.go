// Package planner is the active side's test-authoring surface: a developer describes the entity
// states they want exercised, and the planner computes a Plan (route[s] through the model graph)
// before driving anything.
//
// The generic route-finder — PlanTo/Explore over a Lifecycle under Constraints — now lives in
// the framework (common/testing/umpire) so any consumer can use it without this package's
// Temporal model registry. This file re-exports it, so existing callers keep using planner.X;
// the Temporal Models/DefaultModels registry stays in models.go.
//
// See UMPIRE_PLANNER.md for the developer guide and UMPIRE_DRIVER.md for the broader architecture.
package planner

import umpire "go.temporal.io/server/common/testing/umpire"

// Route-finding types and functions, re-exported from the framework.
type (
	RouteMode   = umpire.RouteMode
	Constraints = umpire.Constraints
	Plan        = umpire.Plan
	Option      = umpire.Option
	Step        = umpire.Step
	Driver      = umpire.Driver
	Resetter    = umpire.Resetter
)

const (
	Shortest  = umpire.Shortest
	AllRoutes = umpire.AllRoutes
	Random    = umpire.Random
)

var (
	PlanTo   = umpire.PlanTo
	Explore  = umpire.Explore
	WithSeed = umpire.WithSeed
)
