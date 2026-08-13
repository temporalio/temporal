// Package planner is the active side's test-authoring surface: a developer describes the entity
// states they want exercised, and the planner computes a Plan (route[s] through the model graph)
// before driving anything.
//
// The generic route-finder — PlanTo/Explore over a Lifecycle under Constraints — lives in the
// framework (common/testing/umpire). This package re-exports it alongside the Temporal
// Models/DefaultModels registry in models.go.
//
// See UMPIRE.md for the broader architecture.
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
