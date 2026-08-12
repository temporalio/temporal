// Package workflow provides typed workflow instructions for sparse regression plans.
package workflow

import "go.temporal.io/server/common/testing/umpire/regress"

var (
	WorkflowType = regress.EntityType("WorkflowRun")
	StateType    = regress.ValueType("WorkflowState")
	RunIDType    = regress.ValueType("RunID")
)

type StateValue string

const (
	Started   StateValue = "started"
	Completed StateValue = "completed"
)

var (
	stateSchema = regress.OutcomeSchema(
		"workflow.state",
		regress.SymbolParameter("workflow", WorkflowType),
		regress.LiteralParameter("state", StateType),
	)
	runIDSchema = regress.ProjectionSchema(
		"workflow.run_id",
		RunIDType,
		regress.SymbolParameter("workflow", WorkflowType),
	)
)

func State(workflow string, state StateValue) regress.Instruction {
	return regress.Outcome(stateSchema, regress.Symbol(workflow), regress.Literal(state))
}

func RunID(workflow string) regress.Projection {
	return regress.Project(runIDSchema, regress.Symbol(workflow))
}

func StateSchema() regress.Schema { return stateSchema }
func RunIDSchema() regress.Schema { return runIDSchema }
