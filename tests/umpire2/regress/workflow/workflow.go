// Package workflow provides typed workflow instructions for sparse regression plans.
package workflow

import "go.temporal.io/server/common/testing/umpire/regress"

var (
	WorkflowType  = regress.EntityType("WorkflowRun")
	StateType     = regress.ValueType("WorkflowState")
	RunIDType     = regress.ValueType("RunID")
	OperationType = regress.EntityType("NexusOperation")
	CallbackType  = regress.EntityType("Callback")
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
	callbackHandlerRunSchema = regress.RelationSchema(
		"workflow.callback_handler_run",
		regress.SymbolParameter("callback", CallbackType),
		regress.SymbolParameter("handler", WorkflowType),
	)
	nexusStorageAbsentSchema = regress.OutcomeSchema(
		"workflow.nexus_storage_absent",
		regress.SymbolParameter("workflow", WorkflowType),
		regress.SymbolParameter("operation", OperationType),
	)
)

func State(workflow string, state StateValue) regress.Instruction {
	return regress.Outcome(stateSchema, regress.Symbol(workflow), regress.Literal(state))
}

func RunID(workflow string) regress.Projection {
	return regress.Project(runIDSchema, regress.Symbol(workflow))
}

func CallbackHandlerRun(callback, handler string) regress.Instruction {
	return regress.Relation(callbackHandlerRunSchema, regress.Symbol(callback), regress.Symbol(handler))
}

func NexusStorageAbsent(workflow, operation string) regress.Instruction {
	return regress.Outcome(nexusStorageAbsentSchema, regress.Symbol(workflow), regress.Symbol(operation))
}

func StateSchema() regress.Schema              { return stateSchema }
func RunIDSchema() regress.Schema              { return runIDSchema }
func CallbackHandlerRunSchema() regress.Schema { return callbackHandlerRunSchema }
func NexusStorageAbsentSchema() regress.Schema { return nexusStorageAbsentSchema }
