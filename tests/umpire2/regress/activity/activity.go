// Package activity provides typed activity instructions for sparse regression plans.
package activity

import "go.temporal.io/server/common/testing/umpire/regress"

var (
	ActivityType  = regress.EntityType("Activity")
	OperationType = regress.EntityType("NexusOperation")
	StateType     = regress.ValueType("ActivityState")
)

type StateValue string

const (
	Completed StateValue = "completed"
)

var (
	stateSchema = regress.OutcomeSchema(
		"activity.state",
		regress.SymbolParameter("activity", ActivityType),
		regress.LiteralParameter("state", StateType),
	)
	linkedToNexusOperationSchema = regress.RelationSchema(
		"activity.linked_to_nexus_operation",
		regress.SymbolParameter("activity", ActivityType),
		regress.SymbolParameter("operation", OperationType),
	)
)

func State(activity string, state StateValue) regress.Instruction {
	return regress.Outcome(stateSchema, regress.Symbol(activity), regress.Literal(state))
}

func LinkedToNexusOperation(activity, operation string) regress.Instruction {
	return regress.Relation(linkedToNexusOperationSchema, regress.Symbol(activity), regress.Symbol(operation))
}

func StateSchema() regress.Schema                  { return stateSchema }
func LinkedToNexusOperationSchema() regress.Schema { return linkedToNexusOperationSchema }
