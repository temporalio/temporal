package workflow

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/hsm"
)

// TransactionPolicy indicates whether a mutable state transaction is happening for an active namespace or passive namespace.

const (
	// Mutable state is a top-level state machine in the state machines framework.
	StateMachineType = "workflow.MutableState"
)

type stateMachineDefinition struct{}

// TODO: Remove this implementation once transition history is fully implemented.
func (s stateMachineDefinition) CompareState(any, any) (int, error) {
	return 0, serviceerror.NewUnimplemented("CompareState not implemented for workflow mutable state")
}

func (stateMachineDefinition) Deserialize([]byte) (any, error) {
	return nil, serviceerror.NewUnimplemented("workflow mutable state persistence is not supported in the HSM framework")
}

// Serialize is a noop as Deserialize is not supported.
func (stateMachineDefinition) Serialize(any) ([]byte, error) {
	return nil, nil
}

func (stateMachineDefinition) Type() string {
	return StateMachineType
}

func RegisterStateMachine(reg *hsm.Registry) error {
	return reg.RegisterMachine(stateMachineDefinition{})
}
