// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package workflow

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/hsm"
)

// TransactionPolicy indicates whether a mutable state transaction is happening for an active namespace or passive namespace.
type TransactionPolicy int

const (
	TransactionPolicyActive  TransactionPolicy = 0
	TransactionPolicyPassive TransactionPolicy = 1
	// Mutable state is a top-level state machine in the state machines framework.
	StateMachineType = "workflow.MutableState"
)

func (policy TransactionPolicy) Ptr() *TransactionPolicy {
	return &policy
}

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
