// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package nexusoperations

import (
	"fmt"
	"time"

	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
)

var (
	TaskTypeTimeout = hsm.TaskType{
		ID:   3,
		Name: "nexusoperations.Timeout",
	}
	TaskTypeInvocation = hsm.TaskType{
		ID:   4,
		Name: "nexusoperations.Invocation",
	}
	TaskTypeBackoff = hsm.TaskType{
		ID:   5,
		Name: "nexusoperations.Backoff",
	}
	TaskTypeCancelation = hsm.TaskType{
		ID:   6,
		Name: "nexusoperations.Cancelation",
	}
	TaskTypeCancelationBackoff = hsm.TaskType{
		ID:   7,
		Name: "nexusoperations.CancelationBackoff",
	}
)

type TimeoutTask struct {
	Deadline time.Time
}

var _ hsm.Task = TimeoutTask{}

func (TimeoutTask) Type() hsm.TaskType {
	return TaskTypeTimeout
}

func (t TimeoutTask) Kind() hsm.TaskKind {
	return hsm.TaskKindTimer{Deadline: t.Deadline}
}

func (TimeoutTask) Concurrent() bool {
	return true
}

// Validate checks if the timeout task is still valid to execute for the given node state.
func (t TimeoutTask) Validate(node *hsm.Node) error {
	op, err := hsm.MachineData[Operation](node)
	if err != nil {
		return err
	}
	if !TransitionTimedOut.Possible(op) {
		return fmt.Errorf(
			"%w: %w: cannot timeout machine in state %v",
			queues.ErrStaleTask,
			hsm.ErrInvalidTransition,
			op.State(),
		)
	}
	return nil
}

type TimeoutTaskSerializer struct{}

func (TimeoutTaskSerializer) Deserialize(data []byte, kind hsm.TaskKind) (hsm.Task, error) {
	if kind, ok := kind.(hsm.TaskKindTimer); ok {
		return TimeoutTask{Deadline: kind.Deadline}, nil
	}
	return nil, fmt.Errorf("%w: expected timer", hsm.ErrInvalidTaskKind)
}

func (TimeoutTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type InvocationTask struct {
	Destination string
}

var _ hsm.Task = InvocationTask{}

func (InvocationTask) Type() hsm.TaskType {
	return TaskTypeInvocation
}

func (t InvocationTask) Kind() hsm.TaskKind {
	return hsm.TaskKindOutbound{Destination: t.Destination}
}

func (InvocationTask) Concurrent() bool {
	return false
}

type InvocationTaskSerializer struct{}

func (InvocationTaskSerializer) Deserialize(data []byte, kind hsm.TaskKind) (hsm.Task, error) {
	if kind, ok := kind.(hsm.TaskKindOutbound); ok {
		return InvocationTask{Destination: kind.Destination}, nil
	}
	return nil, fmt.Errorf("%w: expected outbound", hsm.ErrInvalidTaskKind)
}

func (InvocationTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type BackoffTask struct {
	Deadline time.Time
}

var _ hsm.Task = BackoffTask{}

func (BackoffTask) Type() hsm.TaskType {
	return TaskTypeBackoff
}

func (t BackoffTask) Kind() hsm.TaskKind {
	return hsm.TaskKindTimer{Deadline: t.Deadline}
}

func (BackoffTask) Concurrent() bool {
	return false
}

type BackoffTaskSerializer struct{}

func (BackoffTaskSerializer) Deserialize(data []byte, kind hsm.TaskKind) (hsm.Task, error) {
	if kind, ok := kind.(hsm.TaskKindTimer); ok {
		return BackoffTask{Deadline: kind.Deadline}, nil
	}
	return nil, fmt.Errorf("%w: expected timer", hsm.ErrInvalidTaskKind)
}

func (BackoffTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type CancelationTask struct {
	Destination string
}

var _ hsm.Task = CancelationTask{}

func (CancelationTask) Type() hsm.TaskType {
	return TaskTypeCancelation
}

func (t CancelationTask) Kind() hsm.TaskKind {
	return hsm.TaskKindOutbound{Destination: t.Destination}
}

func (CancelationTask) Concurrent() bool {
	return false
}

type CancelationTaskSerializer struct{}

func (CancelationTaskSerializer) Deserialize(data []byte, kind hsm.TaskKind) (hsm.Task, error) {
	if kind, ok := kind.(hsm.TaskKindOutbound); ok {
		return CancelationTask{Destination: kind.Destination}, nil
	}
	return nil, fmt.Errorf("%w: expected outbound", hsm.ErrInvalidTaskKind)
}

func (CancelationTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type CancelationBackedTask struct {
	Deadline time.Time
}

var _ hsm.Task = CancelationBackedTask{}

func (CancelationBackedTask) Type() hsm.TaskType {
	return TaskTypeCancelationBackoff
}

func (t CancelationBackedTask) Kind() hsm.TaskKind {
	return hsm.TaskKindTimer{Deadline: t.Deadline}
}

func (CancelationBackedTask) Concurrent() bool {
	return false
}

type CancelationBackoffTaskSerializer struct{}

func (CancelationBackoffTaskSerializer) Deserialize(data []byte, kind hsm.TaskKind) (hsm.Task, error) {
	if kind, ok := kind.(hsm.TaskKindTimer); ok {
		return CancelationBackedTask{Deadline: kind.Deadline}, nil
	}
	return nil, fmt.Errorf("%w: expected timer", hsm.ErrInvalidTaskKind)
}

func (CancelationBackoffTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

func RegisterTaskSerializer(reg *hsm.Registry) error {
	if err := reg.RegisterTaskSerializer(TaskTypeTimeout.ID, TimeoutTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeInvocation.ID, InvocationTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeBackoff.ID, BackoffTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeCancelation.ID, CancelationTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeCancelationBackoff.ID, CancelationBackoffTaskSerializer{}); err != nil { // nolint:revive
		return err
	}
	return nil
}
