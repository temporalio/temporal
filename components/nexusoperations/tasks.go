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

	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
)

const (
	TaskTypeTimeout = "nexusoperations.Timeout"
	TaskTypeInvocation = "nexusoperations.Invocation"
	TaskTypeBackoff = "nexusoperations.Backoff"
	TaskTypeCancelation = "nexusoperations.Cancelation"
	TaskTypeCancelationBackoff = "nexusoperations.CancelationBackoff"
)

type TimeoutTask struct {
	Deadline time.Time
}

var _ hsm.Task = TimeoutTask{}

func (TimeoutTask) Type() string {
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
			consts.ErrStaleReference,
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

func (InvocationTask) Type() string {
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

func (BackoffTask) Type() string {
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

func (CancelationTask) Type() string {
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

type CancelationBackoffTask struct {
	Deadline time.Time
}

var _ hsm.Task = CancelationBackoffTask{}

func (CancelationBackoffTask) Type() string {
	return TaskTypeCancelationBackoff
}

func (t CancelationBackoffTask) Kind() hsm.TaskKind {
	return hsm.TaskKindTimer{Deadline: t.Deadline}
}

func (CancelationBackoffTask) Concurrent() bool {
	return false
}

type CancelationBackoffTaskSerializer struct{}

func (CancelationBackoffTaskSerializer) Deserialize(data []byte, kind hsm.TaskKind) (hsm.Task, error) {
	if kind, ok := kind.(hsm.TaskKindTimer); ok {
		return CancelationBackoffTask{Deadline: kind.Deadline}, nil
	}
	return nil, fmt.Errorf("%w: expected timer", hsm.ErrInvalidTaskKind)
}

func (CancelationBackoffTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

func RegisterTaskSerializers(reg *hsm.Registry) error {
	if err := reg.RegisterTaskSerializer(TaskTypeTimeout, TimeoutTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeInvocation, InvocationTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeBackoff, BackoffTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeCancelation, CancelationTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeCancelationBackoff, CancelationBackoffTaskSerializer{}); err != nil { // nolint:revive
		return err
	}
	return nil
}
