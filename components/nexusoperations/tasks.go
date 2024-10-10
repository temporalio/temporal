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

	"go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
)

const (
	TaskTypeTimeout            = "nexusoperations.Timeout"
	TaskTypeInvocation         = "nexusoperations.Invocation"
	TaskTypeBackoff            = "nexusoperations.Backoff"
	TaskTypeCancelation        = "nexusoperations.Cancelation"
	TaskTypeCancelationBackoff = "nexusoperations.CancelationBackoff"
)

type TimeoutTask struct {
	deadline time.Time
}

var _ hsm.Task = TimeoutTask{}

func (TimeoutTask) Type() string {
	return TaskTypeTimeout
}

func (t TimeoutTask) Deadline() time.Time {
	return t.deadline
}

func (TimeoutTask) Destination() string {
	return ""
}

func (TimeoutTask) Concurrent() bool {
	return true
}

// Validate checks if the timeout task is still valid to execute for the given node state.
func (t TimeoutTask) Validate(node *hsm.Node) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
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

func (TimeoutTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return TimeoutTask{deadline: attrs.Deadline}, nil
}

func (TimeoutTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type InvocationTask struct {
	EndpointName string
}

var _ hsm.Task = InvocationTask{}

func (InvocationTask) Type() string {
	return TaskTypeInvocation
}

func (InvocationTask) Deadline() time.Time {
	return hsm.Immediate
}

func (t InvocationTask) Destination() string {
	return t.EndpointName
}

func (InvocationTask) Concurrent() bool {
	return true
}

func (t InvocationTask) Validate(node *hsm.Node) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	op, err := hsm.MachineData[Operation](node)
	if err != nil {
		return err
	}
	if op.State() != enums.NEXUS_OPERATION_STATE_SCHEDULED {
		return fmt.Errorf(
			"%w: operation is not in Scheduled state, current state: %v",
			consts.ErrStaleReference,
			op.State(),
		)
	}
	return nil
}

type InvocationTaskSerializer struct{}

func (InvocationTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return InvocationTask{EndpointName: attrs.Destination}, nil
}

func (InvocationTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type BackoffTask struct {
	deadline time.Time
}

var _ hsm.Task = BackoffTask{}

func (BackoffTask) Type() string {
	return TaskTypeBackoff
}

func (t BackoffTask) Deadline() time.Time {
	return t.deadline
}

func (t BackoffTask) Destination() string {
	return ""
}

func (BackoffTask) Concurrent() bool {
	return true
}

func (t BackoffTask) Validate(node *hsm.Node) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	op, err := hsm.MachineData[Operation](node)
	if err != nil {
		return err
	}
	if op.State() != enums.NEXUS_OPERATION_STATE_BACKING_OFF {
		return fmt.Errorf(
			"%w: operation is not in BackingOff state, current state: %v",
			consts.ErrStaleReference,
			op.State(),
		)
	}
	return nil
}

type BackoffTaskSerializer struct{}

func (BackoffTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return BackoffTask{deadline: attrs.Deadline}, nil
}

func (BackoffTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type CancelationTask struct {
	EndpointName string
}

var _ hsm.Task = CancelationTask{}

func (CancelationTask) Type() string {
	return TaskTypeCancelation
}

func (CancelationTask) Deadline() time.Time {
	return hsm.Immediate
}

func (t CancelationTask) Destination() string {
	return t.EndpointName
}

func (CancelationTask) Concurrent() bool {
	return false
}

type CancelationTaskSerializer struct{}

func (CancelationTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return CancelationTask{EndpointName: attrs.Destination}, nil
}

func (CancelationTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type CancelationBackoffTask struct {
	deadline time.Time
}

var _ hsm.Task = CancelationBackoffTask{}

func (CancelationBackoffTask) Type() string {
	return TaskTypeCancelationBackoff
}

func (t CancelationBackoffTask) Deadline() time.Time {
	return t.deadline
}

func (CancelationBackoffTask) Destination() string {
	return ""
}

func (CancelationBackoffTask) Concurrent() bool {
	return false
}

type CancelationBackoffTaskSerializer struct{}

func (CancelationBackoffTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return CancelationBackoffTask{deadline: attrs.Deadline}, nil
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
