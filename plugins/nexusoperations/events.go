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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/service/history/hsm"
)

type ScheduledEventDefinition struct{}

func (n ScheduledEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (n ScheduledEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
}

type CancelRequestedEventDefinition struct{}

func (n CancelRequestedEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (n CancelRequestedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED
}

type StartedEventDefinition struct{}

func (n StartedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (n StartedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED
}

type CompletedEventDefinition struct{}

func (n CompletedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (n CompletedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED
}

type FailedEventDefinition struct{}

func (n FailedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (n FailedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED
}

type CanceledEventDefinition struct{}

func (n CanceledEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (n CanceledEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED
}

type TimedOutEventDefinition struct{}

func (n TimedOutEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (n TimedOutEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT
}

func RegisterEventDefinitions(reg *hsm.Registry) error {
	if err := reg.RegisterEventDefinition(ScheduledEventDefinition{}); err != nil {
		return err
	}
	if err := reg.RegisterEventDefinition(CancelRequestedEventDefinition{}); err != nil {
		return err
	}
	if err := reg.RegisterEventDefinition(StartedEventDefinition{}); err != nil {
		return err
	}
	if err := reg.RegisterEventDefinition(CompletedEventDefinition{}); err != nil {
		return err
	}
	if err := reg.RegisterEventDefinition(FailedEventDefinition{}); err != nil {
		return err
	}
	if err := reg.RegisterEventDefinition(CanceledEventDefinition{}); err != nil {
		return err
	}
	return reg.RegisterEventDefinition(TimedOutEventDefinition{})
}
