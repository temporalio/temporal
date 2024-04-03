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

type NexusOperationScheduledEventDefinition struct{}

func (n NexusOperationScheduledEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (n NexusOperationScheduledEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
}

type NexusOperationCancelRequestedEventDefinition struct{}

func (n NexusOperationCancelRequestedEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (n NexusOperationCancelRequestedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED
}

type NexusOperationStartedEventDefinition struct{}

func (n NexusOperationStartedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (n NexusOperationStartedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED
}

type NexusOperationCompletedEventDefinition struct{}

func (n NexusOperationCompletedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (n NexusOperationCompletedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED
}

type NexusOperationFailedEventDefinition struct{}

func (n NexusOperationFailedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (n NexusOperationFailedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED
}

type NexusOperationCanceledEventDefinition struct{}

func (n NexusOperationCanceledEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (n NexusOperationCanceledEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED
}

type NexusOperationTimedOutEventDefinition struct{}

func (n NexusOperationTimedOutEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (n NexusOperationTimedOutEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT
}

func RegisterEventDefinitions(reg *hsm.Registry) error {
	if err := reg.RegisterEventDefinition(NexusOperationScheduledEventDefinition{}); err != nil {
		return err
	}
	if err := reg.RegisterEventDefinition(NexusOperationCancelRequestedEventDefinition{}); err != nil {
		return err
	}
	if err := reg.RegisterEventDefinition(NexusOperationStartedEventDefinition{}); err != nil {
		return err
	}
	if err := reg.RegisterEventDefinition(NexusOperationCompletedEventDefinition{}); err != nil {
		return err
	}
	if err := reg.RegisterEventDefinition(NexusOperationFailedEventDefinition{}); err != nil {
		return err
	}
	if err := reg.RegisterEventDefinition(NexusOperationCanceledEventDefinition{}); err != nil {
		return err
	}
	return reg.RegisterEventDefinition(NexusOperationTimedOutEventDefinition{})
}
