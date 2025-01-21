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
	"reflect"
	"strconv"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/service/history/hsm"
)

type ScheduledEventDefinition struct{}

func (d ScheduledEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (d ScheduledEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
}

func (d ScheduledEventDefinition) Apply(root *hsm.Node, event *historypb.HistoryEvent) error {
	token, err := hsm.GenerateEventLoadToken(event)
	if err != nil {
		return err
	}
	_, err = AddChild(root, strconv.FormatInt(event.EventId, 10), event, token)
	return err
}

func (d ScheduledEventDefinition) CherryPick(root *hsm.Node, event *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// We never cherry pick command events, and instead allow user logic to reschedule those commands.
	return hsm.ErrNotCherryPickable
}

type CancelRequestedEventDefinition struct{}

func (d CancelRequestedEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (d CancelRequestedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED
}

func (d CancelRequestedEventDefinition) Apply(root *hsm.Node, event *historypb.HistoryEvent) error {
	_, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
		return o.Cancel(node, event.EventTime.AsTime())
	})

	return err
}

func (d CancelRequestedEventDefinition) CherryPick(root *hsm.Node, event *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// We never cherry pick command events, and instead allow user logic to reschedule those commands.
	return hsm.ErrNotCherryPickable
}

type StartedEventDefinition struct{}

func (d StartedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d StartedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED
}

func (d StartedEventDefinition) Apply(root *hsm.Node, event *historypb.HistoryEvent) error {
	_, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
		return TransitionStarted.Apply(o, EventStarted{
			Time:       event.EventTime.AsTime(),
			Node:       node,
			Attributes: event.GetNexusOperationStartedEventAttributes(),
		})
	})

	return err
}

func (d StartedEventDefinition) CherryPick(root *hsm.Node, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return hsm.ErrNotCherryPickable
	}
	return d.Apply(root, event)
}

type CompletedEventDefinition struct{}

func (d CompletedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d CompletedEventDefinition) Apply(root *hsm.Node, event *historypb.HistoryEvent) error {
	node, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
		return TransitionSucceeded.Apply(o, EventSucceeded{
			Time: event.EventTime.AsTime(),
			Node: node,
		})
	})
	if err != nil {
		return err
	}

	return root.DeleteChild(node.Key)
}

func (d CompletedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED
}

func (d CompletedEventDefinition) CherryPick(root *hsm.Node, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return hsm.ErrNotCherryPickable
	}
	return d.Apply(root, event)
}

type FailedEventDefinition struct{}

func (d FailedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d FailedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED
}

func (d FailedEventDefinition) Apply(root *hsm.Node, event *historypb.HistoryEvent) error {
	node, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
		return TransitionFailed.Apply(o, EventFailed{
			Time:       event.EventTime.AsTime(),
			Attributes: event.GetNexusOperationFailedEventAttributes(),
			Node:       node,
		})
	})
	if err != nil {
		return err
	}

	return root.DeleteChild(node.Key)
}

func (d FailedEventDefinition) CherryPick(root *hsm.Node, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return hsm.ErrNotCherryPickable
	}
	return d.Apply(root, event)
}

type CanceledEventDefinition struct{}

func (d CanceledEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d CanceledEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED
}

func (d CanceledEventDefinition) Apply(root *hsm.Node, event *historypb.HistoryEvent) error {
	node, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
		return TransitionCanceled.Apply(o, EventCanceled{
			Time: event.EventTime.AsTime(),
			Node: node,
		})
	})
	if err != nil {
		return err
	}

	return root.DeleteChild(node.Key)
}

func (d CanceledEventDefinition) CherryPick(root *hsm.Node, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return hsm.ErrNotCherryPickable
	}
	return d.Apply(root, event)
}

type TimedOutEventDefinition struct{}

func (d TimedOutEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d TimedOutEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT
}

func (d TimedOutEventDefinition) Apply(root *hsm.Node, event *historypb.HistoryEvent) error {
	node, err := transitionOperation(root, event, func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error) {
		return TransitionTimedOut.Apply(o, EventTimedOut{
			Node: node,
		})
	})
	if err != nil {
		return err
	}

	return root.DeleteChild(node.Key)
}

func (d TimedOutEventDefinition) CherryPick(root *hsm.Node, event *historypb.HistoryEvent, excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error {
	if _, ok := excludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS]; ok {
		return hsm.ErrNotCherryPickable
	}
	return d.Apply(root, event)
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

func transitionOperation(
	root *hsm.Node,
	event *historypb.HistoryEvent,
	fn func(node *hsm.Node, o Operation) (hsm.TransitionOutput, error),
) (*hsm.Node, error) {
	node, err := findOperationNode(root, event)
	if err != nil {
		return nil, err
	}
	if err := hsm.MachineTransition(node, func(o Operation) (hsm.TransitionOutput, error) {
		return fn(node, o)
	}); err != nil {
		return nil, err
	}
	return node, nil
}

func findOperationNode(root *hsm.Node, event *historypb.HistoryEvent) (*hsm.Node, error) {
	attrs := reflect.ValueOf(event.Attributes).Elem()

	// Attributes is always a struct with a single field (e.g: HistoryEvent_NexusOperationScheduledEventAttributes)
	if attrs.Kind() != reflect.Struct || attrs.NumField() != 1 {
		panic("invalid event, expected Attributes field with a single field struct")
	}

	f := attrs.Field(0).Interface()

	eventIDGetter, ok := f.(interface{ GetScheduledEventId() int64 })
	if !ok {
		panic("Event does not have a ScheduledEventId field")
	}
	coll := MachineCollection(root)
	nodeID := strconv.FormatInt(eventIDGetter.GetScheduledEventId(), 10)
	node, err := coll.Node(nodeID)
	if err != nil {
		return nil, err
	}
	requestIDGetter, ok := f.(interface{ GetRequestId() string })
	if ok && requestIDGetter.GetRequestId() != "" {
		op, err := coll.Data(nodeID)
		if err != nil {
			return nil, err
		}
		if op.RequestId != requestIDGetter.GetRequestId() {
			return nil, fmt.Errorf("%w: event has different request ID (%q) than the machine (%q)",
				hsm.ErrNotCherryPickable, requestIDGetter.GetRequestId(), op.RequestId)
		}
	}
	return node, nil
}
