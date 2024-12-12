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

package nexusoperations_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCherryPick(t *testing.T) {
	setup := func(t *testing.T) (*hsm.Node, nexusoperations.Operation, int64) {
		node := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), time.Hour))
		op, err := hsm.MachineData[nexusoperations.Operation](node)
		require.NoError(t, err)
		eventID, err := hsm.EventIDFromToken(op.ScheduledEventToken)
		require.NoError(t, err)
		return node, op, eventID
	}

	t.Run("NoRequestID", func(t *testing.T) {
		node, _, eventID := setup(t)
		err := nexusoperations.StartedEventDefinition{}.CherryPick(node.Parent, &historypb.HistoryEvent{
			Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
				NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
					ScheduledEventId: eventID,
				},
			},
		},
			nil,
		)
		require.NoError(t, err)
	})
	t.Run("ValidRequestID", func(t *testing.T) {
		node, op, eventID := setup(t)
		err := nexusoperations.StartedEventDefinition{}.CherryPick(node.Parent, &historypb.HistoryEvent{
			Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
				NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
					ScheduledEventId: eventID,
					RequestId:        op.RequestId,
				},
			},
		},
			nil,
		)
		require.NoError(t, err)
	})
	t.Run("InvalidRequestID", func(t *testing.T) {
		node, _, eventID := setup(t)
		err := nexusoperations.StartedEventDefinition{}.CherryPick(node.Parent, &historypb.HistoryEvent{
			Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
				NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
					ScheduledEventId: eventID,
					RequestId:        "invalid",
				},
			},
		},
			nil,
		)
		require.ErrorIs(t, err, hsm.ErrNotCherryPickable)
	})
	t.Run("InvalidScheduledEventID", func(t *testing.T) {
		node, _, eventID := setup(t)
		err := nexusoperations.StartedEventDefinition{}.CherryPick(node.Parent, &historypb.HistoryEvent{
			Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
				NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
					ScheduledEventId: eventID + 1,
				},
			},
		},
			nil,
		)
		require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)
	})
	t.Run("DoubleApply", func(t *testing.T) {
		node, _, eventID := setup(t)
		err := nexusoperations.StartedEventDefinition{}.CherryPick(node.Parent, &historypb.HistoryEvent{
			Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
				NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
					ScheduledEventId: eventID,
				},
			},
		},
			nil,
		)
		require.NoError(t, err)
		err = nexusoperations.StartedEventDefinition{}.CherryPick(node.Parent, &historypb.HistoryEvent{
			Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
				NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
					ScheduledEventId: eventID,
				},
			},
		},
			nil,
		)
		require.ErrorIs(t, err, hsm.ErrInvalidTransition)
	})
	t.Run("shouldExcludeNexusEvents", func(t *testing.T) {
		node, _, _ := setup(t)
		nexusOperations := []hsm.EventDefinition{
			nexusoperations.ScheduledEventDefinition{},
			nexusoperations.StartedEventDefinition{},
			nexusoperations.CompletedEventDefinition{},
			nexusoperations.CancelRequestedEventDefinition{},
			nexusoperations.CanceledEventDefinition{},
			nexusoperations.FailedEventDefinition{},
			nexusoperations.TimedOutEventDefinition{},
		}

		excludeNexusOperation := map[enumspb.ResetReapplyExcludeType]struct{}{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS: {}}
		for _, nexusOperation := range nexusOperations {
			err := nexusOperation.CherryPick(node.Parent, &historypb.HistoryEvent{}, excludeNexusOperation)
			require.ErrorIs(t, err, hsm.ErrNotCherryPickable, "%T should not be cherrypickable when shouldExcludeNexusEvent=true", nexusOperation)
		}
	})
}

func TestTerminalStatesDeletion(t *testing.T) {
	setup := func(t *testing.T) (*hsm.Node, nexusoperations.Operation, int64) {
		node := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), time.Hour))
		op, err := hsm.MachineData[nexusoperations.Operation](node)
		require.NoError(t, err)
		eventID, err := hsm.EventIDFromToken(op.ScheduledEventToken)
		require.NoError(t, err)
		return node, op, eventID
	}

	applyEventAndCheckDeletion := func(
		t *testing.T,
		node *hsm.Node,
		eventID int64,
		def hsm.EventDefinition,
		attr interface{},
	) {
		event := &historypb.HistoryEvent{
			EventTime: timestamppb.Now(),
		}

		switch d := def.(type) {
		case nexusoperations.CompletedEventDefinition:
			event.EventType = d.Type()
			event.Attributes = &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
				NexusOperationCompletedEventAttributes: attr.(*historypb.NexusOperationCompletedEventAttributes),
			}
		case nexusoperations.FailedEventDefinition:
			event.EventType = d.Type()
			event.Attributes = &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
				NexusOperationFailedEventAttributes: attr.(*historypb.NexusOperationFailedEventAttributes),
			}
		case nexusoperations.CanceledEventDefinition:
			event.EventType = d.Type()
			event.Attributes = &historypb.HistoryEvent_NexusOperationCanceledEventAttributes{
				NexusOperationCanceledEventAttributes: attr.(*historypb.NexusOperationCanceledEventAttributes),
			}
		case nexusoperations.TimedOutEventDefinition:
			event.EventType = d.Type()
			event.Attributes = &historypb.HistoryEvent_NexusOperationTimedOutEventAttributes{
				NexusOperationTimedOutEventAttributes: attr.(*historypb.NexusOperationTimedOutEventAttributes),
			}
		default:
			t.Fatalf("unknown event definition type: %T", def)
		}

		err := def.Apply(node.Parent, event)
		require.NoError(t, err)

		coll := nexusoperations.MachineCollection(node.Parent)
		_, err = coll.Node(strconv.FormatInt(eventID, 10))
		require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)
	}

	testCases := []struct {
		name       string
		def        hsm.EventDefinition
		attributes interface{}
	}{
		{
			name: "CompletedDeletesStateMachine",
			def:  nexusoperations.CompletedEventDefinition{},
			attributes: &historypb.NexusOperationCompletedEventAttributes{
				ScheduledEventId: 0,
			},
		},
		{
			name: "FailedDeletesStateMachine",
			def:  nexusoperations.FailedEventDefinition{},
			attributes: &historypb.NexusOperationFailedEventAttributes{
				ScheduledEventId: 0,
			},
		},
		{
			name: "CanceledDeletesStateMachine",
			def:  nexusoperations.CanceledEventDefinition{},
			attributes: &historypb.NexusOperationCanceledEventAttributes{
				ScheduledEventId: 0,
			},
		},
		{
			name: "TimedOutDeletesStateMachine",
			def:  nexusoperations.TimedOutEventDefinition{},
			attributes: &historypb.NexusOperationTimedOutEventAttributes{
				ScheduledEventId: 0,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node, _, eventID := setup(t)

			// Update the event ID in attributes
			switch a := tc.attributes.(type) {
			case *historypb.NexusOperationCompletedEventAttributes:
				a.ScheduledEventId = eventID
			case *historypb.NexusOperationFailedEventAttributes:
				a.ScheduledEventId = eventID
			case *historypb.NexusOperationCanceledEventAttributes:
				a.ScheduledEventId = eventID
			case *historypb.NexusOperationTimedOutEventAttributes:
				a.ScheduledEventId = eventID
			}

			applyEventAndCheckDeletion(t, node, eventID, tc.def, tc.attributes)
		})
	}
}
