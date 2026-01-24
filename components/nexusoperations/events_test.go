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
	"google.golang.org/protobuf/proto"
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
		err := nexusoperations.StartedEventDefinition{}.CherryPick(node.Parent, historypb.HistoryEvent_builder{
			NexusOperationStartedEventAttributes: historypb.NexusOperationStartedEventAttributes_builder{
				ScheduledEventId: eventID,
			}.Build(),
		}.Build(),
			nil,
		)
		require.NoError(t, err)
	})
	t.Run("ValidRequestID", func(t *testing.T) {
		node, op, eventID := setup(t)
		err := nexusoperations.StartedEventDefinition{}.CherryPick(node.Parent, historypb.HistoryEvent_builder{
			NexusOperationStartedEventAttributes: historypb.NexusOperationStartedEventAttributes_builder{
				ScheduledEventId: eventID,
				RequestId:        op.RequestId,
			}.Build(),
		}.Build(),
			nil,
		)
		require.NoError(t, err)
	})
	t.Run("InvalidRequestID", func(t *testing.T) {
		node, _, eventID := setup(t)
		err := nexusoperations.StartedEventDefinition{}.CherryPick(node.Parent, historypb.HistoryEvent_builder{
			NexusOperationStartedEventAttributes: historypb.NexusOperationStartedEventAttributes_builder{
				ScheduledEventId: eventID,
				RequestId:        "invalid",
			}.Build(),
		}.Build(),
			nil,
		)
		require.ErrorIs(t, err, hsm.ErrNotCherryPickable)
	})
	t.Run("InvalidScheduledEventID", func(t *testing.T) {
		node, _, eventID := setup(t)
		err := nexusoperations.StartedEventDefinition{}.CherryPick(node.Parent, historypb.HistoryEvent_builder{
			NexusOperationStartedEventAttributes: historypb.NexusOperationStartedEventAttributes_builder{
				ScheduledEventId: eventID + 1,
			}.Build(),
		}.Build(),
			nil,
		)
		require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)
	})
	t.Run("DoubleApply", func(t *testing.T) {
		node, _, eventID := setup(t)
		err := nexusoperations.StartedEventDefinition{}.CherryPick(node.Parent, historypb.HistoryEvent_builder{
			NexusOperationStartedEventAttributes: historypb.NexusOperationStartedEventAttributes_builder{
				ScheduledEventId: eventID,
			}.Build(),
		}.Build(),
			nil,
		)
		require.NoError(t, err)
		err = nexusoperations.StartedEventDefinition{}.CherryPick(node.Parent, historypb.HistoryEvent_builder{
			NexusOperationStartedEventAttributes: historypb.NexusOperationStartedEventAttributes_builder{
				ScheduledEventId: eventID,
			}.Build(),
		}.Build(),
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
	testCases := []struct {
		name       string
		def        hsm.EventDefinition
		attributes interface{}
	}{
		{
			name: "CompletedDeletesStateMachine",
			def:  nexusoperations.CompletedEventDefinition{},
			attributes: historypb.NexusOperationCompletedEventAttributes_builder{
				ScheduledEventId: 0,
			}.Build(),
		},
		{
			name: "FailedDeletesStateMachine",
			def:  nexusoperations.FailedEventDefinition{},
			attributes: historypb.NexusOperationFailedEventAttributes_builder{
				ScheduledEventId: 0,
			}.Build(),
		},
		{
			name: "CanceledDeletesStateMachine",
			def:  nexusoperations.CanceledEventDefinition{},
			attributes: historypb.NexusOperationCanceledEventAttributes_builder{
				ScheduledEventId: 0,
			}.Build(),
		},
		{
			name: "TimedOutDeletesStateMachine",
			def:  nexusoperations.TimedOutEventDefinition{},
			attributes: historypb.NexusOperationTimedOutEventAttributes_builder{
				ScheduledEventId: 0,
			}.Build(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), time.Hour))
			op, err := hsm.MachineData[nexusoperations.Operation](node)
			require.NoError(t, err)
			eventID, err := hsm.EventIDFromToken(op.ScheduledEventToken)
			require.NoError(t, err)

			// Update the event ID in attributes
			switch a := tc.attributes.(type) {
			case *historypb.NexusOperationCompletedEventAttributes:
				a.SetScheduledEventId(eventID)
			case *historypb.NexusOperationFailedEventAttributes:
				a.SetScheduledEventId(eventID)
			case *historypb.NexusOperationCanceledEventAttributes:
				a.SetScheduledEventId(eventID)
			case *historypb.NexusOperationTimedOutEventAttributes:
				a.SetScheduledEventId(eventID)
			}

			event := historypb.HistoryEvent_builder{
				EventTime: timestamppb.Now(),
			}.Build()

			switch d := tc.def.(type) {
			case nexusoperations.CompletedEventDefinition:
				event.SetEventType(d.Type())
				event.SetNexusOperationCompletedEventAttributes(proto.ValueOrDefault(tc.attributes.(*historypb.NexusOperationCompletedEventAttributes)))
			case nexusoperations.FailedEventDefinition:
				event.SetEventType(d.Type())
				event.SetNexusOperationFailedEventAttributes(proto.ValueOrDefault(tc.attributes.(*historypb.NexusOperationFailedEventAttributes)))
			case nexusoperations.CanceledEventDefinition:
				event.SetEventType(d.Type())
				event.SetNexusOperationCanceledEventAttributes(proto.ValueOrDefault(tc.attributes.(*historypb.NexusOperationCanceledEventAttributes)))
			case nexusoperations.TimedOutEventDefinition:
				event.SetEventType(d.Type())
				event.SetNexusOperationTimedOutEventAttributes(proto.ValueOrDefault(tc.attributes.(*historypb.NexusOperationTimedOutEventAttributes)))
			default:
				t.Fatalf("unknown event definition type: %T", tc.def)
			}

			err = tc.def.Apply(node.Parent, event)
			require.NoError(t, err)

			coll := nexusoperations.MachineCollection(node.Parent)
			_, err = coll.Node(strconv.FormatInt(eventID, 10))
			require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)
		})
	}
}
