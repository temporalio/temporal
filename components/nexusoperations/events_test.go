package nexusoperations_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCherryPick(t *testing.T) {
	setup := func(t *testing.T) (*hsm.Node, nexusoperations.Operation, int64) {
		node := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
			ScheduleToCloseTimeout: durationpb.New(time.Hour),
		}))
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

// TestCallerMetricsNotEmittedOnReapply is the replay-safety evidence for the "emit inside the
// transition" approach: re-applying a terminal event through the registered event definition (the
// path replication and workflow reset use) must NOT emit caller-side metrics. The registered
// definition carries no metrics hook, so the transition stays silent; emission happens only on the
// live executor/completion path, which sets the hook. The live emission is covered by the executor
// and completion tests, so together they show emit-once-on-live, zero-on-replay.
func TestCallerMetricsNotEmittedOnReapply(t *testing.T) {
	testCases := []struct {
		name       string
		def        hsm.EventDefinition
		makeEvent  func(eventID int64) *historypb.HistoryEvent
		metricName string
	}{
		{
			name: "completed",
			def:  nexusoperations.CompletedEventDefinition{},
			makeEvent: func(eventID int64) *historypb.HistoryEvent {
				return &historypb.HistoryEvent{
					EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
					EventTime: timestamppb.Now(),
					Attributes: &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
						NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{ScheduledEventId: eventID},
					},
				}
			},
			metricName: chasmnexus.NexusOperationSuccessCount.Name(),
		},
		{
			name: "failed",
			def:  nexusoperations.FailedEventDefinition{},
			makeEvent: func(eventID int64) *historypb.HistoryEvent {
				return &historypb.HistoryEvent{
					EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
					EventTime: timestamppb.Now(),
					Attributes: &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
						NexusOperationFailedEventAttributes: &historypb.NexusOperationFailedEventAttributes{ScheduledEventId: eventID},
					},
				}
			},
			metricName: chasmnexus.NexusOperationFailedCount.Name(),
		},
		{
			name: "canceled",
			def:  nexusoperations.CanceledEventDefinition{},
			makeEvent: func(eventID int64) *historypb.HistoryEvent {
				return &historypb.HistoryEvent{
					EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
					EventTime: timestamppb.Now(),
					Attributes: &historypb.HistoryEvent_NexusOperationCanceledEventAttributes{
						NexusOperationCanceledEventAttributes: &historypb.NexusOperationCanceledEventAttributes{ScheduledEventId: eventID},
					},
				}
			},
			metricName: chasmnexus.NexusOperationCancelCount.Name(),
		},
		{
			name: "timedout",
			def:  nexusoperations.TimedOutEventDefinition{},
			makeEvent: func(eventID int64) *historypb.HistoryEvent {
				return &historypb.HistoryEvent{
					EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
					EventTime: timestamppb.Now(),
					Attributes: &historypb.HistoryEvent_NexusOperationTimedOutEventAttributes{
						NexusOperationTimedOutEventAttributes: &historypb.NexusOperationTimedOutEventAttributes{ScheduledEventId: eventID},
					},
				}
			},
			metricName: chasmnexus.NexusOperationTimeoutCount.Name(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
				ScheduleToCloseTimeout: durationpb.New(time.Hour),
			}))
			op, err := hsm.MachineData[nexusoperations.Operation](node)
			require.NoError(t, err)
			eventID, err := hsm.EventIDFromToken(op.ScheduledEventToken)
			require.NoError(t, err)

			captureHandler := metricstest.NewCaptureHandler()
			capture := captureHandler.StartCapture()
			defer captureHandler.StopCapture(capture)

			// Apply via the registered definition (nil hook) — the replication/reset re-apply path.
			require.NoError(t, tc.def.Apply(node.Parent, tc.makeEvent(eventID)))

			snapshot := capture.Snapshot()
			require.Empty(t, snapshot[tc.metricName], "terminal counter must not be emitted on re-apply")
			require.Empty(t, snapshot[chasmnexus.NexusOperationScheduleToCloseLatency.Name()], "latency must not be emitted on re-apply")
		})
	}
}

func TestTerminalStatesDeletion(t *testing.T) {
	testCases := []struct {
		name       string
		def        hsm.EventDefinition
		attributes any
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
			node := newOperationNode(t, &hsmtest.NodeBackend{}, mustNewScheduledEvent(time.Now(), &historypb.NexusOperationScheduledEventAttributes{
				ScheduleToCloseTimeout: durationpb.New(time.Hour),
			}))
			op, err := hsm.MachineData[nexusoperations.Operation](node)
			require.NoError(t, err)
			eventID, err := hsm.EventIDFromToken(op.ScheduledEventToken)
			require.NoError(t, err)

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

			event := &historypb.HistoryEvent{
				EventTime: timestamppb.Now(),
			}

			switch d := tc.def.(type) {
			case nexusoperations.CompletedEventDefinition:
				event.EventType = d.Type()
				event.Attributes = &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
					NexusOperationCompletedEventAttributes: tc.attributes.(*historypb.NexusOperationCompletedEventAttributes),
				}
			case nexusoperations.FailedEventDefinition:
				event.EventType = d.Type()
				event.Attributes = &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
					NexusOperationFailedEventAttributes: tc.attributes.(*historypb.NexusOperationFailedEventAttributes),
				}
			case nexusoperations.CanceledEventDefinition:
				event.EventType = d.Type()
				event.Attributes = &historypb.HistoryEvent_NexusOperationCanceledEventAttributes{
					NexusOperationCanceledEventAttributes: tc.attributes.(*historypb.NexusOperationCanceledEventAttributes),
				}
			case nexusoperations.TimedOutEventDefinition:
				event.EventType = d.Type()
				event.Attributes = &historypb.HistoryEvent_NexusOperationTimedOutEventAttributes{
					NexusOperationTimedOutEventAttributes: tc.attributes.(*historypb.NexusOperationTimedOutEventAttributes),
				}
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
