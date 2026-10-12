package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// scheduleOperation is a helper that schedules a nexus operation via the command handler
// and returns the scheduled event and its scheduled event ID (used as the operation key).
func scheduleOperation(t *testing.T, tcx testContext) (*historypb.HistoryEvent, int64) {
	t.Helper()
	err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
		Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
			ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
				Endpoint:               "endpoint",
				Service:                "service",
				Operation:              "op",
				ScheduleToCloseTimeout: durationpb.New(time.Hour),
			},
		},
	}, CommandHandlerOptions{WorkflowTaskCompletedEventID: 1})
	require.NoError(t, err)
	require.NotEmpty(t, tcx.history.Events)
	event := tcx.history.Events[len(tcx.history.Events)-1]
	return event, event.EventId
}

func applyStartedEvent(t *testing.T, tcx testContext, scheduledEventID int64, eventTime time.Time) {
	t.Helper()
	applyEventDefinition[StartedEventDefinition](t, tcx, &historypb.HistoryEvent{
		EventTime: timestamppb.New(eventTime),
		Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
			NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
				ScheduledEventId: scheduledEventID,
				OperationToken:   "token",
			},
		},
	})
}

func applyEventDefinition[D EventDefinition](
	t *testing.T,
	tcx testContext,
	event *historypb.HistoryEvent,
) {
	t.Helper()
	def, ok := eventDefinitionByGoType[D](tcx.registry)
	require.True(t, ok)
	err := def.Apply(tcx.chasmCtx, tcx.wf, event)
	require.NoError(t, err)
}

func assertTerminalEventApplied(
	t *testing.T,
	tcx testContext,
	key int64,
	op *nexusoperationpb.OperationState,
	expectedStatus nexusoperationpb.OperationStatus,
) {
	t.Helper()
	require.Equal(t, expectedStatus, op.GetStatus())
	_, ok := tcx.wf.Operations[key]
	require.False(t, ok, "operation should be removed after terminal event")
}

func TestCherryPick(t *testing.T) {
	t.Run("should exclude nexus events", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		event, _ := scheduleOperation(t, tcx)

		nexusEventDefs := []EventDefinition{
			ScheduledEventDefinition{},
			StartedEventDefinition{},
			CompletedEventDefinition{},
			CancelRequestedEventDefinition{},
			CancelRequestCompletedEventDefinition{},
			CancelRequestFailedEventDefinition{},
			CanceledEventDefinition{},
			FailedEventDefinition{},
			TimedOutEventDefinition{},
		}

		excludeNexus := map[enumspb.ResetReapplyExcludeType]struct{}{
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS: {},
		}
		for _, def := range nexusEventDefs {
			err := def.CherryPick(tcx.chasmCtx, tcx.wf, event, excludeNexus)
			require.ErrorIs(t, err, ErrEventNotCherryPickable,
				"%T should not be cherry-pickable when RESET_REAPPLY_EXCLUDE_TYPE_NEXUS is set", def)
		}
	})

	t.Run("scheduled is never cherry-pickable", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		event, _ := scheduleOperation(t, tcx)

		def := ScheduledEventDefinition{}
		err := def.CherryPick(tcx.chasmCtx, tcx.wf, event, nil)
		require.ErrorIs(t, err, ErrEventNotCherryPickable)
	})

	t.Run("cancel requested is never cherry-pickable", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		event, _ := scheduleOperation(t, tcx)

		def := CancelRequestedEventDefinition{}
		err := def.CherryPick(tcx.chasmCtx, tcx.wf, event, nil)
		require.ErrorIs(t, err, ErrEventNotCherryPickable)
	})

	t.Run("started cherry-pick applies", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		event, _ := scheduleOperation(t, tcx)

		def := StartedEventDefinition{}
		err := def.CherryPick(tcx.chasmCtx, tcx.wf, &historypb.HistoryEvent{
			EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
				NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
					ScheduledEventId: event.EventId,
				},
			},
		}, nil)
		require.NoError(t, err)
	})

	t.Run("started double apply fails", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		event, _ := scheduleOperation(t, tcx)

		def := StartedEventDefinition{}
		startedEvent := &historypb.HistoryEvent{
			EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
				NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
					ScheduledEventId: event.EventId,
				},
			},
		}
		err := def.CherryPick(tcx.chasmCtx, tcx.wf, startedEvent, nil)
		require.NoError(t, err)
		// Second apply should fail — operation is already in STARTED state.
		err = def.CherryPick(tcx.chasmCtx, tcx.wf, startedEvent, nil)
		require.Error(t, err)
	})
}

func TestCompletedEventDefinitionApply(t *testing.T) {
	eventTime := time.Now().UTC()
	buildEvent := func(scheduledEventID int64) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{
			EventTime: timestamppb.New(eventTime),
			Attributes: &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
				NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{
					ScheduledEventId: scheduledEventID,
				},
			},
		}
	}

	t.Run("without started event", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		scheduledEvent, key := scheduleOperation(t, tcx)
		field, ok := tcx.wf.Operations[key]
		require.True(t, ok)
		op := field.Get(tcx.chasmCtx)
		// no start event
		applyEventDefinition[CompletedEventDefinition](t, tcx, buildEvent(scheduledEvent.EventId))
		assertTerminalEventApplied(t, tcx, key, op.OperationState, nexusoperationpb.OPERATION_STATUS_SUCCEEDED)
		require.Equal(t, eventTime, op.GetClosedTime().AsTime())
	})

	t.Run("with started event", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		scheduledEvent, key := scheduleOperation(t, tcx)
		field, ok := tcx.wf.Operations[key]
		require.True(t, ok)
		op := field.Get(tcx.chasmCtx)
		applyStartedEvent(t, tcx, scheduledEvent.EventId, eventTime) // add start event firsts
		applyEventDefinition[CompletedEventDefinition](t, tcx, buildEvent(scheduledEvent.EventId))
		assertTerminalEventApplied(t, tcx, key, op.OperationState, nexusoperationpb.OPERATION_STATUS_SUCCEEDED)
		require.Equal(t, eventTime, op.GetClosedTime().AsTime())
	})
}

func TestFailedEventDefinitionApply(t *testing.T) {
	eventTime := time.Now().UTC()
	buildEvent := func(scheduledEventID int64) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{
			EventTime: timestamppb.New(eventTime),
			Attributes: &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
				NexusOperationFailedEventAttributes: &historypb.NexusOperationFailedEventAttributes{
					ScheduledEventId: scheduledEventID,
					Failure: &failurepb.Failure{
						Message: "nexus operation failed",
						Cause:   &failurepb.Failure{Message: "operation failed"},
					},
				},
			},
		}
	}

	t.Run("without started event", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		scheduledEvent, key := scheduleOperation(t, tcx)
		field, ok := tcx.wf.Operations[key]
		require.True(t, ok)
		op := field.Get(tcx.chasmCtx)
		// no start event
		applyEventDefinition[FailedEventDefinition](t, tcx, buildEvent(scheduledEvent.EventId))
		assertTerminalEventApplied(t, tcx, key, op.OperationState, nexusoperationpb.OPERATION_STATUS_FAILED)
		require.Equal(t, eventTime, op.GetClosedTime().AsTime())
	})

	t.Run("with started event", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		scheduledEvent, key := scheduleOperation(t, tcx)
		field, ok := tcx.wf.Operations[key]
		require.True(t, ok)
		op := field.Get(tcx.chasmCtx)
		applyStartedEvent(t, tcx, scheduledEvent.EventId, eventTime) // add start event first
		applyEventDefinition[FailedEventDefinition](t, tcx, buildEvent(scheduledEvent.EventId))
		assertTerminalEventApplied(t, tcx, key, op.OperationState, nexusoperationpb.OPERATION_STATUS_FAILED)
		require.Equal(t, eventTime, op.GetClosedTime().AsTime())
	})
}

func TestCanceledEventDefinitionApply(t *testing.T) {
	eventTime := time.Now().UTC()
	buildEvent := func(scheduledEventID int64) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{
			EventTime: timestamppb.New(eventTime),
			Attributes: &historypb.HistoryEvent_NexusOperationCanceledEventAttributes{
				NexusOperationCanceledEventAttributes: &historypb.NexusOperationCanceledEventAttributes{
					ScheduledEventId: scheduledEventID,
					Failure: &failurepb.Failure{
						Message: "nexus operation canceled",
						Cause:   &failurepb.Failure{Message: "operation canceled"},
					},
				},
			},
		}
	}

	t.Run("without started event", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		scheduledEvent, key := scheduleOperation(t, tcx)
		field, ok := tcx.wf.Operations[key]
		require.True(t, ok)
		op := field.Get(tcx.chasmCtx)
		// no start event
		applyEventDefinition[CanceledEventDefinition](t, tcx, buildEvent(scheduledEvent.EventId))
		assertTerminalEventApplied(t, tcx, key, op.OperationState, nexusoperationpb.OPERATION_STATUS_CANCELED)
		require.Equal(t, eventTime, op.GetClosedTime().AsTime())
	})

	t.Run("with started event", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		scheduledEvent, key := scheduleOperation(t, tcx)
		field, ok := tcx.wf.Operations[key]
		require.True(t, ok)
		op := field.Get(tcx.chasmCtx)
		applyStartedEvent(t, tcx, scheduledEvent.EventId, eventTime) // add start event first
		applyEventDefinition[CanceledEventDefinition](t, tcx, buildEvent(scheduledEvent.EventId))
		assertTerminalEventApplied(t, tcx, key, op.OperationState, nexusoperationpb.OPERATION_STATUS_CANCELED)
		require.Equal(t, eventTime, op.GetClosedTime().AsTime())
	})
}

func TestTimedOutEventDefinitionApply(t *testing.T) {
	eventTime := time.Now().UTC()
	buildEvent := func(scheduledEventID int64) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{
			EventTime: timestamppb.New(eventTime),
			Attributes: &historypb.HistoryEvent_NexusOperationTimedOutEventAttributes{
				NexusOperationTimedOutEventAttributes: &historypb.NexusOperationTimedOutEventAttributes{
					ScheduledEventId: scheduledEventID,
					Failure: &failurepb.Failure{
						Message: "nexus operation timed out",
						Cause:   &failurepb.Failure{Message: "operation timed out"},
					},
				},
			},
		}
	}

	t.Run("without started event", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		scheduledEvent, key := scheduleOperation(t, tcx)
		field, ok := tcx.wf.Operations[key]
		require.True(t, ok)
		op := field.Get(tcx.chasmCtx)
		// no start event
		applyEventDefinition[TimedOutEventDefinition](t, tcx, buildEvent(scheduledEvent.EventId))
		assertTerminalEventApplied(t, tcx, key, op.OperationState, nexusoperationpb.OPERATION_STATUS_TIMED_OUT)
	})

	t.Run("with started event", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		scheduledEvent, key := scheduleOperation(t, tcx)
		field, ok := tcx.wf.Operations[key]
		require.True(t, ok)
		op := field.Get(tcx.chasmCtx)
		applyStartedEvent(t, tcx, scheduledEvent.EventId, eventTime) // add start event first
		applyEventDefinition[TimedOutEventDefinition](t, tcx, buildEvent(scheduledEvent.EventId))
		assertTerminalEventApplied(t, tcx, key, op.OperationState, nexusoperationpb.OPERATION_STATUS_TIMED_OUT)
	})
}

func TestScheduledEventDefinitionApply(t *testing.T) {
	tcx := newTestContext(t, defaultConfig)

	event := &historypb.HistoryEvent{
		EventId:   int64(10),
		EventTime: timestamppb.Now(),
		Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
			NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
				Endpoint:                     "endpoint",
				EndpointId:                   "endpoint-id",
				Service:                      "service",
				Operation:                    "op",
				ScheduleToCloseTimeout:       durationpb.New(time.Hour),
				RequestId:                    "request-id",
				WorkflowTaskCompletedEventId: 1,
			},
		},
	}

	applyEventDefinition[ScheduledEventDefinition](t, tcx, event)

	field, ok := tcx.wf.Operations[event.EventId]
	require.True(t, ok)
	op := field.Get(tcx.chasmCtx)
	require.Equal(t, "endpoint", op.GetEndpoint())
	require.Equal(t, "endpoint-id", op.GetEndpointId())
	require.Equal(t, "service", op.GetService())
	require.Equal(t, "op", op.GetOperation())
	require.Equal(t, "request-id", op.GetRequestId())
	require.Equal(t, int32(1), op.GetAttempt())
}

func TestStartedEventDefinitionApply(t *testing.T) {
	tcx := newTestContext(t, defaultConfig)
	event, key := scheduleOperation(t, tcx)
	startTime := time.Now().UTC()

	applyEventDefinition[StartedEventDefinition](t, tcx, &historypb.HistoryEvent{
		EventTime: timestamppb.New(startTime),
		Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
			NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
				ScheduledEventId: event.EventId,
				OperationToken:   "test-token",
			},
		},
	})

	field, ok := tcx.wf.Operations[key]
	require.True(t, ok)
	op := field.Get(tcx.chasmCtx)
	require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, op.Status)
	require.Equal(t, "test-token", op.GetOperationToken())
	require.Equal(t, startTime, op.GetStartedTime().AsTime())
}

func TestCancelRequestedEventDefinitionApply(t *testing.T) {
	t.Run("creates cancellation child", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		event, key := scheduleOperation(t, tcx)
		requestedTime := time.Now().UTC()

		applyEventDefinition[CancelRequestedEventDefinition](t, tcx, &historypb.HistoryEvent{
			EventId:   int64(20),
			EventTime: timestamppb.New(requestedTime),
			Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestedEventAttributes{
				NexusOperationCancelRequestedEventAttributes: &historypb.NexusOperationCancelRequestedEventAttributes{
					ScheduledEventId: event.EventId,
				},
			},
		})

		field, ok := tcx.wf.Operations[key]
		require.True(t, ok)
		op := field.Get(tcx.chasmCtx)
		cancellation, hasCancellation := op.Cancellation.TryGet(tcx.chasmCtx)
		require.True(t, hasCancellation)
		require.Equal(t, requestedTime, cancellation.GetRequestedTime().AsTime())
	})

	t.Run("tolerates missing operation", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)

		applyEventDefinition[CancelRequestedEventDefinition](t, tcx, &historypb.HistoryEvent{
			EventId:   int64(20),
			EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestedEventAttributes{
				NexusOperationCancelRequestedEventAttributes: &historypb.NexusOperationCancelRequestedEventAttributes{
					ScheduledEventId: 999, // non-existent
				},
			},
		})
	})
}

func TestCancelRequestCompletedEventDefinitionApply(t *testing.T) {
	tcx := newTestContext(t, defaultConfig)
	event, key := scheduleOperation(t, tcx)

	// First, request cancellation.
	applyEventDefinition[CancelRequestedEventDefinition](t, tcx, &historypb.HistoryEvent{
		EventId:   int64(20),
		EventTime: timestamppb.Now(),
		Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestedEventAttributes{
			NexusOperationCancelRequestedEventAttributes: &historypb.NexusOperationCancelRequestedEventAttributes{
				ScheduledEventId: event.EventId,
			},
		},
	})

	// Transition the operation to STARTED so the cancellation gets scheduled.
	applyEventDefinition[StartedEventDefinition](t, tcx, &historypb.HistoryEvent{
		EventTime: timestamppb.Now(),
		Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
			NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
				ScheduledEventId: event.EventId,
				OperationToken:   "token",
			},
		},
	})

	// Now complete the cancel request.
	applyEventDefinition[CancelRequestCompletedEventDefinition](t, tcx, &historypb.HistoryEvent{
		EventTime: timestamppb.Now(),
		Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestCompletedEventAttributes{
			NexusOperationCancelRequestCompletedEventAttributes: &historypb.NexusOperationCancelRequestCompletedEventAttributes{
				ScheduledEventId: event.EventId,
			},
		},
	})

	field, ok := tcx.wf.Operations[key]
	require.True(t, ok)
	op := field.Get(tcx.chasmCtx)
	cancellation, hasCancellation := op.Cancellation.TryGet(tcx.chasmCtx)
	require.True(t, hasCancellation)
	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED, cancellation.StateMachineState())
}

// TestNexusEventDefinitionsReportMissingOperation verifies that Apply reports a serviceerror.NotFound when the
// event's scheduled event ID has no entry in the workflow's Operations map.
func TestNexusEventDefinitionsReportMissingOperation(t *testing.T) {
	const unknownScheduledEventID = int64(1234)

	testCases := []struct {
		name  string
		def   EventDefinition
		event *historypb.HistoryEvent
	}{
		{
			name: "cancel request completed",
			def:  CancelRequestCompletedEventDefinition{},
			event: &historypb.HistoryEvent{Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestCompletedEventAttributes{
				NexusOperationCancelRequestCompletedEventAttributes: &historypb.NexusOperationCancelRequestCompletedEventAttributes{
					ScheduledEventId: unknownScheduledEventID,
				},
			}},
		},
		{
			name: "cancel request failed",
			def:  CancelRequestFailedEventDefinition{},
			event: &historypb.HistoryEvent{Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestFailedEventAttributes{
				NexusOperationCancelRequestFailedEventAttributes: &historypb.NexusOperationCancelRequestFailedEventAttributes{
					ScheduledEventId: unknownScheduledEventID,
				},
			}},
		},
		{
			name: "started",
			def:  StartedEventDefinition{},
			event: &historypb.HistoryEvent{Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
				NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
					ScheduledEventId: unknownScheduledEventID,
					OperationToken:   "token",
				},
			}},
		},
		{
			name: "completed",
			def:  CompletedEventDefinition{},
			event: &historypb.HistoryEvent{Attributes: &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
				NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{
					ScheduledEventId: unknownScheduledEventID,
				},
			}},
		},
		{
			name: "failed",
			def:  FailedEventDefinition{},
			event: &historypb.HistoryEvent{Attributes: &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
				NexusOperationFailedEventAttributes: &historypb.NexusOperationFailedEventAttributes{
					ScheduledEventId: unknownScheduledEventID,
				},
			}},
		},
		{
			name: "canceled",
			def:  CanceledEventDefinition{},
			event: &historypb.HistoryEvent{Attributes: &historypb.HistoryEvent_NexusOperationCanceledEventAttributes{
				NexusOperationCanceledEventAttributes: &historypb.NexusOperationCanceledEventAttributes{
					ScheduledEventId: unknownScheduledEventID,
				},
			}},
		},
		{
			name: "timed out",
			def:  TimedOutEventDefinition{},
			event: &historypb.HistoryEvent{Attributes: &historypb.HistoryEvent_NexusOperationTimedOutEventAttributes{
				NexusOperationTimedOutEventAttributes: &historypb.NexusOperationTimedOutEventAttributes{
					ScheduledEventId: unknownScheduledEventID,
				},
			}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tcx := newTestContext(t, defaultConfig)
			// Schedule an unrelated operation so the lookup misses on the ID rather than on an empty tree --
			// the shape a forked replication branch actually produces.
			_, existingKey := scheduleOperation(t, tcx)
			require.NotEqual(t, unknownScheduledEventID, existingKey)

			err := tc.def.Apply(tcx.chasmCtx, tcx.wf, tc.event)

			require.ErrorAs(t, err, new(*serviceerror.NotFound),
				"could not get a NotFound error for a a missing operation")
			require.NotErrorIs(t, err, ErrEventNotCherryPickable,
				"a missing operation is not the same as an event that is not cherry-pickable")
		})
	}
}

// TestNexusEventDefinitionsReportInvalidTransition verifies that an event which cannot apply from the operation's
// current state reports chasm.ErrInvalidTransition.
func TestNexusEventDefinitionsReportInvalidTransition(t *testing.T) {
	tcx := newTestContext(t, defaultConfig)
	scheduledEvent, key := scheduleOperation(t, tcx)
	eventTime := time.Now().UTC()

	startedEvent := func() *historypb.HistoryEvent {
		return &historypb.HistoryEvent{
			EventTime: timestamppb.New(eventTime),
			Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
				NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
					ScheduledEventId: scheduledEvent.EventId,
					OperationToken:   "token",
				},
			},
		}
	}

	def, ok := eventDefinitionByGoType[StartedEventDefinition](tcx.registry)
	require.True(t, ok)

	// First Started moves the operation SCHEDULED -> STARTED.
	require.NoError(t, def.CherryPick(tcx.chasmCtx, tcx.wf, startedEvent(), nil))
	field, ok := tcx.wf.Operations[key]
	require.True(t, ok)
	require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, field.Get(tcx.chasmCtx).GetStatus())

	// A second Started is what a reapplied duplicate looks like. The operation is still in the tree, so this is a
	// state problem, not a missing-operation problem.
	err := def.CherryPick(tcx.chasmCtx, tcx.wf, startedEvent(), nil)

	require.ErrorIs(t, err, chasm.ErrInvalidTransition)
	require.NotErrorAs(t, err, new(*serviceerror.NotFound), "the operation is present; this is not a lookup miss")
	require.NotErrorIs(t, err, ErrEventNotCherryPickable)
}

// TestNexusEventDefinitionsRejectMismatchedRequestID verifies that every event definition carrying a request ID
// rejects an event naming a scheduled event ID held by a different operation.
func TestNexusEventDefinitionsRejectMismatchedRequestID(t *testing.T) {
	testCases := []struct {
		name  string
		def   EventDefinition
		event func(scheduledEventID int64, requestID string) *historypb.HistoryEvent
	}{
		{
			name: "started",
			def:  StartedEventDefinition{},
			event: func(scheduledEventID int64, requestID string) *historypb.HistoryEvent {
				return &historypb.HistoryEvent{
					EventTime: timestamppb.New(time.Now().UTC()),
					Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
						NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
							ScheduledEventId: scheduledEventID,
							RequestId:        requestID,
							OperationToken:   "token",
						},
					},
				}
			},
		},
		{
			name: "completed",
			def:  CompletedEventDefinition{},
			event: func(scheduledEventID int64, requestID string) *historypb.HistoryEvent {
				return &historypb.HistoryEvent{
					EventTime: timestamppb.New(time.Now().UTC()),
					Attributes: &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
						NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{
							ScheduledEventId: scheduledEventID,
							RequestId:        requestID,
						},
					},
				}
			},
		},
		{
			name: "failed",
			def:  FailedEventDefinition{},
			event: func(scheduledEventID int64, requestID string) *historypb.HistoryEvent {
				return &historypb.HistoryEvent{
					EventTime: timestamppb.New(time.Now().UTC()),
					Attributes: &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
						NexusOperationFailedEventAttributes: &historypb.NexusOperationFailedEventAttributes{
							ScheduledEventId: scheduledEventID,
							RequestId:        requestID,
							Failure:          &failurepb.Failure{Message: "failed"},
						},
					},
				}
			},
		},
		{
			name: "canceled",
			def:  CanceledEventDefinition{},
			event: func(scheduledEventID int64, requestID string) *historypb.HistoryEvent {
				return &historypb.HistoryEvent{
					EventTime: timestamppb.New(time.Now().UTC()),
					Attributes: &historypb.HistoryEvent_NexusOperationCanceledEventAttributes{
						NexusOperationCanceledEventAttributes: &historypb.NexusOperationCanceledEventAttributes{
							ScheduledEventId: scheduledEventID,
							RequestId:        requestID,
							Failure:          &failurepb.Failure{Message: "canceled"},
						},
					},
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("a foreign request ID is rejected and the operation is untouched", func(t *testing.T) {
				tcx := newTestContext(t, defaultConfig)
				scheduled, key := scheduleOperation(t, tcx)
				field, ok := tcx.wf.Operations[key]
				require.True(t, ok)

				err := tc.def.Apply(tcx.chasmCtx, tcx.wf, tc.event(scheduled.EventId, "another-operations-request-id"))

				require.ErrorIs(t, err, chasm.ErrInvalidTransition)
				require.ErrorContains(t, err, "does not match operation request ID",
					"must be rejected for identity, not because the transition was impossible from this state")
				require.Equal(t, nexusoperationpb.OPERATION_STATUS_SCHEDULED, field.Get(tcx.chasmCtx).GetStatus(),
					"the operation holding this ID must not be resolved by another operation's event")
			})

			t.Run("the operation's own request ID is applied", func(t *testing.T) {
				tcx := newTestContext(t, defaultConfig)
				scheduled, key := scheduleOperation(t, tcx)
				own := tcx.wf.Operations[key].Get(tcx.chasmCtx).GetRequestId()
				require.NotEmpty(t, own)

				require.NoError(t, tc.def.Apply(tcx.chasmCtx, tcx.wf, tc.event(scheduled.EventId, own)))
			})

			t.Run("an absent request ID skips the check", func(t *testing.T) {
				tcx := newTestContext(t, defaultConfig)
				scheduled, _ := scheduleOperation(t, tcx)

				require.NoError(t, tc.def.Apply(tcx.chasmCtx, tcx.wf, tc.event(scheduled.EventId, "")))
			})
		})
	}
}
