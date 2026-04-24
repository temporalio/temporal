package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
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
	}, chasmworkflow.CommandHandlerOptions{WorkflowTaskCompletedEventID: 1})
	require.NoError(t, err)
	require.NotEmpty(t, tcx.history.Events)
	event := tcx.history.Events[len(tcx.history.Events)-1]
	return event, event.EventId
}

func applyStartedEvent(t *testing.T, tcx testContext, scheduledEventID int64, eventTime time.Time) {
	t.Helper()
	applyEventDefinition(t, tcx, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, &historypb.HistoryEvent{
		EventTime: timestamppb.New(eventTime),
		Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
			NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
				ScheduledEventId: scheduledEventID,
				OperationToken:   "token",
			},
		},
	})
}

func applyEventDefinition(
	t *testing.T,
	tcx testContext,
	eventType enumspb.EventType,
	event *historypb.HistoryEvent,
) {
	t.Helper()
	chReg := chasmworkflow.NewRegistry()
	require.NoError(t, chReg.Register(newLibrary(defaultConfig, chasm.NewNexusEndpointProcessor())))
	def, ok := chReg.EventDefinitionByEventType(eventType)
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

		nexusEventDefs := []chasmworkflow.EventDefinition{
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
			require.ErrorIs(t, err, chasmworkflow.ErrEventNotCherryPickable,
				"%T should not be cherry-pickable when RESET_REAPPLY_EXCLUDE_TYPE_NEXUS is set", def)
		}
	})

	t.Run("scheduled is never cherry-pickable", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		event, _ := scheduleOperation(t, tcx)

		def := ScheduledEventDefinition{}
		err := def.CherryPick(tcx.chasmCtx, tcx.wf, event, nil)
		require.ErrorIs(t, err, chasmworkflow.ErrEventNotCherryPickable)
	})

	t.Run("cancel requested is never cherry-pickable", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		event, _ := scheduleOperation(t, tcx)

		def := CancelRequestedEventDefinition{}
		err := def.CherryPick(tcx.chasmCtx, tcx.wf, event, nil)
		require.ErrorIs(t, err, chasmworkflow.ErrEventNotCherryPickable)
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
		applyEventDefinition(t, tcx, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, buildEvent(scheduledEvent.EventId))
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
		applyEventDefinition(t, tcx, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, buildEvent(scheduledEvent.EventId))
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
		applyEventDefinition(t, tcx, enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, buildEvent(scheduledEvent.EventId))
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
		applyEventDefinition(t, tcx, enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, buildEvent(scheduledEvent.EventId))
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
		applyEventDefinition(t, tcx, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED, buildEvent(scheduledEvent.EventId))
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
		applyEventDefinition(t, tcx, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED, buildEvent(scheduledEvent.EventId))
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
		applyEventDefinition(t, tcx, enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, buildEvent(scheduledEvent.EventId))
		assertTerminalEventApplied(t, tcx, key, op.OperationState, nexusoperationpb.OPERATION_STATUS_TIMED_OUT)
	})

	t.Run("with started event", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		scheduledEvent, key := scheduleOperation(t, tcx)
		field, ok := tcx.wf.Operations[key]
		require.True(t, ok)
		op := field.Get(tcx.chasmCtx)
		applyStartedEvent(t, tcx, scheduledEvent.EventId, eventTime) // add start event first
		applyEventDefinition(t, tcx, enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, buildEvent(scheduledEvent.EventId))
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

	applyEventDefinition(t, tcx, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, event)

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

	applyEventDefinition(t, tcx, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, &historypb.HistoryEvent{
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

		def := CancelRequestedEventDefinition{}
		err := def.Apply(tcx.chasmCtx, tcx.wf, &historypb.HistoryEvent{
			EventId:   int64(20),
			EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestedEventAttributes{
				NexusOperationCancelRequestedEventAttributes: &historypb.NexusOperationCancelRequestedEventAttributes{
					ScheduledEventId: event.EventId,
				},
			},
		})
		require.NoError(t, err)

		field, ok := tcx.wf.Operations[key]
		require.True(t, ok)
		op := field.Get(tcx.chasmCtx)
		_, hasCancellation := op.Cancellation.TryGet(tcx.chasmCtx)
		require.True(t, hasCancellation)
	})

	t.Run("tolerates missing operation", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)

		def := CancelRequestedEventDefinition{}
		err := def.Apply(tcx.chasmCtx, tcx.wf, &historypb.HistoryEvent{
			EventId:   int64(20),
			EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestedEventAttributes{
				NexusOperationCancelRequestedEventAttributes: &historypb.NexusOperationCancelRequestedEventAttributes{
					ScheduledEventId: 999, // non-existent
				},
			},
		})
		require.NoError(t, err)
	})
}

func TestCancelRequestCompletedEventDefinitionApply(t *testing.T) {
	tcx := newTestContext(t, defaultConfig)
	event, key := scheduleOperation(t, tcx)

	// First, request cancellation.
	cancelDef := CancelRequestedEventDefinition{}
	err := cancelDef.Apply(tcx.chasmCtx, tcx.wf, &historypb.HistoryEvent{
		EventId:   int64(20),
		EventTime: timestamppb.Now(),
		Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestedEventAttributes{
			NexusOperationCancelRequestedEventAttributes: &historypb.NexusOperationCancelRequestedEventAttributes{
				ScheduledEventId: event.EventId,
			},
		},
	})
	require.NoError(t, err)

	// Transition the operation to STARTED so the cancellation gets scheduled.
	startDef := StartedEventDefinition{}
	err = startDef.Apply(tcx.chasmCtx, tcx.wf, &historypb.HistoryEvent{
		EventTime: timestamppb.Now(),
		Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
			NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
				ScheduledEventId: event.EventId,
				OperationToken:   "token",
			},
		},
	})
	require.NoError(t, err)

	// Now complete the cancel request.
	completedDef := CancelRequestCompletedEventDefinition{}
	err = completedDef.Apply(tcx.chasmCtx, tcx.wf, &historypb.HistoryEvent{
		EventTime: timestamppb.Now(),
		Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestCompletedEventAttributes{
			NexusOperationCancelRequestCompletedEventAttributes: &historypb.NexusOperationCancelRequestCompletedEventAttributes{
				ScheduledEventId: event.EventId,
			},
		},
	})
	require.NoError(t, err)

	field, ok := tcx.wf.Operations[key]
	require.True(t, ok)
	op := field.Get(tcx.chasmCtx)
	cancellation, hasCancellation := op.Cancellation.TryGet(tcx.chasmCtx)
	require.True(t, hasCancellation)
	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED, cancellation.StateMachineState())
}
