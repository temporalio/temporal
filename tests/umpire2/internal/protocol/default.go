package protocol

import (
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/action"
	"go.temporal.io/server/tests/umpire2/internal/model"
)

// Default compiles the canonical Temporal protocol declaration.
func Default() (*Protocol, error) {
	return Compile(defaultDeclaration())
}

func defaultDeclaration() Declaration {
	entities := make([]EntityDeclaration, 0, len(model.DefaultEntities()))
	for _, registered := range model.DefaultEntities() {
		entityType := registered.New().Type()
		declaration := EntityDeclaration{Type: entityType, New: registered.New, Facts: registered.Facts}
		switch entityType {
		case model.WorkflowType:
			declaration.Actions = workflowActions()
			declaration.ActionGaps = workflowActionGaps()
		case model.NexusOperationType:
			declaration.Actions = nexusActions()
			declaration.ActionGaps = nexusActionGaps()
		default:
			declaration.Actions = nil
		}
		entities = append(entities, declaration)
	}
	return Declaration{
		Facts:            model.DefaultFacts(),
		Entities:         entities,
		Relations:        defaultRelationSchemas(),
		RelationDerivers: defaultRelationDerivers(),
		Regression:       defaultRegressionDomain(),
		CausalFootprints: defaultCausalFootprints(),
	}
}

func workflowActions() []ActionBinding {
	return []ActionBinding{
		bind(model.WorkflowType, model.WorkflowCreated, model.WorkflowStart, umpire.Standalone, action.StartWorkflow),
		bind(model.WorkflowType, model.WorkflowStarted, model.WorkflowComplete, umpire.Standalone, action.CompleteWorkflow),
	}
}

func workflowActionGaps() []ActionGap {
	return []ActionGap{
		gap(model.WorkflowType, model.WorkflowStarted, model.WorkflowFail, umpire.Standalone, "needs a workflow failure command driver"),
		gap(model.WorkflowType, model.WorkflowStarted, model.WorkflowCancel, umpire.Standalone, "needs a workflow cancellation command driver"),
		gap(model.WorkflowType, model.WorkflowStarted, model.WorkflowTerminate, umpire.Standalone, "needs a workflow termination RPC driver"),
		gap(model.WorkflowType, model.WorkflowStarted, model.WorkflowTimeout, umpire.Standalone, "server-driven timeout has no atomic action"),
	}
}

func nexusActions() []ActionBinding {
	asyncFailure := action.CompleteWith(
		&nexus.OperationError{
			State: nexus.OperationStateFailed,
			Cause: errors.New("umpire action: injected async failure"),
		},
		model.NexusFail,
	)
	asyncCancellation := action.CompleteWith(
		&nexus.OperationError{
			State: nexus.OperationStateCanceled,
			Cause: errors.New("umpire action: injected async cancellation"),
		},
		model.NexusCancel,
	)
	return []ActionBinding{
		bind(model.NexusOperationType, model.NexusUnspecified, model.NexusSchedule, umpire.Standalone, action.StartStandalone),
		bind(model.NexusOperationType, model.NexusUnspecified, model.NexusSchedule, umpire.Embedded, action.ScheduleEmbedded),
		bind(model.NexusOperationType, model.NexusBackingOff, model.NexusSchedule, umpire.Standalone, action.StartStandalone),
		bind(model.NexusOperationType, model.NexusBackingOff, model.NexusSchedule, umpire.Embedded, action.ScheduleEmbedded),

		bind(model.NexusOperationType, model.NexusScheduled, model.NexusAttemptFailed, umpire.Standalone, action.HandlerRetryable),
		bind(model.NexusOperationType, model.NexusScheduled, model.NexusAttemptFailed, umpire.Embedded, action.HandlerRetryable),
		bind(model.NexusOperationType, model.NexusScheduled, model.NexusStart, umpire.Standalone, action.HandlerAsyncAck),
		bind(model.NexusOperationType, model.NexusScheduled, model.NexusStart, umpire.Embedded, action.HandlerAsyncAck),

		bind(model.NexusOperationType, model.NexusScheduled, model.NexusSucceed, umpire.Standalone, action.HandlerSyncOk),
		bind(model.NexusOperationType, model.NexusScheduled, model.NexusSucceed, umpire.Embedded, action.HandlerSyncOk),
		bind(model.NexusOperationType, model.NexusStarted, model.NexusSucceed, umpire.Standalone, action.CompleteWith(nil, model.NexusSucceed)),
		bind(model.NexusOperationType, model.NexusStarted, model.NexusSucceed, umpire.Embedded, action.CompleteWith(nil, model.NexusSucceed)),

		bind(model.NexusOperationType, model.NexusScheduled, model.NexusFail, umpire.Standalone, action.HandlerOpFailed),
		bind(model.NexusOperationType, model.NexusScheduled, model.NexusFail, umpire.Embedded, action.HandlerOpFailed),
		bind(model.NexusOperationType, model.NexusStarted, model.NexusFail, umpire.Standalone, asyncFailure),
		bind(model.NexusOperationType, model.NexusStarted, model.NexusFail, umpire.Embedded, asyncFailure),

		bind(model.NexusOperationType, model.NexusScheduled, model.NexusCancel, umpire.Standalone, action.HandlerOpCanceled),
		bind(model.NexusOperationType, model.NexusScheduled, model.NexusCancel, umpire.Embedded, action.HandlerOpCanceled),
		bind(model.NexusOperationType, model.NexusStarted, model.NexusCancel, umpire.Standalone, asyncCancellation),
		bind(model.NexusOperationType, model.NexusStarted, model.NexusCancel, umpire.Embedded, asyncCancellation),

		bind(model.NexusOperationType, model.NexusScheduled, model.NexusTimeout, umpire.Standalone, action.TimerForceTimeout(testhooks.NexusForceTimeoutFromScheduled)),
		bind(model.NexusOperationType, model.NexusScheduled, model.NexusTimeout, umpire.Embedded, action.TimerForceTimeout(testhooks.NexusForceTimeoutFromScheduled)),
		bind(model.NexusOperationType, model.NexusBackingOff, model.NexusTimeout, umpire.Standalone, action.TimerForceTimeout(testhooks.NexusForceTimeoutFromBackingOff)),
		bind(model.NexusOperationType, model.NexusBackingOff, model.NexusTimeout, umpire.Embedded, action.TimerForceTimeout(testhooks.NexusForceTimeoutFromBackingOff)),

		bind(model.NexusOperationType, model.NexusScheduled, model.NexusTerminate, umpire.Standalone, action.TerminateFrom(model.NexusScheduled)),
		bind(model.NexusOperationType, model.NexusBackingOff, model.NexusTerminate, umpire.Standalone, action.TerminateFrom(model.NexusBackingOff)),
		bind(model.NexusOperationType, model.NexusStarted, model.NexusTerminate, umpire.Standalone, action.TerminateFrom(model.NexusStarted)),
	}
}

func nexusActionGaps() []ActionGap {
	return []ActionGap{
		gap(model.NexusOperationType, model.NexusStarted, model.NexusTimeout, umpire.Standalone, "needs a real schedule-to-close timer"),
		gap(model.NexusOperationType, model.NexusStarted, model.NexusTimeout, umpire.Embedded, "needs a real schedule-to-close timer"),
		gap(model.NexusOperationType, model.NexusScheduled, model.NexusTerminate, umpire.Embedded, "terminate is standalone-only"),
		gap(model.NexusOperationType, model.NexusBackingOff, model.NexusTerminate, umpire.Embedded, "terminate is standalone-only"),
		gap(model.NexusOperationType, model.NexusStarted, model.NexusTerminate, umpire.Embedded, "terminate is standalone-only"),
		gap(model.NexusOperationType, model.NexusUnspecified, model.NexusReject, umpire.Standalone, "rejection actions are outside the edge-action planner"),
		gap(model.NexusOperationType, model.NexusUnspecified, model.NexusReject, umpire.Embedded, "rejection actions are outside the edge-action planner"),
	}
}

func bind(
	entityType umpire.EntityType,
	from string,
	event string,
	hosting umpire.Hosting,
	executable umpire.Action,
) ActionBinding {
	return ActionBinding{
		Key:    ActionKey{Entity: entityType, From: from, Event: event, Hosting: hosting},
		Action: executable,
	}
}

func gap(
	entityType umpire.EntityType,
	from string,
	event string,
	hosting umpire.Hosting,
	reason string,
) ActionGap {
	return ActionGap{
		Key:    ActionKey{Entity: entityType, From: from, Event: event, Hosting: hosting},
		Reason: reason,
	}
}
