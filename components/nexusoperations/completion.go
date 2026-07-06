package nexusoperations

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func handleSuccessfulOperationResult(
	node *hsm.Node,
	operation Operation,
	result *commonpb.Payload,
	links []*commonpb.Link,
) error {
	eventID, err := hsm.EventIDFromToken(operation.ScheduledEventToken)
	if err != nil {
		return err
	}
	event := node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, func(e *historypb.HistoryEvent) {
		// We must assign to this property, linter doesn't like this.
		// nolint:revive
		e.Attributes = &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
			NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{
				ScheduledEventId: eventID,
				Result:           result,
				RequestId:        operation.RequestId,
			},
		}
		e.Links = links
	})
	return CompletedEventDefinition{}.Apply(node.Parent, event)
}

func handleOperationError(
	node *hsm.Node,
	operation Operation,
	opErr *nexus.OperationError,
) error {
	eventID, err := hsm.EventIDFromToken(operation.ScheduledEventToken)
	if err != nil {
		return err
	}
	var originalCause *failurepb.Failure
	// Special marker for Temporal->Temporal calls to indicate that the original failure should be unwrapped.
	// Temporal uses a wrapper operation error with no additional information to transmit the OperationError over the network.
	// The meaningful information is in the operation error's cause.
	unwrapError := opErr.OriginalFailure.Metadata["unwrap-error"] == "true"

	if unwrapError && opErr.OriginalFailure.Cause != nil {
		var err error
		originalCause, err = commonnexus.NexusFailureToTemporalFailure(*opErr.OriginalFailure.Cause)
		if err != nil {
			return serviceerror.NewInvalidArgumentf("Malformed failure: %v", err)
		}
	} else {
		// Transform the OperationError to either ApplicationFailure or CanceledFailure based on the operation error state.
		originalCause, err = commonnexus.NexusFailureToTemporalFailure(*opErr.OriginalFailure)
		if err != nil {
			return serviceerror.NewInvalidArgumentf("Malformed failure: %v", err)
		}
	}

	switch opErr.State { // nolint:exhaustive
	case nexus.OperationStateFailed:
		event := node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, func(e *historypb.HistoryEvent) {
			// We must assign to this property, linter doesn't like this.
			// nolint:revive
			e.Attributes = &historypb.HistoryEvent_NexusOperationFailedEventAttributes{
				NexusOperationFailedEventAttributes: &historypb.NexusOperationFailedEventAttributes{
					Failure:          createNexusOperationFailure(operation, eventID, originalCause),
					ScheduledEventId: eventID,
					RequestId:        operation.RequestId,
				},
			}
		})

		return FailedEventDefinition{}.Apply(node.Parent, event)
	case nexus.OperationStateCanceled:
		if originalCause.GetCanceledFailureInfo() == nil {
			// Old SDKs may send an ApplicationFailure for canceled operation causes.
			originalCause = &failurepb.Failure{
				Message:    originalCause.GetMessage(),
				StackTrace: originalCause.GetStackTrace(),
				FailureInfo: &failurepb.Failure_CanceledFailureInfo{
					CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
				},
				Cause: originalCause.GetCause(),
			}
		}
		event := node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED, func(e *historypb.HistoryEvent) {
			// We must assign to this property, linter doesn't like this.
			// nolint:revive
			e.Attributes = &historypb.HistoryEvent_NexusOperationCanceledEventAttributes{
				NexusOperationCanceledEventAttributes: &historypb.NexusOperationCanceledEventAttributes{
					Failure:          createNexusOperationFailure(operation, eventID, originalCause),
					ScheduledEventId: eventID,
					RequestId:        operation.RequestId,
				},
			}
		})

		return CanceledEventDefinition{}.Apply(node.Parent, event)
	default:
		// Both the Nexus Client and CompletionHandler reject invalid states, but just in case, we return this as a
		// transition error.
		return fmt.Errorf("unexpected operation state: %v", opErr.State)
	}
}

// fabricateStartedEventIfMissing adds a NEXUS_OPERATION_STARTED history event and transitions the
// operation to NEXUS_OPERATION_STATE_STARTED if it has not started yet. It is necessary if the
// completion is received before the start response. Callers that need to know whether a start was
// fabricated should check the operation's state before calling (see TransitionStarted.Possible).
func fabricateStartedEventIfMissing(
	node *hsm.Node,
	requestID string,
	operationToken string,
	startTime *timestamppb.Timestamp,
	links []*commonpb.Link,
) error {
	operation, err := hsm.MachineData[Operation](node)
	if err != nil {
		return err
	}

	// The operation was already started, ignore.
	if !TransitionStarted.Possible(operation) {
		return nil
	}

	eventID, err := hsm.EventIDFromToken(operation.ScheduledEventToken)
	if err != nil {
		return err
	}

	event := node.AddHistoryEvent(enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
			NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{
				ScheduledEventId: eventID,
				OperationToken:   operationToken,
				// TODO(bergundy): Remove this fallback after the 1.27 release.
				OperationId: operationToken,
				RequestId:   requestID,
			},
		}
		e.Links = links
		if startTime != nil {
			e.EventTime = startTime
		}
	})
	return (StartedEventDefinition{}).Apply(node.Parent, event)
}

// CompletionHandler resolves async Nexus operation completions delivered to the history service
// and emits the caller-side terminal metrics. It is provided via fx and injected into the history
// handler so the metrics handler and tag config are sourced from the dependency graph rather than
// threaded through the call site.
type CompletionHandler struct {
	metricsHandler metrics.Handler
	config         *Config
}

// NewCompletionHandler returns a CompletionHandler. Wired via fx; see Module.
func NewCompletionHandler(metricsHandler metrics.Handler, config *Config) *CompletionHandler {
	return &CompletionHandler{metricsHandler: metricsHandler, config: config}
}

// Handle resolves an async Nexus operation completion.
func (h *CompletionHandler) Handle(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	requestID string,
	operationToken string,
	startTime *timestamppb.Timestamp,
	links []*commonpb.Link,
	result *commonpb.Payload,
	opFailedError *nexus.OperationError,
) error {
	// The initial version of the completion token did not include a request ID.
	// Only retry Access without a run ID if the request ID is not empty.
	isRetryableNotFoundErr := requestID != ""
	// emitMetrics is populated inside the Access closure and invoked only after the write
	// transaction commits successfully, so a failed commit (which retries the completion) does not
	// double-count the metric. See operationMetricsHandler's doc comment.
	var emitMetrics func()
	err := env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		if err := node.CheckRunning(); err != nil {
			return err
		}
		operation, err := hsm.MachineData[Operation](node)
		if err != nil {
			return err
		}
		// If the operation has not started yet, this completion arrived before the start response and
		// fabricateStartedEventIfMissing will transition it to started below; the executor never emitted
		// schedule-to-start in that case, so we emit it here.
		fabricatedStart := TransitionStarted.Possible(operation)
		if err := fabricateStartedEventIfMissing(node, requestID, operationToken, startTime, links); err != nil {
			return err
		}
		if requestID != "" && operation.RequestId != requestID {
			isRetryableNotFoundErr = false
			return serviceerror.NewNotFound("operation not found")
		}

		if opFailedError != nil {
			err = handleOperationError(node, operation, opFailedError)
		} else {
			err = handleSuccessfulOperationResult(node, operation, result, nil)
		}
		// TODO(bergundy): Remove this once the operation auto-deletes itself from the tree on completion with state
		// based replication.
		if errors.Is(err, hsm.ErrInvalidTransition) {
			isRetryableNotFoundErr = false
			return serviceerror.NewNotFound("operation not found")
		}
		if err != nil {
			return err
		}
		// fabricatedStart means the executor never emitted schedule-to-start, so we emit it here.
		emitScheduleToStart := fabricatedStart && operation.StartedTime != nil
		emitMetrics = h.deferredCompletionMetric(operation, node.NamespaceName(), node.WorkflowTypeName(), opFailedError, emitScheduleToStart, env.Now())
		return nil
	})
	if errors.As(err, new(*serviceerror.NotFound)) && isRetryableNotFoundErr && ref.WorkflowKey.RunID != "" {
		// Try again without a run ID in case the original run was reset.
		ref.WorkflowKey.RunID = ""
		// VersionedTransition is for a specific run. After reset, the TransitionCount will
		// start from 1 again. Reset the TransitionCount to 0 here to fallback to old ref
		// validation logic.
		ref.StateMachineRef.MutableStateVersionedTransition = nil
		ref.StateMachineRef.MachineInitialVersionedTransition.TransitionCount = 0
		ref.StateMachineRef.MachineLastUpdateVersionedTransition.TransitionCount = 0
		return h.Handle(ctx, env, ref, requestID, operationToken, startTime, links, result, opFailedError)
	}
	if err != nil {
		return err
	}
	if emitMetrics != nil {
		emitMetrics()
	}
	return nil
}

// deferredCompletionMetric builds the post-commit caller-side emit for a resolved async completion:
// the terminal outcome (the canceled vs failed split mirrors handleOperationError so the metric
// matches the recorded transition) plus, when the start was fabricated here, schedule-to-start.
func (h *CompletionHandler) deferredCompletionMetric(
	operation Operation,
	namespaceName, workflowType string,
	opFailedError *nexus.OperationError,
	emitScheduleToStart bool,
	closeTime time.Time,
) func() {
	metricsHandler := h.metricsHandler
	metricTagConfig := h.config.ResolvedMetricTagConfig()
	return func() {
		if emitScheduleToStart {
			emitScheduleToStartLatency(metricsHandler, metricTagConfig, operation, namespaceName, workflowType, operation.StartedTime.AsTime())
		}
		switch {
		case opFailedError == nil:
			emitOperationSucceeded(metricsHandler, metricTagConfig, operation, namespaceName, workflowType, closeTime)
		case opFailedError.State == nexus.OperationStateCanceled:
			emitOperationCanceled(metricsHandler, metricTagConfig, operation, namespaceName, workflowType, closeTime)
		default:
			emitOperationFailed(metricsHandler, metricTagConfig, operation, namespaceName, workflowType, closeTime)
		}
	}
}
