package workflowresend

import (
	"maps"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/common/wideevents"
	historyi "go.temporal.io/server/service/history/interfaces"
)

type LifecycleEvent struct {
	ShardContext historyi.ShardContext
	NamespaceID  namespace.ID
	Execution    *commonpb.WorkflowExecution
	ResendPhase  string
	Message      string
	Outcome      string
	Err          error
	Details      map[string]any
}

// EmitLifecycleEvent emits a parent or child workflow resend checkpoint.
func EmitLifecycleEvent(event LifecycleEvent) {
	eventDetails := make(map[string]any, len(event.Details)+4)
	maps.Copy(eventDetails, event.Details)
	eventDetails["event_type"] = wideevents.ParentChildLifecycleEventType
	eventDetails["phase"] = event.ResendPhase
	eventDetails["outcome"] = event.Outcome
	eventDetails["local_cluster"] = event.ShardContext.GetClusterMetadata().GetCurrentClusterName()

	namespaceName := ""
	if entry, namespaceErr := event.ShardContext.GetNamespaceRegistry().GetNamespaceByID(event.NamespaceID); namespaceErr == nil {
		namespaceName = entry.Name().String()
	}
	sourceCluster, ok := eventDetails["source_cluster"].(string)
	if !ok {
		sourceCluster = ""
	}
	delete(eventDetails, "source_cluster")
	if event.Err != nil {
		eventDetails["error"] = event.Err.Error()
		eventDetails["error_type"] = util.ErrorType(event.Err)
	}
	payload := wideevents.ReplicationLifecyclePayload{
		TaskType:      wideevents.ReplTaskSyncWorkflowState,
		Shard:         event.ShardContext.GetShardID(),
		Namespace:     namespaceName,
		NamespaceID:   event.NamespaceID.String(),
		WorkflowID:    event.Execution.GetWorkflowId(),
		RunID:         event.Execution.GetRunId(),
		SourceCluster: sourceCluster,
	}
	switch event.Outcome {
	case wideevents.ParentChildOutcomeScheduled,
		wideevents.ParentChildOutcomeStarted,
		wideevents.ParentChildOutcomeDeduplicated:
		eventDetails["operation"] = wideevents.ReplOperationStandbyVerificationSyncState
		eventDetails["message"] = event.Message
		payload.Phase = wideevents.ReplicationExecuting
		payload.Details = eventDetails
		wideevents.Emit(event.ShardContext.GetEventLogger(), payload)
	case wideevents.ParentChildOutcomeSucceeded, wideevents.ParentChildOutcomeSourceNotFound:
		eventDetails["operation"] = wideevents.ReplOperationStandbyVerificationSyncState
		eventDetails["message"] = event.Message
		payload.Phase = wideevents.ReplicationApplied
		payload.Outcome = wideevents.ParentChildOutcomeVerified
		payload.Details = eventDetails
		wideevents.Emit(event.ShardContext.GetEventLogger(), payload)
	default:
		wideevents.EmitReplicationError(
			event.ShardContext.GetEventLogger(),
			payload,
			wideevents.ReplOperationStandbyVerificationSyncState,
			event.Message,
			event.Err,
			eventDetails,
		)
	}
}
