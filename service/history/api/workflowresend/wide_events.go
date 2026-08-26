package workflowresend

import (
	"maps"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/common/wideevents"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// EmitLifecycleEvent emits a parent or child workflow resend checkpoint.
func EmitLifecycleEvent(
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	resendPhase string,
	message string,
	outcome string,
	err error,
	details map[string]any,
) {
	eventDetails := make(map[string]any, len(details)+4)
	maps.Copy(eventDetails, details)
	eventDetails["event_type"] = wideevents.ParentChildLifecycleEventType
	eventDetails["phase"] = resendPhase
	eventDetails["outcome"] = outcome
	eventDetails["local_cluster"] = shardContext.GetClusterMetadata().GetCurrentClusterName()

	namespaceName := ""
	if entry, namespaceErr := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID); namespaceErr == nil {
		namespaceName = entry.Name().String()
	}
	sourceCluster, ok := eventDetails["source_cluster"].(string)
	if !ok {
		sourceCluster = ""
	}
	delete(eventDetails, "source_cluster")
	if err != nil {
		eventDetails["error"] = err.Error()
		eventDetails["error_type"] = util.ErrorType(err)
	}
	payload := wideevents.ReplicationLifecyclePayload{
		TaskType:      wideevents.ReplTaskSyncWorkflowState,
		Shard:         shardContext.GetShardID(),
		Namespace:     namespaceName,
		NamespaceID:   namespaceID.String(),
		WorkflowID:    execution.GetWorkflowId(),
		RunID:         execution.GetRunId(),
		SourceCluster: sourceCluster,
	}
	switch outcome {
	case wideevents.ParentChildOutcomeScheduled,
		wideevents.ParentChildOutcomeStarted,
		wideevents.ParentChildOutcomeDeduplicated:
		eventDetails["operation"] = wideevents.ReplOperationStandbyVerificationSyncState
		eventDetails["message"] = message
		payload.Phase = wideevents.ReplicationExecuting
		payload.Details = eventDetails
		wideevents.Emit(shardContext.GetEventLogger(), payload)
	case wideevents.ParentChildOutcomeSucceeded, wideevents.ParentChildOutcomeSourceNotFound:
		eventDetails["operation"] = wideevents.ReplOperationStandbyVerificationSyncState
		eventDetails["message"] = message
		payload.Phase = wideevents.ReplicationApplied
		payload.Outcome = wideevents.ParentChildOutcomeVerified
		payload.Details = eventDetails
		wideevents.Emit(shardContext.GetEventLogger(), payload)
	default:
		wideevents.EmitReplicationError(
			shardContext.GetEventLogger(),
			payload,
			wideevents.ReplOperationStandbyVerificationSyncState,
			message,
			err,
			eventDetails,
		)
	}
}
