package frontend

import (
	"maps"
	"slices"
	"time"

	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/wideevents"
)

// This file builds the input structs for the frontend's namespace_lifecycle emitters (defined in
// common/wideevents) from frontend domain objects, so namespace_handler.go stays a pre-mutation
// snapshot (for updates) plus one emit call per operation — mirroring how
// service/history/replication/replication_events.go relates to its handlers.

// namespaceStateFields snapshots the full NamespaceInfo / NamespaceConfig / NamespaceReplicationConfig
// field set the namespace_lifecycle events report from a persisted namespace record. isGlobal is
// passed separately because it is not stored on the detail. Maps and slices are cloned so a later
// in-place mutation of the record does not disturb an earlier snapshot.
func namespaceStateFields(detail *persistencespb.NamespaceDetail, isGlobal bool) wideevents.NamespaceStateFields {
	info := detail.GetInfo()
	config := detail.GetConfig()
	repl := detail.GetReplicationConfig()
	return wideevents.NamespaceStateFields{
		Description:                 info.GetDescription(),
		Owner:                       info.GetOwner(),
		State:                       info.GetState().String(),
		IsGlobalNamespace:           isGlobal,
		Data:                        maps.Clone(info.GetData()),
		ConfigVersion:               detail.GetConfigVersion(),
		FailoverVersion:             detail.GetFailoverVersion(),
		FailoverNotificationVersion: detail.GetFailoverNotificationVersion(),
		FailoverEndTime:             detail.GetFailoverEndTime().AsTime().Format(time.RFC3339),
		Retention:                   config.GetRetention().AsDuration().String(),
		HistoryArchivalState:        config.GetHistoryArchivalState().String(),
		HistoryArchivalURI:          config.GetHistoryArchivalUri(),
		VisibilityArchivalState:     config.GetVisibilityArchivalState().String(),
		VisibilityArchivalURI:       config.GetVisibilityArchivalUri(),
		ArchivalBucket:              config.GetArchivalBucket(),
		CustomSearchAttributeAlias:  maps.Clone(config.GetCustomSearchAttributeAliases()),
		BadBinaries:                 badBinaryStrings(config.GetBadBinaries()),
		WorkflowRuleIDs:             slices.Sorted(maps.Keys(config.GetWorkflowRules())),
		ActiveCluster:               repl.GetActiveClusterName(),
		Clusters:                    slices.Clone(repl.GetClusters()),
		ReplicationState:            repl.GetState().String(),
		FailoverHistory:             toFailoverHistoryEntries(repl.GetFailoverHistory()),
	}
}

// registerRequestFields snapshots the RegisterNamespace RPC request as received, in the same shape as
// the persisted snapshot, so the two can be diffed on the event. It reads only the namespace fields;
// the request's security_token is deliberately not carried.
func registerRequestFields(req *workflowservice.RegisterNamespaceRequest) wideevents.NamespaceStateFields {
	return wideevents.NamespaceStateFields{
		Description:             req.GetDescription(),
		Owner:                   req.GetOwnerEmail(),
		IsGlobalNamespace:       req.GetIsGlobalNamespace(),
		Data:                    maps.Clone(req.GetData()),
		Retention:               req.GetWorkflowExecutionRetentionPeriod().AsDuration().String(),
		HistoryArchivalState:    req.GetHistoryArchivalState().String(),
		HistoryArchivalURI:      req.GetHistoryArchivalUri(),
		VisibilityArchivalState: req.GetVisibilityArchivalState().String(),
		VisibilityArchivalURI:   req.GetVisibilityArchivalUri(),
		ActiveCluster:           req.GetActiveClusterName(),
		Clusters:                clusterNames(req.GetClusters()),
	}
}

// updateRequestFields snapshots the UpdateNamespace RPC request as received. The request is a sparse
// patch, so a field the client did not set reads as its zero value here (an unset retention as "0s",
// an unset state as "Unspecified"); the persisted before/after carry the resolved values. It reads
// only the namespace fields; the request's security_token is deliberately not carried.
func updateRequestFields(req *workflowservice.UpdateNamespaceRequest) wideevents.NamespaceStateFields {
	info := req.GetUpdateInfo()
	config := req.GetConfig()
	repl := req.GetReplicationConfig()
	return wideevents.NamespaceStateFields{
		Description:                info.GetDescription(),
		Owner:                      info.GetOwnerEmail(),
		State:                      info.GetState().String(),
		Data:                       maps.Clone(info.GetData()),
		Retention:                  config.GetWorkflowExecutionRetentionTtl().AsDuration().String(),
		HistoryArchivalState:       config.GetHistoryArchivalState().String(),
		HistoryArchivalURI:         config.GetHistoryArchivalUri(),
		VisibilityArchivalState:    config.GetVisibilityArchivalState().String(),
		VisibilityArchivalURI:      config.GetVisibilityArchivalUri(),
		CustomSearchAttributeAlias: maps.Clone(config.GetCustomSearchAttributeAliases()),
		BadBinaries:                badBinaryStrings(config.GetBadBinaries()),
		ActiveCluster:              repl.GetActiveClusterName(),
		Clusters:                   clusterNames(repl.GetClusters()),
		ReplicationState:           repl.GetState().String(),
	}
}

// buildNamespaceRegisteredInput builds the input for a namespace_registered event.
func buildNamespaceRegisteredInput(req *persistence.CreateNamespaceRequest, nsID string, rawReq *workflowservice.RegisterNamespaceRequest) wideevents.NamespaceRegisteredInput {
	return wideevents.NamespaceRegisteredInput{
		Namespace:   req.Namespace.GetInfo().GetName(),
		NamespaceID: nsID,
		Fields:      namespaceStateFields(req.Namespace, req.IsGlobalNamespace),
		Requested:   registerRequestFields(rawReq),
	}
}

// buildNamespaceUpdatedInput builds the input for a namespace_updated event from the pre-mutation
// snapshot and the persisted (post-mutation) record. rawReq is the UpdateNamespace request as
// received, or nil for DeprecateNamespace, which reuses this event but carries no field inputs.
func buildNamespaceUpdatedInput(
	before wideevents.NamespaceStateFields,
	updated *persistencespb.NamespaceDetail,
	isGlobal bool,
	isFailover bool,
	isPromotion bool,
	rawReq *workflowservice.UpdateNamespaceRequest,
) wideevents.NamespaceUpdatedInput {
	var requested wideevents.NamespaceStateFields
	if rawReq != nil {
		requested = updateRequestFields(rawReq)
	}
	return wideevents.NamespaceUpdatedInput{
		Namespace:       updated.GetInfo().GetName(),
		NamespaceID:     updated.GetInfo().GetId(),
		IsFailover:      isFailover,
		IsPromotion:     isPromotion,
		DeleteBadBinary: rawReq.GetDeleteBadBinary(),
		Before:          before,
		After:           namespaceStateFields(updated, isGlobal),
		Requested:       requested,
	}
}

// toFailoverHistoryEntries converts a namespace's failover history to the wide-event form.
func toFailoverHistoryEntries(history []*persistencespb.FailoverStatus) []wideevents.FailoverHistoryEntry {
	entries := make([]wideevents.FailoverHistoryEntry, 0, len(history))
	for _, fs := range history {
		entries = append(entries, wideevents.FailoverHistoryEntry{
			FailoverVersion: fs.GetFailoverVersion(),
			FailoverTime:    fs.GetFailoverTime().AsTime().Format(time.RFC3339),
		})
	}
	return entries
}

// badBinaryStrings renders a namespace's bad-binaries config as checksum -> BadBinaryInfo text.
func badBinaryStrings(bb *namespacepb.BadBinaries) map[string]string {
	binaries := bb.GetBinaries()
	if len(binaries) == 0 {
		return nil
	}
	out := make(map[string]string, len(binaries))
	for checksum, info := range binaries {
		out[checksum] = info.String()
	}
	return out
}

// clusterNames extracts the cluster names from a request's cluster replication configs.
func clusterNames(clusters []*replicationpb.ClusterReplicationConfig) []string {
	if len(clusters) == 0 {
		return nil
	}
	names := make([]string, 0, len(clusters))
	for _, c := range clusters {
		names = append(names, c.GetClusterName())
	}
	return names
}
