package frontend

import (
	"slices"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/wideevents"
)

// This file builds the input structs for the frontend's namespace_lifecycle emitters (defined in
// common/wideevents) from frontend domain objects, so namespace_handler.go stays a pre-mutation
// snapshot (for updates) plus one emit call per operation — mirroring how
// service/history/replication/replication_events.go relates to its handlers.

// namespaceStateFields snapshots the fields the namespace_lifecycle events report from a namespace
// record. isGlobal is passed separately because it is not stored on the detail. Slices are cloned so
// a later in-place mutation of the record does not disturb an earlier snapshot.
func namespaceStateFields(detail *persistencespb.NamespaceDetail, isGlobal bool) wideevents.NamespaceStateFields {
	info := detail.GetInfo()
	config := detail.GetConfig()
	repl := detail.GetReplicationConfig()
	return wideevents.NamespaceStateFields{
		Description:                 info.GetDescription(),
		State:                       info.GetState().String(),
		IsGlobalNamespace:           isGlobal,
		ConfigVersion:               detail.GetConfigVersion(),
		FailoverVersion:             detail.GetFailoverVersion(),
		FailoverNotificationVersion: detail.GetFailoverNotificationVersion(),
		FailoverEndTime:             detail.GetFailoverEndTime().AsTime().Format(time.RFC3339),
		Retention:                   config.GetRetention().AsDuration().String(),
		HistoryArchivalState:        config.GetHistoryArchivalState().String(),
		VisibilityArchivalState:     config.GetVisibilityArchivalState().String(),
		ActiveCluster:               repl.GetActiveClusterName(),
		Clusters:                    slices.Clone(repl.GetClusters()),
		ReplicationState:            repl.GetState().String(),
		FailoverHistory:             toFailoverHistoryEntries(repl.GetFailoverHistory()),
	}
}

// buildNamespaceRegisteredInput builds the input for a namespace_registered event.
func buildNamespaceRegisteredInput(req *persistence.CreateNamespaceRequest, nsID string) wideevents.NamespaceRegisteredInput {
	return wideevents.NamespaceRegisteredInput{
		Namespace:   req.Namespace.GetInfo().GetName(),
		NamespaceID: nsID,
		Fields:      namespaceStateFields(req.Namespace, req.IsGlobalNamespace),
	}
}

// buildNamespaceUpdatedInput builds the input for a namespace_updated event from the pre-mutation
// snapshot and the persisted (post-mutation) record.
func buildNamespaceUpdatedInput(
	before wideevents.NamespaceStateFields,
	updated *persistencespb.NamespaceDetail,
	isGlobal bool,
	isFailover bool,
	isPromotion bool,
) wideevents.NamespaceUpdatedInput {
	return wideevents.NamespaceUpdatedInput{
		Namespace:   updated.GetInfo().GetName(),
		NamespaceID: updated.GetInfo().GetId(),
		IsFailover:  isFailover,
		IsPromotion: isPromotion,
		Before:      before,
		After:       namespaceStateFields(updated, isGlobal),
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
