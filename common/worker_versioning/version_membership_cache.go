//nolint:staticheck
package worker_versioning

import enumspb "go.temporal.io/api/enums/v1"

// VersionMembershipCache is used to cache results of Matching's CheckTaskQueueVersionMembership
// calls (used internally by the worker versioning pinned override validation).
//
// Implementations are expected to be safe for concurrent use.
type VersionMembershipCache interface {
	// Get returns (isMember, ok). ok=false means there was no cached value.
	Get(
		namespaceID string,
		taskQueue string,
		taskQueueType enumspb.TaskQueueType,
		deploymentName string,
		buildID string,
	) (isMember bool, ok bool)

	Put(
		namespaceID string,
		taskQueue string,
		taskQueueType enumspb.TaskQueueType,
		deploymentName string,
		buildID string,
		isMember bool,
	)
}
