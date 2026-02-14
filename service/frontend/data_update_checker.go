package frontend

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/persistence"
)

// DataUpdateCheckResult contains the result of checking for a data-only update.
type DataUpdateCheckResult struct {
	// ModifiedData is the data map to persist (nil if not using fast-path)
	ModifiedData map[string]string
	// ShouldFastPath is true if only data field should be updated (skip other logic)
	ShouldFastPath bool
	// ShouldReplicate is true to force replication even for single-cluster namespaces
	ShouldReplicate bool
}

// NamespaceDataUpdateChecker determines if a namespace update request
// should be handled as a data-only fast-path update.
type NamespaceDataUpdateChecker interface {
	// CheckDataOnlyUpdate checks if the update should bypass standard
	// update logic and only update the namespace data field.
	//
	// Parameters:
	//   - ctx: context for the operation
	//   - currentNamespace: the existing namespace from persistence
	//   - updateRequest: the incoming update request
	//
	// Returns:
	//   - result: contains modifiedData, shouldFastPath, and shouldReplicate flags
	//   - err: error if validation fails (request will be rejected)
	//
	// When result.ShouldFastPath is true, any other update fields in the request are
	// silently dropped and only the modifiedData is persisted.
	CheckDataOnlyUpdate(
		ctx context.Context,
		currentNamespace *persistence.GetNamespaceResponse,
		updateRequest *workflowservice.UpdateNamespaceRequest,
	) (DataUpdateCheckResult, error)
}

// NoopDataUpdateChecker is the default implementation that never
// triggers the fast-path, allowing all updates to flow through normal logic.
type NoopDataUpdateChecker struct{}

// NewNoopDataUpdateChecker creates a new NoopDataUpdateChecker.
func NewNoopDataUpdateChecker() NamespaceDataUpdateChecker {
	return &NoopDataUpdateChecker{}
}

// CheckDataOnlyUpdate always returns an empty result to use the standard update flow.
func (n *NoopDataUpdateChecker) CheckDataOnlyUpdate(
	ctx context.Context,
	currentNamespace *persistence.GetNamespaceResponse,
	updateRequest *workflowservice.UpdateNamespaceRequest,
) (DataUpdateCheckResult, error) {
	return DataUpdateCheckResult{}, nil
}
