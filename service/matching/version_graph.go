package matching

import (
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/persistence/v1"
)

/// Represents a graph of version information associated with a specific task queue
type VersionsGraph struct{}

/// Extract version graphs from the persisted task queue info
func VersionGraphFromProto(info *persistence.VersioningData) *VersionsGraph {
	return &VersionsGraph{}
}

func (*VersionsGraph) AsOrderingResponse() *matchingservice.GetWorkerBuildIdOrderingResponse {
	return &matchingservice.GetWorkerBuildIdOrderingResponse{
		Response: &workflowservice.GetWorkerBuildIdOrderingResponse{},
	}
}
