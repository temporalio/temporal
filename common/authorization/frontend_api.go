package authorization

import (
	"go.temporal.io/server/common/api"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var healthCheckAPI = map[string]struct{}{
	healthpb.Health_Check_FullMethodName:                             {},
	"/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo": {},
}

func IsReadOnlyNamespaceAPI(workflowServiceMethod string) bool {
	fullApiName := api.WorkflowServicePrefix + workflowServiceMethod
	metadata := api.GetMethodMetadata(fullApiName)
	return metadata.Scope == api.ScopeNamespace && metadata.Access == api.AccessReadOnly
}

func IsReadOnlyGlobalAPI(workflowServiceMethod string) bool {
	fullApiName := api.WorkflowServicePrefix + workflowServiceMethod
	metadata := api.GetMethodMetadata(fullApiName)
	return metadata.Scope == api.ScopeCluster && metadata.Access == api.AccessReadOnly
}

func IsHealthCheckAPI(fullApi string) bool {
	_, found := healthCheckAPI[fullApi]
	return found
}
