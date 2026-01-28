package nexus

// SystemCallbackURL is the reserved callback URL used to route Nexus operation callbacks
// internally within Temporal. It must match the scheme/host used in validation and routing logic.
const SystemCallbackURL = "temporal://system"

// SystemNexusCompletionURL is the internal URL for completing system Nexus operations.
// Used by WaitExternalWorkflow callbacks to complete the caller's Nexus operation
// when the target workflow closes.
const SystemNexusCompletionURL = "temporal://system-nexus-complete"

// SystemEndpointName is the reserved endpoint name for system Nexus operations.
// Workflows can use this endpoint to invoke built-in Temporal server operations
// like WaitExternalWorkflowCompletion without requiring a user-defined endpoint.
const SystemEndpointName = "__temporal_system"

// SystemServiceName is the service name used for system Nexus operations.
const SystemServiceName = "temporal.system.v1"

// System operation names
const (
	// WaitExternalWorkflowCompletionOperationName is the operation name for waiting
	// on an external workflow to complete.
	WaitExternalWorkflowCompletionOperationName = "WaitExternalWorkflowCompletion"
)

// IsSystemEndpoint returns true if the endpoint name is the reserved system endpoint.
func IsSystemEndpoint(endpoint string) bool {
	return endpoint == SystemEndpointName
}
