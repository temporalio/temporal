package nexus

// SystemCallbackURL is the reserved callback URL used to route Nexus operation callbacks
// internally within Temporal. It must match the scheme/host used in validation and routing logic.
const SystemCallbackURL = "temporal://system"

// SystemEndpoint is the reserved endpoint name for Temporal system operations.
// Operation requests for this endpoint are routed internally within the history service.
const SystemEndpoint = "__temporal_system"

// PROTOTYPE
const DispatchWorkerCallbackURL = "temporal://system/dispatch-worker-callback"

// PROTOTYPE
// Keys of expected headers when worker callbacks are sent over gRPC APIs.
const (
	WorkerCallbackTargetNamespaceHeader  = "target-namespace"
	WorkerCallbackTargetActivityHeader   = "target-activity"
	WorkerCallbackTargetTaskQueueHeader  = "target-task-queue"
	WorkerCallbackTargetActivityIDHeader = "target-activity-id"
	// Server-only routing metadata: the Nexus service/operation whose completion triggered the
	// worker callback. Passed alongside the target-* headers and surfaced to the dispatched
	// activity's poller as ActivityInvocationSource.
	WorkerCallbackNexusServiceHeader   = "nexus-service"
	WorkerCallbackNexusOperationHeader = "nexus-operation"
	WorkerCallbackBrandHeader          = "worker-callback"
	WorkerCallbackTypeHeader             = "worker-callback-type"
	WorkerCallbackMetadataHeader         = "worker-callback-metadata"
)
