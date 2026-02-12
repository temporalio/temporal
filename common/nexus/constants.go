package nexus

// SystemCallbackURL is the reserved callback URL used to route Nexus operation callbacks
// internally within Temporal. It must match the scheme/host used in validation and routing logic.
const SystemCallbackURL = "temporal://system"

// Worker control Nexus service and operations for server-to-worker communication.
const (
	// WorkerControlService is the Nexus service name for worker control operations.
	WorkerControlService = "temporal.api.worker.v1.WorkerService"
	// CancelActivitiesOperation is the operation to cancel running activities.
	CancelActivitiesOperation = "cancel-activities"
)
