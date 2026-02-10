package nexus

// SystemCallbackURL is the reserved callback URL used to route Nexus operation callbacks
// internally within Temporal. It must match the scheme/host used in validation and routing logic.
const SystemCallbackURL = "temporal://system"

// Worker control Nexus service and operations for server-to-worker communication.
const (
	// WorkerControlService is the Nexus service name for worker control operations.
	WorkerControlService = "temporal-sys-worker-control"
	// CancelActivitiesOperation is the operation to cancel running activities.
	CancelActivitiesOperation = "cancel-activities"
)

// WorkerControlQueuePrefix is the prefix for Nexus task queues used to deliver control commands
// (e.g., activity cancellation) to workers.
// Control queues are always single-partition to ensure tasks reach the correct worker.
const WorkerControlQueuePrefix = "/temporal-sys/worker-commands"
