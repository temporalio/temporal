package nexus

// SystemCallbackURL is the reserved callback URL used to route Nexus operation callbacks
// internally within Temporal. It must match the scheme/host used in validation and routing logic.
const SystemCallbackURL = "temporal://system"

// WorkerControlQueuePrefix is the prefix for Nexus task queues used to deliver control commands
// (e.g., activity cancellation) to workers. The full queue name is constructed as:
// {WorkerControlQueuePrefix}/{worker_grouping_key}
// where worker_grouping_key is provided by the worker in its heartbeat.
// Control queues are always single-partition to ensure tasks reach the correct worker.
const WorkerControlQueuePrefix = "/temporal-sys/worker-commands"
