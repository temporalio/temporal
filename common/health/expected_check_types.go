package health

// HealthCheck type constants — short, machine-readable identifiers for each check.
// Used in HealthCheck.CheckType for programmatic matching/grouping.
// Human-readable details go in HealthCheck.Message (dynamically populated with values).
//
// We use string constants instead of a proto enum for flexibility: new check types
// can be added without proto changes, and the message field can describe exactly
// what went wrong with actual values (e.g. "RPC latency 850.00ms exceeded 500.00ms threshold").
const (
	CheckTypeGRPCHealth          = "grpc_health"
	CheckTypeRPCLatency          = "rpc_latency"
	CheckTypeRPCErrorRatio       = "rpc_error_ratio"
	CheckTypePersistenceLatency  = "persistence_latency"
	CheckTypePersistenceErrRatio = "persistence_error_ratio"
	CheckTypeHostAvailability    = "host_availability"
	CheckTypeTaskQueueBacklog    = "task_queue_backlog"
)
