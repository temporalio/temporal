package authorization

// Defines principal types and names supported in Temporal.
const (
	// Identifies internal Temporal-managed services such as history,
	// matching, per-namespace worker, etc.
	InternalPrincipalType = "temporal"
	InternalPrincipalName = "internal"
)
