package namespace

import "time"

const (
	// MinRetentionGlobal is a hard limit for the minimun retention duration for global
	// namespaces (to allow time for replication).
	MinRetentionGlobal = 1 * 24 * time.Hour

	// MinRetentionLocal is a hard limit for the minimun retention duration for local
	// namespaces. Allow short values but disallow zero to avoid confusion with
	// interpreting zero as infinite.
	MinRetentionLocal = 1 * time.Hour
)
