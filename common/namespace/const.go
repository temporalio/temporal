package namespace

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

const (
	// MinRetentionGlobal is a hard limit for the minimun retention duration for global
	// namespaces (to allow time for replication).
	MinRetentionGlobal = 1 * 24 * time.Hour

	// MinRetentionLocal is a hard limit for the minimun retention duration for local
	// namespaces. Allow short values but disallow zero to avoid confusion with
	// interpreting zero as infinite.
	MinRetentionLocal = 1 * time.Hour
)

// ErrNamespaceHandover is error indicating namespace is in handover state and cannot process request.
var ErrNamespaceHandover = serviceerror.NewUnavailablef("Namespace replication in %s state.", enumspb.REPLICATION_STATE_HANDOVER)
