package temporalzfs

import (
	"github.com/temporalio/temporal-zfs/pkg/store"
)

// FSStoreProvider is the pluggable interface for FS storage backends.
// OSS implements this with PebbleStoreProvider. SaaS implements with
// CDSStoreProvider (backed by Walker) via fx.Decorate in saas-temporal.
//
// This is the sole extension point for SaaS — all other FS components
// (CHASM archetype, gRPC service, FUSE mount) are identical between OSS and SaaS.
type FSStoreProvider interface {
	// GetStore returns a store.Store scoped to a specific FS execution.
	// The returned store provides full key isolation for that execution.
	GetStore(shardID int32, namespaceID string, filesystemID string) (store.Store, error)

	// DeleteStore deletes all FS data for a specific filesystem execution.
	// Called by DataCleanupTask when a filesystem transitions to DELETED.
	DeleteStore(shardID int32, namespaceID string, filesystemID string) error

	// Close releases all resources (PebbleDB instances, Walker sessions, etc.)
	Close() error
}
