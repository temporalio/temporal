package temporalfs

import (
	"github.com/temporalio/temporal-fs/pkg/store"
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

	// Close releases all resources (PebbleDB instances, Walker sessions, etc.)
	Close() error
}
