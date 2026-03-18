package temporalfs

import "io"

// FSStoreProvider is the pluggable interface for FS storage backends.
// OSS implements this with PebbleStoreProvider. SaaS can implement with WalkerStore.
//
// This is the sole extension point for SaaS — all other FS components
// (CHASM archetype, gRPC service, FUSE mount) are identical between OSS and SaaS.
type FSStoreProvider interface {
	// GetStore returns an FSStore scoped to a specific FS execution.
	// The returned store provides full key isolation for that execution.
	GetStore(shardID int32, namespaceID string, filesystemID string) (FSStore, error)

	// Close releases all resources (PebbleDB instances, Walker sessions, etc.)
	io.Closer
}

// FSStore is the key-value storage interface used by the FS layer.
// This mirrors temporal-fs/pkg/store.Store and will be replaced by a direct
// import once the temporal-fs module is available as a dependency.
type FSStore interface {
	// Get retrieves the value for the given key. Returns nil, nil if not found.
	Get(key []byte) ([]byte, error)

	// Set stores a key-value pair.
	Set(key []byte, value []byte) error

	// Delete removes a key.
	Delete(key []byte) error

	// NewBatch creates a new write batch for atomic operations.
	NewBatch() FSBatch

	// Scan iterates over keys in [start, end) range, calling fn for each.
	// Iteration stops if fn returns false.
	Scan(start, end []byte, fn func(key, value []byte) bool) error

	// Close releases the store's resources.
	Close() error
}

// FSBatch is an atomic write batch.
type FSBatch interface {
	Set(key []byte, value []byte) error
	Delete(key []byte) error
	Commit() error
	Close() error
}
