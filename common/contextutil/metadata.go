package contextutil

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
)

type (
	metadataContextKey struct{}

	// metadataContext is used to store workflow and activity metadata
	metadataContext struct {
		sync.Mutex
		Metadata          map[string]any
		MarkedActivityIDs map[string]struct{}
	}
)

var metadataCtxKey = metadataContextKey{}

const (
	// MetadataKeyWorkflowType is the context metadata key for workflow type
	MetadataKeyWorkflowType = "workflow-type"
	// MetadataKeyWorkflowTaskQueue is the context metadata key for workflow task queue
	MetadataKeyWorkflowTaskQueue = "workflow-task-queue"

	activityTypePrefix      = "activity-type-"
	activityTaskQueuePrefix = "activity-task-queue-"
)

// hashActivityID returns a gRPC-trailer-safe key suffix from an arbitrary activity ID.
// Uses FNV-64a: standard library, fast, deterministic, 16 hex chars ([0-9a-f]).
// Collision probability ~1 in 340 trillion for 1,000 activities per request.
// Alternatives: SHA-256 (overkill, 64 chars), CRC-32 (worse distribution),
// raw encoding (unbounded length, may contain invalid gRPC key chars).
func hashActivityID(activityID string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(activityID))
	return fmt.Sprintf("%016x", h.Sum64())
}

// ActivityTypeKey returns the metadata key for the given activity ID's type.
// The activity ID is hashed to produce a gRPC-trailer-safe key suffix.
func ActivityTypeKey(activityID string) string {
	return activityTypePrefix + hashActivityID(activityID)
}

// ActivityTaskQueueKey returns the metadata key for the given activity ID's task queue.
// The activity ID is hashed to produce a gRPC-trailer-safe key suffix.
func ActivityTaskQueueKey(activityID string) string {
	return activityTaskQueuePrefix + hashActivityID(activityID)
}

// ContextMetadataMarkActivityID marks an activity ID on the context for metadata resolution.
// The handler knows which activity (from the task token) but not its type or task queue.
// Mutable state knows the activity details but not which activity the request targets.
// This bridges the two: the handler marks the ID, and SetContextMetadata (during
// closeTransaction) resolves it to type and task queue from mutable state.
// Cannot be used for transactions that remove the activity from mutable state
// (e.g., activity completion), since it won't be available for resolution.
func ContextMetadataMarkActivityID(ctx context.Context, activityID string) bool {
	metadataCtx := getMetadataContext(ctx)
	if metadataCtx == nil {
		return false
	}
	metadataCtx.Lock()
	defer metadataCtx.Unlock()
	metadataCtx.MarkedActivityIDs[activityID] = struct{}{}
	return true
}

// ContextMetadataGetActivityIDs returns the marked activity IDs from the context.
func ContextMetadataGetActivityIDs(ctx context.Context) []string {
	metadataCtx := getMetadataContext(ctx)
	if metadataCtx == nil {
		return nil
	}

	metadataCtx.Lock()
	defer metadataCtx.Unlock()

	if len(metadataCtx.MarkedActivityIDs) == 0 {
		return nil
	}
	ids := make([]string, 0, len(metadataCtx.MarkedActivityIDs))
	for id := range metadataCtx.MarkedActivityIDs {
		ids = append(ids, id)
	}
	return ids
}

// getMetadataContext extracts metadata context from golang context.
func getMetadataContext(ctx context.Context) *metadataContext {
	metadataCtx := ctx.Value(metadataCtxKey)
	if metadataCtx == nil {
		return nil
	}
	mc, ok := metadataCtx.(*metadataContext)
	if !ok {
		return nil
	}
	return mc
}

// WithMetadataContext adds a metadata context to the given context.
func WithMetadataContext(ctx context.Context) context.Context {
	metadataCtx := &metadataContext{
		Metadata:          make(map[string]any),
		MarkedActivityIDs: make(map[string]struct{}),
	}
	return context.WithValue(ctx, metadataCtxKey, metadataCtx)
}

// ContextHasMetadata returns true if the context has metadata support.
// This can be used to debug whether a context has been properly initialized with metadata.
func ContextHasMetadata(ctx context.Context) bool {
	return getMetadataContext(ctx) != nil
}

// ContextMetadataSet sets a metadata key-value pair in the context, overwriting any existing value.
func ContextMetadataSet(ctx context.Context, key string, value any) bool {
	metadataCtx := getMetadataContext(ctx)
	if metadataCtx == nil {
		return false
	}

	metadataCtx.Lock()
	defer metadataCtx.Unlock()

	metadataCtx.Metadata[key] = value
	return true
}

// ContextMetadataGet retrieves a metadata value from the context.
func ContextMetadataGet(ctx context.Context, key string) (any, bool) {
	metadataCtx := getMetadataContext(ctx)
	if metadataCtx == nil {
		return nil, false
	}

	metadataCtx.Lock()
	defer metadataCtx.Unlock()

	value, ok := metadataCtx.Metadata[key]
	return value, ok
}

// ContextMetadataGetAll retrieves all metadata from the context as a map copy.
func ContextMetadataGetAll(ctx context.Context) map[string]any {
	metadataCtx := getMetadataContext(ctx)
	if metadataCtx == nil {
		return nil
	}

	metadataCtx.Lock()
	defer metadataCtx.Unlock()

	// Return a copy to prevent external modifications
	result := make(map[string]any, len(metadataCtx.Metadata))
	for k, v := range metadataCtx.Metadata {
		result[k] = v
	}
	return result
}
