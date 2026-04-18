package contextutil

import (
	"context"
	"strings"
	"sync"
)

type (
	metadataContextKey struct{}

	// metadataContext is used to store workflow and activity metadata
	metadataContext struct {
		sync.Mutex
		Metadata map[string]any
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

// ActivityTypeKey returns the metadata key for the given activity ID's type.
func ActivityTypeKey(activityID string) string {
	return activityTypePrefix + activityID
}

// ActivityTaskQueueKey returns the metadata key for the given activity ID's task queue.
func ActivityTaskQueueKey(activityID string) string {
	return activityTaskQueuePrefix + activityID
}

// ContextMetadataMarkActivityID marks an activity ID on the context for metadata resolution.
// The handler knows which activity (from the task token) but not its type or task queue.
// Mutable state knows the activity details but not which activity the request targets.
// This bridges the two: the handler marks the ID, and SetContextMetadata (during
// closeTransaction) resolves it to type and task queue from mutable state.
// Cannot be used for transactions that remove the activity from mutable state
// (e.g., activity completion), since it won't be available for resolution.
func ContextMetadataMarkActivityID(ctx context.Context, activityID string) bool {
	return ContextMetadataSet(ctx, activityTypePrefix+activityID, "")
}

// ContextMetadataGetActivityIDs returns the unique activity IDs present in the context metadata.
func ContextMetadataGetActivityIDs(ctx context.Context) []string {
	metadataCtx := getMetadataContext(ctx)
	if metadataCtx == nil {
		return nil
	}

	metadataCtx.Lock()
	defer metadataCtx.Unlock()

	seen := make(map[string]struct{})
	for key := range metadataCtx.Metadata {
		if id, ok := strings.CutPrefix(key, activityTypePrefix); ok {
			seen[id] = struct{}{}
		}
	}

	if len(seen) == 0 {
		return nil
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
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
		Metadata: make(map[string]any),
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
