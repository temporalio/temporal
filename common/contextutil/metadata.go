package contextutil

import (
	"context"
	"encoding/json"
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
	// metadataKeyActivityMetadata is the context metadata key for paired activity type and task queue metadata.
	// A single request (e.g., RespondWorkflowTaskCompleted) can carry multiple activity types and task queues,
	// so these are stored together as a JSON-serialized []ActivityMetadata.
	// Use ContextMetadataAddActivity and ContextMetadataGetActivities instead of ContextMetadataSet/Get for this key.
	metadataKeyActivityMetadata = "activity-metadata"
)

// PropagatedMetadataKeys returns the explicit allowlist of context metadata keys propagated
// via gRPC trailers across service boundaries. Only selected keys are included to avoid
// accidentally sending large payloads.
func PropagatedMetadataKeys() []string {
	return []string{
		MetadataKeyWorkflowType,
		MetadataKeyWorkflowTaskQueue,
		metadataKeyActivityMetadata,
	}
}

// ActivityMetadata represents a paired activity type and task queue.
type ActivityMetadata struct {
	ActivityType string `json:"activityType"`
	TaskQueue    string `json:"taskQueue"`
}

// serializeActivityMetadata serializes activity metadata pairs to a JSON string
// for trailer propagation.
func serializeActivityMetadata(activities []ActivityMetadata) (string, error) {
	data, err := json.Marshal(activities)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// deserializeActivityMetadata deserializes activity metadata pairs from a JSON string.
func deserializeActivityMetadata(value string) ([]ActivityMetadata, error) {
	var activities []ActivityMetadata
	if err := json.Unmarshal([]byte(value), &activities); err != nil {
		return nil, err
	}
	return activities, nil
}

// ContextMetadataAddActivity appends an activity's type and task queue to the context metadata.
// Returns false if the context has no metadata support.
// This operation is atomic — the entire read-modify-write is performed under a single lock.
func ContextMetadataAddActivity(ctx context.Context, activityType, taskQueue string) bool {
	metadataCtx := getMetadataContext(ctx)
	if metadataCtx == nil {
		return false
	}

	metadataCtx.Lock()
	defer metadataCtx.Unlock()

	var existing []ActivityMetadata
	if raw, ok := metadataCtx.Metadata[metadataKeyActivityMetadata]; ok {
		str, ok := raw.(string)
		if !ok {
			return false
		}
		var err error
		existing, err = deserializeActivityMetadata(str)
		if err != nil {
			return false
		}
	}

	activities := append(existing, ActivityMetadata{
		ActivityType: activityType,
		TaskQueue:    taskQueue,
	})
	serialized, err := serializeActivityMetadata(activities)
	if err != nil {
		return false
	}
	metadataCtx.Metadata[metadataKeyActivityMetadata] = serialized
	return true
}

// ContextMetadataGetActivities retrieves all activity metadata from the context.
// Returns nil if the context has no metadata support or no activities have been added.
func ContextMetadataGetActivities(ctx context.Context) []ActivityMetadata {
	value, ok := ContextMetadataGet(ctx, metadataKeyActivityMetadata)
	if !ok {
		return nil
	}
	str, ok := value.(string)
	if !ok {
		return nil
	}
	activities, err := deserializeActivityMetadata(str)
	if err != nil {
		return nil
	}
	return activities
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
// For metadataKeyActivityMetadata, the value must be a valid JSON string (as produced by
// the trailer interceptor). Unlike ContextMetadataAddActivity which appends atomically,
// this overwrites the entire activity metadata — use ContextMetadataAddActivity for programmatic use.
func ContextMetadataSet(ctx context.Context, key string, value any) bool {
	metadataCtx := getMetadataContext(ctx)
	if metadataCtx == nil {
		return false
	}

	// ContextMetadataAddActivity is preferred for this key, but we allow direct Set as long as the
	// value is valid JSON. Validates on write to catch corruption early, before it propagates through
	// gRPC trailers to other services where it would silently fail to deserialize.
	if key == metadataKeyActivityMetadata {
		str, ok := value.(string)
		if !ok {
			return false
		}
		if _, err := deserializeActivityMetadata(str); err != nil {
			return false
		}
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
