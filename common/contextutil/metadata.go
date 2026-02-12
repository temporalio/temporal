package contextutil

import (
	"context"
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
)

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

// ContextMetadataSet sets a metadata key-value pair in the context.
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
