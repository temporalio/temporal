package contextutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddMetadataContext(t *testing.T) {
	t.Run("adds metadata context to empty context", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithMetadataContext(ctx)

		metadataCtx := getMetadataContext(ctx)
		require.NotNil(t, metadataCtx)
		require.NotNil(t, metadataCtx.Metadata)
		require.Empty(t, metadataCtx.Metadata)
	})

	t.Run("returns new context with metadata", func(t *testing.T) {
		ctx := context.Background()
		ctxWithMetadata := WithMetadataContext(ctx)

		require.NotEqual(t, ctx, ctxWithMetadata)
		require.Nil(t, getMetadataContext(ctx))
		require.NotNil(t, getMetadataContext(ctxWithMetadata))
	})
}

func TestContextMetadataSet(t *testing.T) {
	t.Run("sets string value successfully", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		success := ContextMetadataSet(ctx, "key1", "value1")
		require.True(t, success)

		metadataCtx := getMetadataContext(ctx)
		require.NotNil(t, metadataCtx)
		require.Equal(t, "value1", metadataCtx.Metadata["key1"])
	})

	t.Run("sets multiple values successfully", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		ContextMetadataSet(ctx, "key1", "value1")
		ContextMetadataSet(ctx, "key2", 42)
		ContextMetadataSet(ctx, "key3", true)

		metadataCtx := getMetadataContext(ctx)
		require.NotNil(t, metadataCtx)
		require.Equal(t, "value1", metadataCtx.Metadata["key1"])
		require.Equal(t, 42, metadataCtx.Metadata["key2"])
		require.Equal(t, true, metadataCtx.Metadata["key3"])
	})

	t.Run("overwrites existing value", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		ContextMetadataSet(ctx, "key1", "value1")
		ContextMetadataSet(ctx, "key1", "value2")

		metadataCtx := getMetadataContext(ctx)
		require.NotNil(t, metadataCtx)
		require.Equal(t, "value2", metadataCtx.Metadata["key1"])
	})

	t.Run("returns false when context has no metadata", func(t *testing.T) {
		ctx := context.Background()

		success := ContextMetadataSet(ctx, "key1", "value1")
		require.False(t, success)
	})

	t.Run("supports various value types", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		type customStruct struct {
			Field string
		}

		testCases := []struct {
			key   string
			value any
		}{
			{"string", "test"},
			{"int", 123},
			{"float", 3.14},
			{"bool", true},
			{"slice", []string{"a", "b", "c"}},
			{"map", map[string]int{"a": 1, "b": 2}},
			{"struct", customStruct{Field: "test"}},
			{"nil", nil},
		}

		for _, tc := range testCases {
			t.Run(tc.key, func(t *testing.T) {
				success := ContextMetadataSet(ctx, tc.key, tc.value)
				require.True(t, success)

				metadataCtx := getMetadataContext(ctx)
				require.NotNil(t, metadataCtx)
				require.Equal(t, tc.value, metadataCtx.Metadata[tc.key])
			})
		}
	})
}

func TestContextMetadataGet(t *testing.T) {
	t.Run("retrieves existing value", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, "key1", "value1")

		value, ok := ContextMetadataGet(ctx, "key1")
		require.True(t, ok)
		require.Equal(t, "value1", value)
	})

	t.Run("returns false for non-existent key", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		value, ok := ContextMetadataGet(ctx, "nonexistent")
		require.False(t, ok)
		require.Nil(t, value)
	})

	t.Run("returns false when context has no metadata", func(t *testing.T) {
		ctx := context.Background()

		value, ok := ContextMetadataGet(ctx, "key1")
		require.False(t, ok)
		require.Nil(t, value)
	})

	t.Run("retrieves nil value correctly", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, "nilKey", nil)

		value, ok := ContextMetadataGet(ctx, "nilKey")
		require.True(t, ok)
		require.Nil(t, value)
	})

	t.Run("retrieves various value types", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		ContextMetadataSet(ctx, "string", "test")
		ContextMetadataSet(ctx, "int", 42)
		ContextMetadataSet(ctx, "slice", []int{1, 2, 3})

		strVal, ok := ContextMetadataGet(ctx, "string")
		require.True(t, ok)
		require.Equal(t, "test", strVal)

		intVal, ok := ContextMetadataGet(ctx, "int")
		require.True(t, ok)
		require.Equal(t, 42, intVal)

		sliceVal, ok := ContextMetadataGet(ctx, "slice")
		require.True(t, ok)
		require.Equal(t, []int{1, 2, 3}, sliceVal)
	})
}

func TestContextMetadataGetAll(t *testing.T) {
	t.Run("retrieves all metadata", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, "key1", "value1")
		ContextMetadataSet(ctx, "key2", 42)

		allMetadata := ContextMetadataGetAll(ctx)
		require.NotNil(t, allMetadata)
		require.Len(t, allMetadata, 2)
		require.Equal(t, "value1", allMetadata["key1"])
		require.Equal(t, 42, allMetadata["key2"])
	})

	t.Run("returns nil when context has no metadata", func(t *testing.T) {
		ctx := context.Background()

		allMetadata := ContextMetadataGetAll(ctx)
		require.Nil(t, allMetadata)
	})

	t.Run("returns empty map when no metadata set", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		allMetadata := ContextMetadataGetAll(ctx)
		require.NotNil(t, allMetadata)
		require.Empty(t, allMetadata)
	})

	t.Run("returned map is a copy", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, "key1", "value1")

		allMetadata := ContextMetadataGetAll(ctx)
		allMetadata["key2"] = "value2"

		// Original should not be affected
		_, ok := ContextMetadataGet(ctx, "key2")
		require.False(t, ok)
	})
}

func TestGetMetadataContext(t *testing.T) {
	t.Run("returns nil for context without metadata", func(t *testing.T) {
		ctx := context.Background()
		metadataCtx := getMetadataContext(ctx)
		require.Nil(t, metadataCtx)
	})

	t.Run("returns metadata context when present", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		metadataCtx := getMetadataContext(ctx)
		require.NotNil(t, metadataCtx)
	})

	t.Run("returns nil for wrong type in context", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), metadataCtxKey, "wrong type")
		metadataCtx := getMetadataContext(ctx)
		require.Nil(t, metadataCtx)
	})
}

func TestMetadataContextWithContextCancellation(t *testing.T) {
	t.Run("metadata survives context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		ctx = WithMetadataContext(ctx)

		ContextMetadataSet(ctx, "key1", "value1")

		// Cancel the context
		cancel()

		// Metadata should still be accessible
		value, ok := ContextMetadataGet(ctx, "key1")
		require.True(t, ok)
		require.Equal(t, "value1", value)

		// Should still be able to set new values
		success := ContextMetadataSet(ctx, "key2", "value2")
		require.True(t, success)

		value, ok = ContextMetadataGet(ctx, "key2")
		require.True(t, ok)
		require.Equal(t, "value2", value)
	})
}

func TestMetadataContextIsolation(t *testing.T) {
	t.Run("contexts with different metadata are isolated", func(t *testing.T) {
		ctx1 := WithMetadataContext(context.Background())
		ctx2 := WithMetadataContext(context.Background())

		ContextMetadataSet(ctx1, "key", "value1")
		ContextMetadataSet(ctx2, "key", "value2")

		value1, ok1 := ContextMetadataGet(ctx1, "key")
		value2, ok2 := ContextMetadataGet(ctx2, "key")

		require.True(t, ok1)
		require.True(t, ok2)
		require.Equal(t, "value1", value1)
		require.Equal(t, "value2", value2)
	})

	t.Run("child context does not inherit parent metadata", func(t *testing.T) {
		parentCtx := WithMetadataContext(context.Background())
		ContextMetadataSet(parentCtx, "key", "parent-value")

		type testContextKey string
		childCtx := context.WithValue(parentCtx, testContextKey("other-key"), "other-value")

		// Child can still access parent's metadata context
		value, ok := ContextMetadataGet(childCtx, "key")
		require.True(t, ok)
		require.Equal(t, "parent-value", value)

		// Setting in child affects parent (same metadata context)
		ContextMetadataSet(childCtx, "key2", "child-value")
		value, ok = ContextMetadataGet(parentCtx, "key2")
		require.True(t, ok)
		require.Equal(t, "child-value", value)
	})

	t.Run("adding metadata context twice creates new isolated context", func(t *testing.T) {
		ctx1 := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx1, "key", "value1")

		ctx2 := WithMetadataContext(ctx1)
		ContextMetadataSet(ctx2, "key", "value2")

		value1, ok1 := ContextMetadataGet(ctx1, "key")
		value2, ok2 := ContextMetadataGet(ctx2, "key")

		require.True(t, ok1)
		require.True(t, ok2)
		require.Equal(t, "value1", value1)
		require.Equal(t, "value2", value2)
	})
}

func TestContextHasMetadata(t *testing.T) {
	t.Run("returns true when context has metadata", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		require.True(t, ContextHasMetadata(ctx))
	})

	t.Run("returns false for context without metadata", func(t *testing.T) {
		ctx := context.Background()
		require.False(t, ContextHasMetadata(ctx))
	})

	t.Run("returns true after setting metadata values", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, "key1", "value1")
		ContextMetadataSet(ctx, "key2", "value2")

		require.True(t, ContextHasMetadata(ctx))
	})

	t.Run("returns true for empty metadata context", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		// No values set, but metadata context exists
		require.True(t, ContextHasMetadata(ctx))
	})

	t.Run("child context inherits metadata from parent", func(t *testing.T) {
		parentCtx := WithMetadataContext(context.Background())
		ContextMetadataSet(parentCtx, "key", "value")

		type testContextKey string
		childCtx := context.WithValue(parentCtx, testContextKey("other-key"), "other-value")

		require.True(t, ContextHasMetadata(parentCtx))
		require.True(t, ContextHasMetadata(childCtx))
	})

	t.Run("returns false for wrong type in context", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), metadataCtxKey, "wrong type")
		require.False(t, ContextHasMetadata(ctx))
	})

	t.Run("returns true for cancelled context with metadata", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		ctx = WithMetadataContext(ctx)
		cancel()

		require.True(t, ContextHasMetadata(ctx))
	})

	t.Run("multiple contexts with metadata are independent", func(t *testing.T) {
		ctx1 := WithMetadataContext(context.Background())
		ctx2 := WithMetadataContext(context.Background())
		ctx3 := context.Background()

		require.True(t, ContextHasMetadata(ctx1))
		require.True(t, ContextHasMetadata(ctx2))
		require.False(t, ContextHasMetadata(ctx3))
	})
}

func TestContextMetadataGetActivityTypeAndTaskQueue(t *testing.T) {
	t.Run("returns false without metadata context", func(t *testing.T) {
		ctx := context.Background()
		actType, taskQueue, ok := ContextMetadataGetActivityTypeAndTaskQueue(ctx)
		require.False(t, ok)
		require.Empty(t, actType)
		require.Empty(t, taskQueue)
	})

	t.Run("returns false when no activity metadata", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		actType, taskQueue, ok := ContextMetadataGetActivityTypeAndTaskQueue(ctx)
		require.False(t, ok)
		require.Empty(t, actType)
		require.Empty(t, taskQueue)
	})

	t.Run("returns single activity type and task queue", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, ActivityTypeKey(42), "my-activity")
		ContextMetadataSet(ctx, ActivityTaskQueueKey(42), "my-queue")

		actType, taskQueue, ok := ContextMetadataGetActivityTypeAndTaskQueue(ctx)
		require.True(t, ok)
		require.Equal(t, "my-activity", actType)
		require.Equal(t, "my-queue", taskQueue)
	})

	t.Run("returns false when multiple activities present", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, ActivityTypeKey(1), "activity-a")
		ContextMetadataSet(ctx, ActivityTaskQueueKey(1), "queue-a")
		ContextMetadataSet(ctx, ActivityTypeKey(2), "activity-b")
		ContextMetadataSet(ctx, ActivityTaskQueueKey(2), "queue-b")

		actType, taskQueue, ok := ContextMetadataGetActivityTypeAndTaskQueue(ctx)
		require.False(t, ok)
		require.Empty(t, actType)
		require.Empty(t, taskQueue)
	})

	t.Run("returns false when only activity type present", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, ActivityTypeKey(1), "my-activity")

		actType, taskQueue, ok := ContextMetadataGetActivityTypeAndTaskQueue(ctx)
		require.False(t, ok)
		require.Empty(t, taskQueue)
		_ = actType
	})

	t.Run("returns false when only task queue present", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, ActivityTaskQueueKey(1), "my-queue")

		actType, taskQueue, ok := ContextMetadataGetActivityTypeAndTaskQueue(ctx)
		require.False(t, ok)
		require.Empty(t, actType)
		_ = taskQueue
	})

	t.Run("ignores workflow metadata keys", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, MetadataKeyWorkflowType, "my-workflow")
		ContextMetadataSet(ctx, MetadataKeyWorkflowTaskQueue, "workflow-queue")
		ContextMetadataSet(ctx, ActivityTypeKey(7), "my-activity")
		ContextMetadataSet(ctx, ActivityTaskQueueKey(7), "activity-queue")

		actType, taskQueue, ok := ContextMetadataGetActivityTypeAndTaskQueue(ctx)
		require.True(t, ok)
		require.Equal(t, "my-activity", actType)
		require.Equal(t, "activity-queue", taskQueue)
	})

	t.Run("ignores standalone activity metadata keys", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, MetadataKeyStandaloneActivityType, "standalone-act")
		ContextMetadataSet(ctx, MetadataKeyStandaloneActivityTaskQueue, "standalone-queue")
		ContextMetadataSet(ctx, ActivityTypeKey(3), "my-activity")
		ContextMetadataSet(ctx, ActivityTaskQueueKey(3), "my-queue")

		actType, taskQueue, ok := ContextMetadataGetActivityTypeAndTaskQueue(ctx)
		require.True(t, ok)
		require.Equal(t, "my-activity", actType)
		require.Equal(t, "my-queue", taskQueue)
	})
}

func TestActivityTypeKey(t *testing.T) {
	key := ActivityTypeKey(1)
	require.Equal(t, "activity-type-1", key)
	require.Equal(t, key, ActivityTypeKey(1))
	require.NotEqual(t, ActivityTypeKey(1), ActivityTypeKey(2))
}

func TestActivityTaskQueueKey(t *testing.T) {
	key := ActivityTaskQueueKey(1)
	require.Equal(t, "activity-task-queue-1", key)
	require.Equal(t, key, ActivityTaskQueueKey(1))
	require.NotEqual(t, ActivityTaskQueueKey(1), ActivityTaskQueueKey(2))
}

func TestContextMetadataMarkActivityID(t *testing.T) {
	t.Run("marks activity ID on valid context", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		require.True(t, ContextMetadataMarkActivityID(ctx, "act-1"))

		ids := ContextMetadataGetMarkedActivityIDs(ctx)
		require.Equal(t, []string{"act-1"}, ids)
	})

	t.Run("returns false without metadata context", func(t *testing.T) {
		ctx := context.Background()
		require.False(t, ContextMetadataMarkActivityID(ctx, "act-1"))
	})

	t.Run("marks multiple activity IDs", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		require.True(t, ContextMetadataMarkActivityID(ctx, "act-1"))
		require.True(t, ContextMetadataMarkActivityID(ctx, "act-2"))

		ids := ContextMetadataGetMarkedActivityIDs(ctx)
		require.ElementsMatch(t, []string{"act-1", "act-2"}, ids)
	})
}

func TestContextMetadataGetMarkedActivityIDs(t *testing.T) {
	t.Run("returns nil without metadata context", func(t *testing.T) {
		ctx := context.Background()
		require.Nil(t, ContextMetadataGetMarkedActivityIDs(ctx))
	})

	t.Run("returns nil when no activities marked", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		require.Nil(t, ContextMetadataGetMarkedActivityIDs(ctx))
	})

	t.Run("returns single marked activity ID", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataMarkActivityID(ctx, "act-1")

		ids := ContextMetadataGetMarkedActivityIDs(ctx)
		require.Equal(t, []string{"act-1"}, ids)
	})

	t.Run("returns multiple marked activity IDs", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataMarkActivityID(ctx, "act-1")
		ContextMetadataMarkActivityID(ctx, "act-2")

		ids := ContextMetadataGetMarkedActivityIDs(ctx)
		require.Len(t, ids, 2)
		require.ElementsMatch(t, []string{"act-1", "act-2"}, ids)
	})

	t.Run("ignores non-activity keys", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, MetadataKeyWorkflowType, "my-workflow")
		ContextMetadataSet(ctx, MetadataKeyWorkflowTaskQueue, "my-queue")
		ContextMetadataMarkActivityID(ctx, "act-1")

		ids := ContextMetadataGetMarkedActivityIDs(ctx)
		require.Equal(t, []string{"act-1"}, ids)
	})

	t.Run("marked IDs are separate from resolved metadata", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataMarkActivityID(ctx, "act-1")
		ContextMetadataSet(ctx, ActivityTaskQueueKey(5), "my-queue")

		ids := ContextMetadataGetMarkedActivityIDs(ctx)
		require.Equal(t, []string{"act-1"}, ids)

		val, ok := ContextMetadataGet(ctx, ActivityTaskQueueKey(5))
		require.True(t, ok)
		require.Equal(t, "my-queue", val)
	})
}
