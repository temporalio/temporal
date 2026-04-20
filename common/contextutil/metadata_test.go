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

func TestActivityTypeKey(t *testing.T) {
	key := ActivityTypeKey("act-1")
	require.Regexp(t, `^activity-type-[0-9a-f]{16}$`, key)
	require.Equal(t, key, ActivityTypeKey("act-1"))
	require.NotEqual(t, ActivityTypeKey("act-1"), ActivityTypeKey("act-2"))

	unsafeKey := ActivityTypeKey("send email #1 (urgent)! / 订单-42 + foo 🚀")
	require.Regexp(t, `^activity-type-[0-9a-f]{16}$`, unsafeKey)
}

func TestActivityTaskQueueKey(t *testing.T) {
	key := ActivityTaskQueueKey("act-1")
	require.Regexp(t, `^activity-task-queue-[0-9a-f]{16}$`, key)
	require.Equal(t, key, ActivityTaskQueueKey("act-1"))
	require.NotEqual(t, ActivityTaskQueueKey("act-1"), ActivityTaskQueueKey("act-2"))

	unsafeKey := ActivityTaskQueueKey("send email #1 (urgent)! / 订单-42 + foo 🚀")
	require.Regexp(t, `^activity-task-queue-[0-9a-f]{16}$`, unsafeKey)
}

func TestContextMetadataMarkActivityID(t *testing.T) {
	t.Run("marks activity ID on valid context", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		require.True(t, ContextMetadataMarkActivityID(ctx, "act-1"))

		ids := ContextMetadataGetActivityIDs(ctx)
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

		ids := ContextMetadataGetActivityIDs(ctx)
		require.ElementsMatch(t, []string{"act-1", "act-2"}, ids)
	})
}

func TestContextMetadataGetActivityIDs(t *testing.T) {
	t.Run("returns nil without metadata context", func(t *testing.T) {
		ctx := context.Background()
		require.Nil(t, ContextMetadataGetActivityIDs(ctx))
	})

	t.Run("returns nil when no activities marked", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		require.Nil(t, ContextMetadataGetActivityIDs(ctx))
	})

	t.Run("returns single marked activity ID", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataMarkActivityID(ctx, "act-1")

		ids := ContextMetadataGetActivityIDs(ctx)
		require.Equal(t, []string{"act-1"}, ids)
	})

	t.Run("returns multiple marked activity IDs", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataMarkActivityID(ctx, "act-1")
		ContextMetadataMarkActivityID(ctx, "act-2")

		ids := ContextMetadataGetActivityIDs(ctx)
		require.Len(t, ids, 2)
		require.ElementsMatch(t, []string{"act-1", "act-2"}, ids)
	})

	t.Run("ignores non-activity keys", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, MetadataKeyWorkflowType, "my-workflow")
		ContextMetadataSet(ctx, MetadataKeyWorkflowTaskQueue, "my-queue")
		ContextMetadataMarkActivityID(ctx, "act-1")

		ids := ContextMetadataGetActivityIDs(ctx)
		require.Equal(t, []string{"act-1"}, ids)
	})

	t.Run("marked IDs are separate from resolved metadata", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataMarkActivityID(ctx, "act-1")
		ContextMetadataSet(ctx, ActivityTaskQueueKey("act-1"), "my-queue")

		ids := ContextMetadataGetActivityIDs(ctx)
		require.Equal(t, []string{"act-1"}, ids)

		// Resolved metadata uses hashed keys, separate from marked IDs
		val, ok := ContextMetadataGet(ctx, ActivityTaskQueueKey("act-1"))
		require.True(t, ok)
		require.Equal(t, "my-queue", val)
	})
}
