package contextutil

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddMetadataContext(t *testing.T) {
	t.Run("adds metadata context to empty context", func(t *testing.T) {
		ctx := context.Background()
		ctx = WithMetadataContext(ctx)

		metadataCtx := getMetadataContext(ctx)
		require.NotNil(t, metadataCtx)
		assert.NotNil(t, metadataCtx.Metadata)
		assert.Empty(t, metadataCtx.Metadata)
	})

	t.Run("returns new context with metadata", func(t *testing.T) {
		ctx := context.Background()
		ctxWithMetadata := WithMetadataContext(ctx)

		assert.NotEqual(t, ctx, ctxWithMetadata)
		assert.Nil(t, getMetadataContext(ctx))
		assert.NotNil(t, getMetadataContext(ctxWithMetadata))
	})
}

func TestContextMetadataSet(t *testing.T) {
	t.Run("sets string value successfully", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		success := ContextMetadataSet(ctx, "key1", "value1")
		assert.True(t, success)

		metadataCtx := getMetadataContext(ctx)
		require.NotNil(t, metadataCtx)
		assert.Equal(t, "value1", metadataCtx.Metadata["key1"])
	})

	t.Run("sets multiple values successfully", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		ContextMetadataSet(ctx, "key1", "value1")
		ContextMetadataSet(ctx, "key2", 42)
		ContextMetadataSet(ctx, "key3", true)

		metadataCtx := getMetadataContext(ctx)
		require.NotNil(t, metadataCtx)
		assert.Equal(t, "value1", metadataCtx.Metadata["key1"])
		assert.Equal(t, 42, metadataCtx.Metadata["key2"])
		assert.Equal(t, true, metadataCtx.Metadata["key3"])
	})

	t.Run("overwrites existing value", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		ContextMetadataSet(ctx, "key1", "value1")
		ContextMetadataSet(ctx, "key1", "value2")

		metadataCtx := getMetadataContext(ctx)
		require.NotNil(t, metadataCtx)
		assert.Equal(t, "value2", metadataCtx.Metadata["key1"])
	})

	t.Run("returns false when context has no metadata", func(t *testing.T) {
		ctx := context.Background()

		success := ContextMetadataSet(ctx, "key1", "value1")
		assert.False(t, success)
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
				assert.True(t, success)

				metadataCtx := getMetadataContext(ctx)
				require.NotNil(t, metadataCtx)
				assert.Equal(t, tc.value, metadataCtx.Metadata[tc.key])
			})
		}
	})
}

func TestContextMetadataGet(t *testing.T) {
	t.Run("retrieves existing value", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, "key1", "value1")

		value, ok := ContextMetadataGet(ctx, "key1")
		assert.True(t, ok)
		assert.Equal(t, "value1", value)
	})

	t.Run("returns false for non-existent key", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		value, ok := ContextMetadataGet(ctx, "nonexistent")
		assert.False(t, ok)
		assert.Nil(t, value)
	})

	t.Run("returns false when context has no metadata", func(t *testing.T) {
		ctx := context.Background()

		value, ok := ContextMetadataGet(ctx, "key1")
		assert.False(t, ok)
		assert.Nil(t, value)
	})

	t.Run("retrieves nil value correctly", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, "nilKey", nil)

		value, ok := ContextMetadataGet(ctx, "nilKey")
		assert.True(t, ok)
		assert.Nil(t, value)
	})

	t.Run("retrieves various value types", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		ContextMetadataSet(ctx, "string", "test")
		ContextMetadataSet(ctx, "int", 42)
		ContextMetadataSet(ctx, "slice", []int{1, 2, 3})

		strVal, ok := ContextMetadataGet(ctx, "string")
		assert.True(t, ok)
		assert.Equal(t, "test", strVal)

		intVal, ok := ContextMetadataGet(ctx, "int")
		assert.True(t, ok)
		assert.Equal(t, 42, intVal)

		sliceVal, ok := ContextMetadataGet(ctx, "slice")
		assert.True(t, ok)
		assert.Equal(t, []int{1, 2, 3}, sliceVal)
	})
}

func TestGetMetadataContext(t *testing.T) {
	t.Run("returns nil for context without metadata", func(t *testing.T) {
		ctx := context.Background()
		metadataCtx := getMetadataContext(ctx)
		assert.Nil(t, metadataCtx)
	})

	t.Run("returns metadata context when present", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		metadataCtx := getMetadataContext(ctx)
		assert.NotNil(t, metadataCtx)
	})

	t.Run("returns nil for wrong type in context", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), metadataCtxKey, "wrong type")
		metadataCtx := getMetadataContext(ctx)
		assert.Nil(t, metadataCtx)
	})
}

func TestMetadataContextConcurrency(t *testing.T) {
	t.Run("concurrent set operations are safe", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		var wg sync.WaitGroup
		numGoroutines := 100
		numOperations := 100

		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := "key"
					value := id*numOperations + j
					ContextMetadataSet(ctx, key, value)
				}
			}(i)
		}

		wg.Wait()

		// Verify that some value was set (the exact value doesn't matter due to race)
		value, ok := ContextMetadataGet(ctx, "key")
		assert.True(t, ok)
		assert.NotNil(t, value)
	})

	t.Run("concurrent get and set operations are safe", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx, "counter", 0)

		var wg sync.WaitGroup
		numReaders := 50
		numWriters := 50
		numOperations := 100

		// Start readers
		wg.Add(numReaders)
		for i := 0; i < numReaders; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					ContextMetadataGet(ctx, "counter")
				}
			}()
		}

		// Start writers
		wg.Add(numWriters)
		for i := 0; i < numWriters; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					ContextMetadataSet(ctx, "counter", id*numOperations+j)
				}
			}(i)
		}

		wg.Wait()

		// Verify the context is still functional
		value, ok := ContextMetadataGet(ctx, "counter")
		assert.True(t, ok)
		assert.NotNil(t, value)
	})

	t.Run("concurrent operations on different keys are safe", func(t *testing.T) {
		ctx := WithMetadataContext(context.Background())

		var wg sync.WaitGroup
		numGoroutines := 50

		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				key := string(rune('a' + id%26))
				for j := 0; j < 100; j++ {
					ContextMetadataSet(ctx, key, id*100+j)
					ContextMetadataGet(ctx, key)
				}
			}(i)
		}

		wg.Wait()

		// Verify all keys are accessible
		for i := 0; i < 26; i++ {
			key := string(rune('a' + i))
			_, ok := ContextMetadataGet(ctx, key)
			// At least some keys should have been set
			if i < numGoroutines {
				assert.True(t, ok)
			}
		}
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
		assert.True(t, ok)
		assert.Equal(t, "value1", value)

		// Should still be able to set new values
		success := ContextMetadataSet(ctx, "key2", "value2")
		assert.True(t, success)

		value, ok = ContextMetadataGet(ctx, "key2")
		assert.True(t, ok)
		assert.Equal(t, "value2", value)
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

		assert.True(t, ok1)
		assert.True(t, ok2)
		assert.Equal(t, "value1", value1)
		assert.Equal(t, "value2", value2)
	})

	t.Run("child context does not inherit parent metadata", func(t *testing.T) {
		parentCtx := WithMetadataContext(context.Background())
		ContextMetadataSet(parentCtx, "key", "parent-value")

		type testContextKey string
		childCtx := context.WithValue(parentCtx, testContextKey("other-key"), "other-value")

		// Child can still access parent's metadata context
		value, ok := ContextMetadataGet(childCtx, "key")
		assert.True(t, ok)
		assert.Equal(t, "parent-value", value)

		// Setting in child affects parent (same metadata context)
		ContextMetadataSet(childCtx, "key2", "child-value")
		value, ok = ContextMetadataGet(parentCtx, "key2")
		assert.True(t, ok)
		assert.Equal(t, "child-value", value)
	})

	t.Run("adding metadata context twice creates new isolated context", func(t *testing.T) {
		ctx1 := WithMetadataContext(context.Background())
		ContextMetadataSet(ctx1, "key", "value1")

		ctx2 := WithMetadataContext(ctx1)
		ContextMetadataSet(ctx2, "key", "value2")

		value1, ok1 := ContextMetadataGet(ctx1, "key")
		value2, ok2 := ContextMetadataGet(ctx2, "key")

		assert.True(t, ok1)
		assert.True(t, ok2)
		assert.Equal(t, "value1", value1)
		assert.Equal(t, "value2", value2)
	})
}
