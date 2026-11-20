package matching

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log/tag"
)

func TestMatchingEngineLogKey(t *testing.T) {
	t.Parallel()

	engine := &matchingEngineImpl{
		outstandingPollers: collection.NewSyncMap[string, context.CancelFunc](),
	}
	want := fmt.Sprintf("*matching.matchingEngineImpl(%p)", engine)
	require.Equal(t, want, tag.Any("key", engine).Value())

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		for range 1000 {
			engine.outstandingPollers.Set("poller", func() {})
			engine.outstandingPollers.Delete("poller")
		}
	})
	for range 1000 {
		require.Equal(t, want, tag.Any("key", engine).Value())
	}
}
