package multioperation

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/testx/failpoint"
)

// TestUpdateWithStart_Checkpoint is not parallel, since AllocsPerRun doesn't allow it.
func TestUpdateWithStart_Checkpoint(t *testing.T) {
	// Without failpoints enabled, or with an idle registry, the call sites cost a nil check or an
	// atomic load, and allocate nothing.
	for name, source := range map[string]*failpoints.Source{
		"nil source":      nil,
		"disabled source": failpoints.NewSource(failpoints.Decision{}),
		"idle source":     failpoints.NewEnabledSource(),
	} {
		uws := &updateWithStart{namespaceId: "ns-id", failpointSource: source}
		for _, key := range []failpoint.Key{
			failpoints.UpdateWithStartBetweenLockAndStart,
			failpoints.UpdateWithStartClosingWorkflowRetry,
		} {
			require.NoError(t, uws.checkpoint(key), name)
			allocs := testing.AllocsPerRun(100, func() { _ = uws.checkpoint(key) })
			require.Zero(t, allocs, name)
		}
	}

	source := failpoints.NewEnabledSource()
	key := failpoints.UpdateWithStartBetweenLockAndStart.ForScope("ns-id")
	fp := failpoint.NewFailpoint[error](key)
	fp.Return(errors.New("injected"), failpoint.Always)
	source.Failpoints().Set(key, fp)
	uws := &updateWithStart{namespaceId: "ns-id", failpointSource: source}
	require.ErrorContains(t, uws.checkpoint(failpoints.UpdateWithStartBetweenLockAndStart), "injected")
	uws = &updateWithStart{namespaceId: "other-ns-id", failpointSource: source}
	require.NoError(t, uws.checkpoint(failpoints.UpdateWithStartBetweenLockAndStart))
}

func BenchmarkUpdateWithStart_Checkpoint_Off(b *testing.B) {
	for name, source := range map[string]*failpoints.Source{
		"disabled": failpoints.NewSource(failpoints.Decision{}),
		"idle":     failpoints.NewEnabledSource(),
	} {
		uws := &updateWithStart{namespaceId: "ns-id", failpointSource: source}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = uws.checkpoint(failpoints.UpdateWithStartBetweenLockAndStart)
			}
		})
	}
}
