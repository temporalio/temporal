package clock_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"google.golang.org/protobuf/types/known/durationpb"
)

func newTestTimeSkippingWrapper(base clock.TimeSource) *clock.TimeSkippingTimeSourceWrapper {
	return clock.WrapTimeSourceWithTimeSkippingInfo(base, nil).(*clock.TimeSkippingTimeSourceWrapper)
}

func TestTimeSkippingTimeSource_Now_NilInfo(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	ts := newTestTimeSkippingWrapper(base)

	ts.SetTimeSkippingInfo(nil)

	require.Equal(t, time.Unix(100, 0), ts.Now())
}

func TestTimeSkippingTimeSource_Now_WithNoDuration(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	ts := newTestTimeSkippingWrapper(base)

	ts.SetTimeSkippingInfo(&persistencespb.TimeSkippingInfo{})

	require.Equal(t, time.Unix(100, 0), ts.Now())
}

func TestTimeSkippingTimeSource_Now_WithSkipping(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	ts := newTestTimeSkippingWrapper(base)
	tsInfo := &persistencespb.TimeSkippingInfo{
		AccumulatedSkippedDuration: durationpb.New(10 * time.Second),
	}

	ts.SetTimeSkippingInfo(tsInfo)
	require.Equal(t, time.Unix(110, 0), ts.Now())
	tsInfo.AccumulatedSkippedDuration = durationpb.New(120 * time.Second)
	require.Equal(t, time.Unix(220, 0), ts.Now())
}

func TestTimeSkippingTimeSource_Since_WithSkipping(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	ts := newTestTimeSkippingWrapper(base)

	// Skipping offset does not affect Since — it delegates to the base.
	ts.SetTimeSkippingInfo(&persistencespb.TimeSkippingInfo{
		AccumulatedSkippedDuration: durationpb.New(50 * time.Second),
	})

	past := time.Unix(90, 0)
	require.Equal(t, 60*time.Second, ts.Since(past))
}

func TestTimeSkippingTimeSource_AfterFunc_DelegatesToBase(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	tsInfo := &persistencespb.TimeSkippingInfo{
		AccumulatedSkippedDuration: durationpb.New(50 * time.Second),
	}
	ts := clock.WrapTimeSourceWithTimeSkippingInfo(base, tsInfo)

	fired := false
	ts.AfterFunc(time.Second, func() { fired = true })

	require.False(t, fired)
	base.Advance(time.Second)
	require.True(t, fired)
}

func TestTimeSkippingTimeSource_NewTimer_DelegatesToBase(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	tsInfo := &persistencespb.TimeSkippingInfo{
		AccumulatedSkippedDuration: durationpb.New(50 * time.Second),
	}
	ts := clock.WrapTimeSourceWithTimeSkippingInfo(base, tsInfo)

	ch, _ := ts.NewTimer(time.Second)

	select {
	case <-ch:
		t.Fatal("timer should not fire before deadline")
	default:
	}

	base.Advance(time.Second)

	select {
	case <-ch:
		// fired as expected
	default:
		t.Fatal("timer should have fired after deadline")
	}
}
