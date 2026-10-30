package clock_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func TestTimeSkippingTimeSource_Now_NilGetter(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	ts := clock.WrapTimeSourceWithTimeSkipping(base, nil)

	require.Equal(t, time.Unix(100, 0), ts.Now())
}

func TestTimeSkippingTimeSource_Now_WithZeroOffset(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	ts := clock.WrapTimeSourceWithTimeSkipping(base, func() time.Duration { return 0 })

	require.Equal(t, time.Unix(100, 0), ts.Now())
}

func TestTimeSkippingTimeSource_Now_ReadsOffsetLazily(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	offset := 10 * time.Second
	ts := clock.WrapTimeSourceWithTimeSkipping(base, func() time.Duration { return offset })

	require.Equal(t, time.Unix(110, 0), ts.Now())

	offset = 120 * time.Second
	require.Equal(t, time.Unix(220, 0), ts.Now())
}

func TestTimeSkippingTimeSource_Since_IncludesOffset(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	base.Update(time.Unix(100, 0))
	ts := clock.WrapTimeSourceWithTimeSkipping(base, func() time.Duration { return 50 * time.Second })

	// Since delegates to Now(), which includes the offset: (100+50) - 90 = 60s.
	require.Equal(t, 60*time.Second, ts.Since(time.Unix(90, 0)))
}

func TestTimeSkippingTimeSource_AfterFunc_DelegatesToBase(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	ts := clock.WrapTimeSourceWithTimeSkipping(base, func() time.Duration { return 50 * time.Second })

	fired := false
	ts.AfterFunc(time.Second, func() { fired = true })

	require.False(t, fired)
	base.Advance(time.Second)
	require.True(t, fired)
}

func TestTimeSkippingTimeSource_NewTimer_DelegatesToBase(t *testing.T) {
	t.Parallel()

	base := clock.NewEventTimeSource()
	ts := clock.WrapTimeSourceWithTimeSkipping(base, func() time.Duration { return 50 * time.Second })

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
