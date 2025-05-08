package hybrid_logical_clock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
)

func Test_Next_ReturnsGreaterClock(t *testing.T) {
	t.Parallel()
	t0 := Zero(1)
	timesource := clock.NewEventTimeSource()

	// Same wallclock
	timesource.Update(time.Unix(0, 0).UTC())
	t1 := Next(t0, timesource)
	assert.Equal(t, 1, Compare(t0, t1))
	// Greater wallclock
	timesource.Update(time.Unix(0, 1).UTC())
	t2 := Next(t1, timesource)
	assert.Equal(t, 1, Compare(t1, t2))
}

func Test_Compare(t *testing.T) {
	t.Parallel()
	t0 := Clock{WallClock: 1, Version: 1, ClusterId: 1}
	t1 := Clock{WallClock: 1, Version: 1, ClusterId: 1}
	assert.Equal(t, 0, Compare(&t0, &t1))
	assert.True(t, Equal(&t0, &t1))

	t0 = Clock{WallClock: 1, Version: 1, ClusterId: 1}
	t1 = Clock{WallClock: 1, Version: 1, ClusterId: 2}
	assert.Equal(t, 1, Compare(&t0, &t1))
	// Let's get a -1 in there for sanity
	assert.Equal(t, -1, Compare(&t1, &t0))

	t0 = Clock{WallClock: 1, Version: 1, ClusterId: 1}
	t1 = Clock{WallClock: 1, Version: 2, ClusterId: 1}
	assert.Equal(t, 1, Compare(&t0, &t1))

	t0 = Clock{WallClock: 1, Version: 1, ClusterId: 1}
	t1 = Clock{WallClock: 2, Version: 1, ClusterId: 1}
	assert.Equal(t, 1, Compare(&t0, &t1))

	assert.True(t, Greater(&t1, &t0))
	assert.True(t, Less(&t0, &t1))
}

func Test_Max_ReturnsMaximum(t *testing.T) {
	t.Parallel()
	t0 := Zero(1)
	t1 := Zero(2)

	max := Max(t0, t1)
	assert.Equal(t, max, t1)
	// Just in case it doesn't work in reverse order...
	max = Max(t1, t0)
	assert.Equal(t, max, t1)
}

func Test_Min_ReturnsMinimum(t *testing.T) {
	t.Parallel()
	t0 := Zero(1)
	t1 := Zero(2)

	min := Min(t0, t1)
	assert.Equal(t, min, t0)
	// Just in case it doesn't work in reverse order...
	min = Min(t1, t0)
	assert.Equal(t, min, t0)
}

func Test_UTC_ReturnsTimeInUTC(t *testing.T) {
	t.Parallel()
	assert.Equal(t, time.Unix(0, 0).UTC(), UTC(Zero(0)))

	timesource := clock.NewEventTimeSource()
	now := time.Date(1999, 12, 31, 23, 59, 59, 999000000, time.UTC)
	timesource.Update(now)
	assert.Equal(t, now, UTC(Next(Zero(0), timesource)))
}
