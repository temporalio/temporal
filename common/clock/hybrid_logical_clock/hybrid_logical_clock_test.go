package hybrid_logical_clock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Next_ReturnsGreaterClock(t *testing.T) {
	t0 := Zero(1)
	// Same wallclock
	t1 := next(t0, 0)
	assert.Equal(t, Compare(t0, t1), 1)
	// Greater wallclock
	t2 := next(t1, 1)
	assert.Equal(t, Compare(t1, t2), 1)
}

func Test_Compare(t *testing.T) {
	var t0 Clock
	var t1 Clock

	t0 = Clock{WallClock: 1, Version: 1, ClusterId: 1}
	t1 = Clock{WallClock: 1, Version: 1, ClusterId: 1}
	assert.Equal(t, Compare(t0, t1), 0)
	assert.True(t, Equal(t0, t1))

	t0 = Clock{WallClock: 1, Version: 1, ClusterId: 1}
	t1 = Clock{WallClock: 1, Version: 1, ClusterId: 2}
	assert.Equal(t, Compare(t0, t1), 1)
	// Let's get a -1 in there for sanity
	assert.Equal(t, Compare(t1, t0), -1)

	t0 = Clock{WallClock: 1, Version: 1, ClusterId: 1}
	t1 = Clock{WallClock: 1, Version: 2, ClusterId: 1}
	assert.Equal(t, Compare(t0, t1), 1)

	t0 = Clock{WallClock: 1, Version: 1, ClusterId: 1}
	t1 = Clock{WallClock: 2, Version: 1, ClusterId: 1}
	assert.Equal(t, Compare(t0, t1), 1)

	assert.True(t, Greater(t1, t0))
	assert.True(t, Less(t0, t1))
}

func Test_Max_ReturnsMaximum(t *testing.T) {
	t0 := Zero(1)
	t1 := Zero(2)

	max := Max(t0, t1)
	assert.Equal(t, max, t1)
	// Just in case it doesn't work in reverse order...
	max = Max(t1, t0)
	assert.Equal(t, max, t1)
}
