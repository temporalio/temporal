package hybrid_logical_clock

import (
	"errors"
	"time"

	clockpb "go.temporal.io/server/api/clock/v1"
)

var ErrClocksEqual = errors.New("HybridLogicalClocks are equal")

type Clock = clockpb.HybridLogicalClock

// Next generates the next clock timestamp given the current clock.
// HybridLogicalClock requires the previous clock to ensure that time doesn't move backwards and the next clock is
// monotonically increasing.
func Next(clock Clock) Clock {
	wallclock := time.Now().UnixMilli()
	return next(clock, wallclock)
}

func next(clock Clock, wallclock int64) Clock {
	// Ensure time does not move backwards
	if wallclock < clock.GetWallClock() {
		wallclock = clock.GetWallClock()
	}
	// Ensure timestamp is monotonically increasing
	if wallclock == clock.GetWallClock() {
		clock.Version = clock.GetVersion() + 1
	} else {
		clock.Version = 0
		clock.WallClock = wallclock
	}

	return Clock{WallClock: wallclock, Version: clock.Version, ClusterId: clock.ClusterId}
}

// Zero generates a zeroed logical clock for the cluster ID.
func Zero(clusterID int64) Clock {
	return Clock{WallClock: 0, Version: 0, ClusterId: clusterID}
}

func sign[T int64 | int32](x T) int {
	if x > 0 {
		return 1
	}
	if x < 0 {
		return -1
	}
	return 0
}

// Compare 2 clocks, returns 0 if a == b, -1 if a > b, 1 if a < b
func Compare(a Clock, b Clock) int {
	if a.WallClock == b.WallClock {
		if a.Version == b.Version {
			return sign(b.ClusterId - a.ClusterId)
		}
		return sign(b.Version - a.Version)
	}
	return sign(b.WallClock - a.WallClock)
}

// Greater returns true if a is greater than b
func Greater(a Clock, b Clock) bool {
	return Compare(b, a) > 0
}

// Greater returns true if a is greater than b
func Less(a Clock, b Clock) bool {
	return Compare(a, b) > 0
}

// Max returns the maximum of two clocks
func Max(a Clock, b Clock) Clock {
	if Compare(a, b) > 0 {
		return b
	}
	return a
}

// Equal returns whether two clocks are equal
func Equal(a Clock, b Clock) bool {
	return Compare(a, b) == 0
}
