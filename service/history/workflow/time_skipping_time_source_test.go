package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTimeSkippingTimeSource_NowWithNoSkip(t *testing.T) {
	base := clock.NewEventTimeSource()
	base.Update(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	ts := newTimeSkippingTimeSource(base, nil)

	assert.Equal(t, base.Now(), ts.Now())
}

func TestTimeSkippingTimeSource_NowAfterAdvance(t *testing.T) {
	base := clock.NewEventTimeSource()
	realNow := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	base.Update(realNow)

	ts := newTimeSkippingTimeSource(base, nil)
	ts.advance(24 * time.Hour)

	assert.Equal(t, realNow.Add(24*time.Hour), ts.Now())
	// base is unchanged
	assert.Equal(t, realNow, base.Now())
}

func TestTimeSkippingTimeSource_NowWithMultipleAdvances(t *testing.T) {
	base := clock.NewEventTimeSource()
	realNow := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	base.Update(realNow)

	ts := newTimeSkippingTimeSource(base, nil)
	ts.advance(24 * time.Hour)
	ts.advance(48 * time.Hour)

	assert.Equal(t, realNow.Add(72*time.Hour), ts.Now())
}

func TestTimeSkippingTimeSource_ReconstructedFromPersistedDetails(t *testing.T) {
	base := clock.NewEventTimeSource()
	realNow := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	base.Update(realNow)

	// Simulate two previously persisted skip events: 10h + 5h = 15h total offset
	details := []*persistencespb.TimeSkippedDetails{
		{DurationToSkip: timeSkippedDurationToTimestamp(10 * time.Hour)},
		{DurationToSkip: timeSkippedDurationToTimestamp(5 * time.Hour)},
	}

	ts := newTimeSkippingTimeSource(base, details)

	assert.Equal(t, realNow.Add(15*time.Hour), ts.Now())
}

func TestTimeSkippingTimeSource_Since(t *testing.T) {
	base := clock.NewEventTimeSource()
	realNow := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	base.Update(realNow)

	ts := newTimeSkippingTimeSource(base, nil)
	ts.advance(10 * time.Hour)

	past := realNow.Add(-5 * time.Hour) // 5 hours before real now
	// virtual now = realNow + 10h, so since(past) = 15h
	assert.Equal(t, 15*time.Hour, ts.Since(past))
}

func TestTimeSkippingTimeSource_DelegatesTimersToBase(t *testing.T) {
	base := clock.NewEventTimeSource()
	base.Update(time.Now())

	ts := newTimeSkippingTimeSource(base, nil)

	fired := false
	ts.AfterFunc(time.Millisecond, func() { fired = true })
	base.Advance(time.Millisecond)

	assert.True(t, fired, "AfterFunc should fire via base time source")
}

func TestComputeTotalSkippedOffset(t *testing.T) {
	details := []*persistencespb.TimeSkippedDetails{
		{DurationToSkip: timeSkippedDurationToTimestamp(3 * time.Hour)},
		{DurationToSkip: timeSkippedDurationToTimestamp(7 * time.Hour)},
		{DurationToSkip: timeSkippedDurationToTimestamp(2 * time.Hour)},
	}
	assert.Equal(t, 12*time.Hour, computeTotalSkippedOffset(details))
}

func TestComputeTotalSkippedOffset_Empty(t *testing.T) {
	assert.Equal(t, time.Duration(0), computeTotalSkippedOffset(nil))
	assert.Equal(t, time.Duration(0), computeTotalSkippedOffset([]*persistencespb.TimeSkippedDetails{}))
}

// roundtrip helper — verifies encoding/decoding is consistent
func TestTimeSkippedDurationRoundtrip(t *testing.T) {
	durations := []time.Duration{
		0,
		time.Second,
		time.Hour,
		24 * time.Hour,
		365 * 24 * time.Hour,
		time.Hour + 30*time.Minute + 15*time.Second,
	}
	for _, d := range durations {
		ts := timeSkippedDurationToTimestamp(d)
		assert.Equal(t, d, timeSkippedDurationFromTimestamp(ts))
	}
}

func TestTimeSkippedDurationFromTimestamp_Nil(t *testing.T) {
	assert.Equal(t, time.Duration(0), timeSkippedDurationFromTimestamp((*timestamppb.Timestamp)(nil)))
}
