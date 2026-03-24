package clock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTimeSkippingTimeSource_NowWithNoSkip(t *testing.T) {
	base := NewEventTimeSource()
	base.Update(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	ts := NewTimeSkippingTimeSource(base, nil)

	assert.Equal(t, base.Now(), ts.Now())
}

func TestTimeSkippingTimeSource_NowAfterAdvance(t *testing.T) {
	base := NewEventTimeSource()
	realNow := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	base.Update(realNow)

	ts := NewTimeSkippingTimeSource(base, nil)
	ts.Advance(24 * time.Hour)

	assert.Equal(t, realNow.Add(24*time.Hour), ts.Now())
	// base is unchanged
	assert.Equal(t, realNow, base.Now())
}

func TestTimeSkippingTimeSource_NowWithMultipleAdvances(t *testing.T) {
	base := NewEventTimeSource()
	realNow := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	base.Update(realNow)

	ts := NewTimeSkippingTimeSource(base, nil)
	ts.Advance(24 * time.Hour)
	ts.Advance(48 * time.Hour)

	assert.Equal(t, realNow.Add(72*time.Hour), ts.Now())
}

func TestTimeSkippingTimeSource_ReconstructedFromPersistedDetails(t *testing.T) {
	base := NewEventTimeSource()
	realNow := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	base.Update(realNow)

	// Simulate two previously persisted skip events: 10h + 5h = 15h total offset
	details := []*persistencespb.TimeSkippedDetails{
		{DurationToSkip: TimeSkippedDurationToTimestamp(10 * time.Hour)},
		{DurationToSkip: TimeSkippedDurationToTimestamp(5 * time.Hour)},
	}

	ts := NewTimeSkippingTimeSource(base, details)

	assert.Equal(t, realNow.Add(15*time.Hour), ts.Now())
}

func TestTimeSkippingTimeSource_Since(t *testing.T) {
	base := NewEventTimeSource()
	realNow := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	base.Update(realNow)

	ts := NewTimeSkippingTimeSource(base, nil)
	ts.Advance(10 * time.Hour)

	past := realNow.Add(-5 * time.Hour) // 5 hours before real now
	// virtual now = realNow + 10h, so since(past) = 15h
	assert.Equal(t, 15*time.Hour, ts.Since(past))
}

func TestTimeSkippingTimeSource_DelegatesTimersToBase(t *testing.T) {
	base := NewEventTimeSource()
	base.Update(time.Now())

	ts := NewTimeSkippingTimeSource(base, nil)

	fired := false
	ts.AfterFunc(time.Millisecond, func() { fired = true })
	base.Advance(time.Millisecond)

	assert.True(t, fired, "AfterFunc should fire via base time source")
}

func TestComputeTotalSkippedOffset(t *testing.T) {
	details := []*persistencespb.TimeSkippedDetails{
		{DurationToSkip: TimeSkippedDurationToTimestamp(3 * time.Hour)},
		{DurationToSkip: TimeSkippedDurationToTimestamp(7 * time.Hour)},
		{DurationToSkip: TimeSkippedDurationToTimestamp(2 * time.Hour)},
	}
	assert.Equal(t, 12*time.Hour, ComputeTotalSkippedOffset(details))
}

func TestComputeTotalSkippedOffset_Empty(t *testing.T) {
	assert.Equal(t, time.Duration(0), ComputeTotalSkippedOffset(nil))
	assert.Equal(t, time.Duration(0), ComputeTotalSkippedOffset([]*persistencespb.TimeSkippedDetails{}))
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
		ts := TimeSkippedDurationToTimestamp(d)
		assert.Equal(t, d, TimeSkippedDurationFromTimestamp(ts))
	}
}

func TestTimeSkippedDurationFromTimestamp_Nil(t *testing.T) {
	assert.Equal(t, time.Duration(0), TimeSkippedDurationFromTimestamp((*timestamppb.Timestamp)(nil)))
}
