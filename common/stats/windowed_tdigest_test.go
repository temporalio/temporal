package stats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func n(seconds int) time.Time {
	return time.Unix(int64(seconds), 0)
}

type (
	RecordingValue struct {
		Value float64
		Time  time.Time
		Count uint64
	}
	TimeWindowedStatsTestCase struct {
		Name            string
		WindowConfig    WindowConfig
		Expectations    []TestExpectation
		RecordingValues []RecordingValue
	}
	TestExpectation func(t *testing.T, tc TimeWindowedStatsTestCase, stats *timeWindowedTDigest)
)

// TestWindowConfig is the default window configuration used in tests.
// It plays nicely with the "n" function above.
var TestWindowConfig = WindowConfig{
	WindowSize:  1 * time.Second,
	WindowCount: 10,
}
var TestWindowConfigBlanks = WindowConfig{
	WindowSize:         1 * time.Second,
	WindowCount:        10,
	FillBlankIntervals: true,
}

func computeSimpleAverage(values []RecordingValue) map[int]RecordingValue {
	averages := make(map[int]RecordingValue)
	for _, value := range values {
		existing := averages[value.Time.Second()]
		averages[value.Time.Second()] = RecordingValue{
			existing.Value + value.Value,
			existing.Time,
			existing.Count + value.Count,
		}
	}
	return averages
}

func countNonEmptyWindows(stats *timeWindowedTDigest) int {
	count := 0
	for _, bucket := range stats.windows {
		if bucket.tdigest != nil {
			count++
		}
	}
	return count
}

func incrementingData(start time.Time, count int) []RecordingValue {
	RecordingValues := make([]RecordingValue, count)
	for i := range count {
		RecordingValues[i] = RecordingValue{
			Value: float64(i),
			Time:  start.Add(time.Duration(i) * time.Second),
			Count: 1,
		}
	}
	return RecordingValues
}

func TestWindowedDigest(t *testing.T) {
	// Use when unique windows are less than the max window count
	simpleExpectations := []TestExpectation{
		func(t *testing.T, tc TimeWindowedStatsTestCase, stats *timeWindowedTDigest) {
			averages := computeSimpleAverage(tc.RecordingValues)
			require.Equal(t, len(averages), countNonEmptyWindows(stats))
			for bucket, value := range averages {
				window := stats.SubWindowForTime(n(bucket))
				require.InDelta(t, value.Value/float64(value.Count), window.TrimmedMean(0, 1.0), 0.01)
			}
		},
	}

	testCases := []TimeWindowedStatsTestCase{
		{"empty", TestWindowConfig, simpleExpectations, nil},
		{"single-value", TestWindowConfig, simpleExpectations, []RecordingValue{
			{float64(10), n(1), 1},
		}},
		{"full-buckets", TestWindowConfig, simpleExpectations,
			incrementingData(n(1), 10),
		},
		{"overflow", TestWindowConfig, []TestExpectation{
			func(t *testing.T, tc TimeWindowedStatsTestCase, stats *timeWindowedTDigest) {
				require.Equal(t, 10, countNonEmptyWindows(stats))
				// datapoints will be 10-19, giving us max=19, min=10, avg=14.5
				require.InDelta(t, 14.5, stats.TrimmedMean(0, 1.0), 0.01)
				require.InDelta(t, 19, stats.Quantile(1.0), 0.01)
				require.InDelta(t, 10, stats.Quantile(0.0), 0.01)
			},
		}, incrementingData(n(1), 20)},
		{"blank-drops-old-data", TestWindowConfigBlanks, []TestExpectation{
			func(t *testing.T, tc TimeWindowedStatsTestCase, stats *timeWindowedTDigest) {
				require.Equal(t, 10, countNonEmptyWindows(stats))
				// avg=max=min because the old datapoint expired
				require.InDelta(t, 20, stats.TrimmedMean(0, 1.0), 0.01)
				require.InDelta(t, 20, stats.Quantile(1.0), 0.01)
				require.InDelta(t, 20, stats.Quantile(0.0), 0.01)
			}}, []RecordingValue{
			{float64(10), n(1), 1},
			{float64(20), n(11), 1},
		}},
		{"fill-blanks-simple", TestWindowConfigBlanks, []TestExpectation{
			func(t *testing.T, tc TimeWindowedStatsTestCase, stats *timeWindowedTDigest) {
				require.Equal(t, 5, countNonEmptyWindows(stats))
				require.InDelta(t, 15, stats.TrimmedMean(0, 1.0), 0.01)
				require.InDelta(t, 20, stats.Quantile(1.0), 0.01)
				require.InDelta(t, 10, stats.Quantile(0.0), 0.01)
			}}, []RecordingValue{
			{float64(10), n(1), 1},
			{float64(20), n(5), 1},
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			stats := NewWindowedTDigest(tc.WindowConfig)
			for _, value := range tc.RecordingValues {
				stats.RecordMulti(value.Value, value.Time, value.Count)
			}
			for _, expectation := range tc.Expectations {
				expectation(t, tc, stats.(*timeWindowedTDigest))
			}
		})
	}
}

func TestWindowedDigest_OldTimestampDropped(t *testing.T) {
	stats := NewWindowedTDigest(TestWindowConfig)
	// Record at t=5, then try to record at t=1 which is before the earliest window.
	stats.Record(100, n(5))
	stats.Record(999, n(1))
	td := stats.(*timeWindowedTDigest)
	require.Equal(t, 1, countNonEmptyWindows(td))
	require.InDelta(t, 100, stats.Quantile(0.5), 0.01)
}

func TestWindowedDigest_GapTimestampDropped(t *testing.T) {
	// Without FillBlankIntervals, a gap between windows causes the value to be dropped.
	stats := NewWindowedTDigest(TestWindowConfig)
	// Create window at t=1 (covers [1,2)), then advance to t=5 (covers [5,6)).
	// t=3 falls in the gap [2,5) with no window.
	stats.Record(10, n(1))
	stats.Record(50, n(5))
	stats.Record(999, n(3)) // should be dropped (in gap)
	td := stats.(*timeWindowedTDigest)
	require.Equal(t, 2, countNonEmptyWindows(td))
	require.InDelta(t, 30, stats.TrimmedMean(0, 1.0), 0.01)
}

func TestWindowedDigest_WindowBoundaryInclusiveExclusive(t *testing.T) {
	stats := NewWindowedTDigest(TestWindowConfig)
	// Record at t=1, creating window [1s, 2s).
	stats.Record(10, n(1))
	td := stats.(*timeWindowedTDigest)

	// t=1 (start) is inclusive — should find the window.
	w := td.SubWindowForTime(n(1))
	require.NotNil(t, w)
	require.InDelta(t, 10, w.Quantile(0.5), 0.01)

	// t=2 (end) is exclusive — should NOT find the window.
	w2 := td.SubWindowForTime(n(2))
	require.Nil(t, w2)
}

func TestWindowedDigest_SubWindowForTimeMissing(t *testing.T) {
	stats := NewWindowedTDigest(TestWindowConfig).(*timeWindowedTDigest)
	// No data recorded yet — all windows are uninitialized.
	w := stats.SubWindowForTime(n(5))
	require.Nil(t, w)
}

func TestWindowedDigest_RecordMultiWeighted(t *testing.T) {
	stats := NewWindowedTDigest(TestWindowConfig)
	// RecordMulti with count=5 should weight the value heavily.
	stats.RecordMulti(10, n(1), 5)
	stats.RecordMulti(20, n(1), 1)
	// Weighted mean: (10*5 + 20*1) / 6 = 11.67
	require.InDelta(t, 11.67, stats.TrimmedMean(0, 1.0), 0.5)
}

func TestWindowedDigest_MultipleValuesInSameWindow(t *testing.T) {
	stats := NewWindowedTDigest(TestWindowConfig)
	// All values land in the same 1-second window [1s, 2s).
	halfwidth := 500 * time.Millisecond
	stats.Record(10, n(1))
	stats.Record(20, n(1).Add(halfwidth))
	stats.Record(30, n(1).Add(halfwidth))
	td := stats.(*timeWindowedTDigest)
	require.Equal(t, 1, countNonEmptyWindows(td))
	require.InDelta(t, 20, stats.TrimmedMean(0, 1.0), 0.01) // (10+20+30)/3
	require.InDelta(t, 30, stats.Quantile(1.0), 0.01)
	require.InDelta(t, 10, stats.Quantile(0.0), 0.01)
}

func TestWindowedDigest_RingBufferWrapPreservesNewest(t *testing.T) {
	cfg := WindowConfig{WindowSize: 1 * time.Second, WindowCount: 3}
	stats := NewWindowedTDigest(cfg)
	// Insert 5 values into 5 different windows; ring buffer holds 3.
	for i := 1; i <= 5; i++ {
		stats.Record(float64(i*10), n(i))
	}
	td := stats.(*timeWindowedTDigest)
	require.Equal(t, 3, countNonEmptyWindows(td))
	// Windows 3, 4, 5 should survive (values 30, 40, 50).
	require.InDelta(t, 50, stats.Quantile(1.0), 0.01)
	require.InDelta(t, 30, stats.Quantile(0.0), 0.01)
	require.InDelta(t, 40, stats.TrimmedMean(0, 1.0), 0.01)
}

func TestWindowedDigest_RecordToLatestWindow(t *testing.T) {
	stats := NewWindowedTDigest(TestWindowConfig)
	// RecordToLatestWindow uses time.Now(), so it should create a window.
	stats.RecordToLatestWindow(42)
	stats.RecordMultiToLatestWindow(100, 3)
	td := stats.(*timeWindowedTDigest)
	require.Equal(t, 1, countNonEmptyWindows(td))
	// Weighted mean: (42*1 + 100*3) / 4 = 85.5
	require.InDelta(t, 85.5, stats.TrimmedMean(0, 1.0), 0.01)
}
