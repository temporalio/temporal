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
