package stats

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

// Uses declarations from windowed_tdigest_test.go
// func n(seconds int) time.Time
// RecordingValue struct {
// 	Value float64
// 	Time  time.Time
// 	Count uint64
// }

type UIntGenerator interface {
	Uint64() uint64
}

// -                 ms, us, ns
// BenchmarkNewWindow 8,058,719 ns/op
// 8ms to add 1000 new windows each containing a single datapoint.
// For windows sized to more than a few milliseconds, this means window
// creation will not be a bottleneck.
func BenchmarkNewWindow(b *testing.B) {
	var i int
	for b.Loop() {
		stats, _ := NewWindowedTDigest(WindowConfig{
			WindowSize:  1 * time.Second,
			WindowCount: 300,
		})
		statsImpl := stats.(*timeWindowedTDigest)
		for range 1000 {
			now := n(i)
			statsImpl.RecordMulti(float64(i), now, 52)
			i++
		}
	}
}

// -                                                 ms, us, ns
// BenchmarkTenMillionRecordQuery/trimmed-mean       99,039,996 ns/op
// BenchmarkTenMillionRecordQuery/quantile           98,738,240 ns/op
// BenchmarkTenMillionRecordQuery/subwindow-quantile      1,509 ns/op
func BenchmarkTenMillionRecordQuery(b *testing.B) {
	stats, _ := NewWindowedTDigest(WindowConfig{
		WindowSize:  1 * time.Second,
		WindowCount: 300,
	})
	datapoints := generateDatapoints(10_000_000, 10_000, rand.NewPCG(7, 42))
	for _, dp := range datapoints {
		stats.RecordMulti(dp.Value, dp.Time, dp.Count)
	}
	b.Run("trimmed-mean", func(b *testing.B) {
		for b.Loop() {
			stats.TrimmedMean(0, 1.0)
		}
	})
	b.Run("quantile", func(b *testing.B) {
		for b.Loop() {
			stats.Quantile(0.9)
		}
	})
	b.Run("subwindow-quantile", func(b *testing.B) {
		for b.Loop() {
			stats.SubWindowForTime(datapoints[9_000_000].Time).Quantile(0.9)
		}
	})
}

// How to read this chart: Lower numbers are better. There are two datasets tested,
// 10k and 10M. Observe that increasing window size for a fixed datapoint count
// has a nonlinear impact on latency. Observe that increasing datapoint count
// for a fixed window size also has a somewhat super-linear impact on latency.
// -                                                        s, ms, us, ns
// BenchmarkInsert/datapoints-10,000-window-size-1             81,422,795 ns/op
// BenchmarkInsert/datapoints-10,000-window-size-10             8,321,690 ns/op
// BenchmarkInsert/datapoints-10,000-window-size-100            1,497,160 ns/op
// BenchmarkInsert/datapoints-10,000-window-size-1000           1,697,073 ns/op
// BenchmarkInsert/datapoints-10,000-window-size-10000          4,462,444 ns/op
// BenchmarkInsert/datapoints-10,000-window-size-100000         <invalid> ns/op
// -------------------------------------------------------------------------------
// BenchmarkInsert/datapoints-10,000,000-window-size-1                DNF ns/op
// BenchmarkInsert/datapoints-10,000,000-window-size-10     9,668,609,417 ns/op
// BenchmarkInsert/datapoints-10,000,000-window-size-100    1,483,158,625 ns/op
// BenchmarkInsert/datapoints-10,000,000-window-size-1000   1,716,553,958 ns/op
// BenchmarkInsert/datapoints-10,000,000-window-size-10000  4,623,385,375 ns/op
// BenchmarkInsert/datapoints-10,000,000-window-size-100000 5,567,272,666 ns/op
func BenchmarkInsert(b *testing.B) {
	slidingBenchmark("datapoints-10,000", b, 10_000, 1, 10_000)
	slidingBenchmark("datapoints-10,000,000", b, 10_000_000, 10, 1000)
}

func generateDatapoints(datapoints int, pointsPerBucket uint64, random UIntGenerator) (output []RecordingValue) {
	output = make([]RecordingValue, datapoints)
	timeIdx := uint64(1)
	for i := range datapoints {
		output[i] = RecordingValue{
			Value: float64(i),
			Time:  n(int(timeIdx)),
			Count: random.Uint64() % 100,
		}
		// Expected-value of pointsPerBucket records per second, with uniform variation
		if random.Uint64()%pointsPerBucket == 0 {
			timeIdx++
		}
	}
	return
}

func slidingBenchmark(baseName string, b *testing.B, datapointCount, startWindowSize, maxWindowSize int) {
	if (startWindowSize != 1 && startWindowSize%10 != 0) || maxWindowSize%10 != 0 ||
		datapointCount%10 != 0 {
		b.Skip("startWindowSize, maxWindowSize, datapointCount must be a multiple of 10")
	}
	datapoints := generateDatapoints(datapointCount, 1, rand.NewPCG(7, 42))
	for windowSize := startWindowSize; windowSize <= maxWindowSize; windowSize *= 10 {
		b.Run(fmt.Sprintf("%s-window-size-%d", baseName, windowSize), func(b *testing.B) {
			for b.Loop() {
				stats, _ := NewWindowedTDigest(WindowConfig{
					WindowSize:  time.Duration(windowSize) * time.Second,
					WindowCount: datapointCount / windowSize,
				})
				for _, dp := range datapoints {
					stats.RecordMulti(dp.Value, dp.Time, dp.Count)
				}
			}
		})
	}
}
