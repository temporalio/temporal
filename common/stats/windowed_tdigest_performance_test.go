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

// -                                                            s, ms, us, ns
// BenchmarkTenMillionRecordQuery/trimmed-mean-size-10         11,655,230,875 ns/op
// BenchmarkTenMillionRecordQuery/quantile-size-10             11,743,078,833 ns/op
// BenchmarkTenMillionRecordQuery/subwindow-quantile-size-10        1,795,897 ns/op
// BenchmarkTenMillionRecordQuery/trimmed-mean-size-100         5,297,755,167 ns/op
// BenchmarkTenMillionRecordQuery/quantile-size-100             4,950,984,583 ns/op
// BenchmarkTenMillionRecordQuery/subwindow-quantile-size-100         144,574 ns/op
// BenchmarkTenMillionRecordQuery/trimmed-mean-size-1000        3,844,660,708 ns/op
// BenchmarkTenMillionRecordQuery/quantile-size-1000            3,646,952,708 ns/op
// BenchmarkTenMillionRecordQuery/subwindow-quantile-size-1000         13,883 ns/op
// BenchmarkTenMillionRecordQuery/trimmed-mean-size-10000         473,836,570 ns/op
// BenchmarkTenMillionRecordQuery/quantile-size-10000             473,955,278 ns/op
// BenchmarkTenMillionRecordQuery/subwindow-quantile-size-10000       798,834 ns/op
func BenchmarkTenMillionRecordQuery(b *testing.B) {
	for windowSize := 10; windowSize <= 10_000; windowSize *= 10 {
		stats, _ := NewWindowedTDigest(WindowConfig{
			WindowSize:  time.Duration(windowSize) * time.Second,
			WindowCount: 10_000_000 / windowSize,
		})
		datapoints := generateDatapoints(10_000_000, 1, rand.NewPCG(7, 42))
		for _, dp := range datapoints {
			stats.RecordMulti(dp.Value, dp.Time, dp.Count)
		}
		b.Run(fmt.Sprintf("trimmed-mean-size-%d", windowSize), func(b *testing.B) {
			for b.Loop() {
				stats.TrimmedMean(0, 1.0)
			}
		})
		b.Run(fmt.Sprintf("quantile-size-%d", windowSize), func(b *testing.B) {
			for b.Loop() {
				stats.Quantile(0.9)
			}
		})
		b.Run(fmt.Sprintf("subwindow-quantile-size-%d", windowSize), func(b *testing.B) {
			for b.Loop() {
				stats.SubWindowForTime(datapoints[9_000_000].Time).Quantile(0.9)
			}
		})
	}
}

// How to read this chart: Lower numbers are better. There are two datasets tested,
// 10k and 10M. Observe that increasing window size for a fixed datapoint count
// has a nonlinear impact on latency. Observe that increasing datapoint count
// for a fixed window size also has a somewhat super-linear impact on latency.
// Increasing window rotation by reducing the total window count was tested.
// It did not make a significant (<1%) impact on latency
// -                                                        s, ms, us, ns       GB, MB, KB,  B
// BenchmarkInsert/datapoints-10,000-window-size-1             79,594,851 ns/op    220,978,893 B/op     90,277 allocs/op
// BenchmarkInsert/datapoints-10,000-window-size-10             8,262,293 ns/op     22,684,401 B/op     18,272 allocs/op
// BenchmarkInsert/datapoints-10,000-window-size-100            1,496,739 ns/op      2,852,384 B/op     11,067 allocs/op
// BenchmarkInsert/datapoints-10,000-window-size-1000           1,764,475 ns/op        867,330 B/op     10,344 allocs/op
// BenchmarkInsert/datapoints-10,000-window-size-10000          4,493,649 ns/op      1,161,041 B/op     10,316 allocs/op
// BenchmarkInsert/datapoints-10,000-window-size-100000     <Same as above. Did not saturate all the windows>
// -                                                        s, ms, us, ns       GB, MB, KB,  B
// BenchmarkInsert/datapoints-10,000,000-window-size-1      <Not tested due to test runtime>
// BenchmarkInsert/datapoints-10,000,000-window-size-10     9,569,462,583 ns/op 22,680,717,288 B/op 18,302,020 allocs/op
// BenchmarkInsert/datapoints-10,000,000-window-size-100    1,888,510,125 ns/op  2,851,910,864 B/op 11,101,984 allocs/op
// BenchmarkInsert/datapoints-10,000,000-window-size-1000   1,782,219,708 ns/op    869,035,776 B/op 10,381,987 allocs/op
// BenchmarkInsert/datapoints-10,000,000-window-size-10000  4,656,263,666 ns/op  1,162,586,672 B/op 10,353,995 allocs/op
// BenchmarkInsert/datapoints-10,000,000-window-size-100000 5,698,841,500 ns/op  1,248,606,488 B/op 10,356,404 allocs/op
func BenchmarkInsert(b *testing.B) {
	slidingBenchmark("datapoints-10,000", b, 10_000, 1, 10_000)
	slidingBenchmark("datapoints-10,000,000", b, 10_000_000, 10, 100_000)
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
			b.ReportAllocs()
			for b.Loop() {
				stats, _ := NewWindowedTDigest(WindowConfig{
					WindowSize:  time.Duration(windowSize) * time.Second,
					WindowCount: datapointCount,
				})
				for _, dp := range datapoints {
					stats.RecordMulti(dp.Value, dp.Time, dp.Count)
				}
			}
		})
	}
}
