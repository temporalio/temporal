package stats

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
	"unsafe"
)

// Benchmark output:
/*
cpu: Apple M5 Max
                                                       │   new.txt    │
                                                       │    sec/op    │
NewWindow-18                                             512.5µ ±  1%
TenMillionRecordQuery/trimmed-mean-size-10-18             10.38 ±  1%
TenMillionRecordQuery/quantile-size-10-18                 10.56 ±  2%
TenMillionRecordQuery/subwindow-quantile-size-10-18      324.8µ ±  1%
TenMillionRecordQuery/trimmed-mean-size-100-18            4.846 ±  1%
TenMillionRecordQuery/quantile-size-100-18                4.842 ±  1%
TenMillionRecordQuery/subwindow-quantile-size-100-18     32.31µ ±  0%
TenMillionRecordQuery/trimmed-mean-size-1000-18           3.519 ±  0%
TenMillionRecordQuery/quantile-size-1000-18               3.509 ±  0%
TenMillionRecordQuery/subwindow-quantile-size-1000-18    3.495µ ±  0%
TenMillionRecordQuery/trimmed-mean-size-10000-18         458.3m ±  0%
TenMillionRecordQuery/quantile-size-10000-18             459.3m ±  0%
TenMillionRecordQuery/subwindow-quantile-size-10000-18   581.5n ±  1%
Insert/10k-datapoints-retain-all-window-size-10-18       269.6µ ±  0%
Insert/10k-datapoints-retain-all-window-size-100-18      534.1µ ±  1%
Insert/10k-datapoints-retain-all-window-size-1000-18     1.447m ±  0%
Insert/10k-datapoints-retain-all-window-size-10000-18    4.372m ±  0%
Insert/10M-datapoints-retain-all-window-size-10-18        7.091 ± 30%
Insert/10M-datapoints-retain-all-window-size-100-18      542.6m ±  1%
Insert/10M-datapoints-retain-all-window-size-1000-18      1.420 ±  1%
Insert/10M-datapoints-retain-all-window-size-10000-18     4.334 ±  0%
Insert/10M-datapoints-retain-all-window-size-100000-18    5.276 ±  0%
Insert/10M-datapoints-retain-10k-window-size-10-18       225.1m ±  1%
Insert/10M-datapoints-retain-10k-window-size-100-18      486.2m ±  0%
Insert/10M-datapoints-retain-10k-window-size-1000-18      1.406 ±  1%
Insert/10M-datapoints-retain-10k-window-size-10000-18     4.313 ±  1%
geomean                                                  74.94m

                                                       │    new.txt     │
                                                       │      B/op      │
NewWindow-18                                             4.751Mi ± 0%
TenMillionRecordQuery/trimmed-mean-size-10-18            864.3Mi ± 0%
TenMillionRecordQuery/quantile-size-10-18                864.3Mi ± 0%
TenMillionRecordQuery/subwindow-quantile-size-10-18        0.000 ± 0%
TenMillionRecordQuery/trimmed-mean-size-100-18           327.3Mi ± 0%
TenMillionRecordQuery/quantile-size-100-18               327.3Mi ± 0%
TenMillionRecordQuery/subwindow-quantile-size-100-18       0.000 ± 0%
TenMillionRecordQuery/trimmed-mean-size-1000-18          109.8Mi ± 0%
TenMillionRecordQuery/quantile-size-1000-18              109.8Mi ± 0%
TenMillionRecordQuery/subwindow-quantile-size-1000-18      0.000 ± 0%
TenMillionRecordQuery/trimmed-mean-size-10000-18         14.83Mi ± 0%
TenMillionRecordQuery/quantile-size-10000-18             14.83Mi ± 0%
TenMillionRecordQuery/subwindow-quantile-size-10000-18     0.000 ± 0%
Insert/10k-datapoints-retain-all-window-size-10-18       12.96Ki ± 1%
Insert/10k-datapoints-retain-all-window-size-100-18      8.750Ki ± 0%
Insert/10k-datapoints-retain-all-window-size-1000-18     6.496Ki ± 0%
Insert/10k-datapoints-retain-all-window-size-10000-18    488.3Ki ± 0%
Insert/10M-datapoints-retain-all-window-size-10-18       15.47Gi ± 0%
Insert/10M-datapoints-retain-all-window-size-100-18      799.8Mi ± 0%
Insert/10M-datapoints-retain-all-window-size-1000-18     166.6Mi ± 0%
Insert/10M-datapoints-retain-all-window-size-10000-18    493.2Mi ± 0%
Insert/10M-datapoints-retain-all-window-size-100000-18   579.8Mi ± 0%
Insert/10M-datapoints-retain-10k-window-size-10-18       11.46Mi ± 0%
Insert/10M-datapoints-retain-10k-window-size-100-18      8.823Mi ± 0%
Insert/10M-datapoints-retain-10k-window-size-1000-18     8.456Mi ± 0%
Insert/10M-datapoints-retain-10k-window-size-10000-18    453.9Mi ± 0%
geomean                                                               ¹
¹ summaries must be >0 to compute geomean

                                                       │    new.txt    │
                                                       │   allocs/op   │
NewWindow-18                                             1.502k ± 0%
TenMillionRecordQuery/trimmed-mean-size-10-18            1.069M ± 0%
TenMillionRecordQuery/quantile-size-10-18                1.069M ± 0%
TenMillionRecordQuery/subwindow-quantile-size-10-18       0.000 ± 0%
TenMillionRecordQuery/trimmed-mean-size-100-18           122.3k ± 0%
TenMillionRecordQuery/quantile-size-100-18               122.3k ± 0%
TenMillionRecordQuery/subwindow-quantile-size-100-18      0.000 ± 0%
TenMillionRecordQuery/trimmed-mean-size-1000-18          12.95k ± 0%
TenMillionRecordQuery/quantile-size-1000-18              12.95k ± 0%
TenMillionRecordQuery/subwindow-quantile-size-1000-18     0.000 ± 0%
TenMillionRecordQuery/trimmed-mean-size-10000-18         1.304k ± 0%
TenMillionRecordQuery/quantile-size-10000-18             1.304k ± 0%
TenMillionRecordQuery/subwindow-quantile-size-10000-18    0.000 ± 0%
Insert/10k-datapoints-retain-all-window-size-10-18        266.0 ± 0%
Insert/10k-datapoints-retain-all-window-size-100-18       264.0 ± 0%
Insert/10k-datapoints-retain-all-window-size-1000-18      261.0 ± 0%
Insert/10k-datapoints-retain-all-window-size-10000-18     306.0 ± 0%
Insert/10M-datapoints-retain-all-window-size-10-18       5.302M ± 0%
Insert/10M-datapoints-retain-all-window-size-100-18      552.0k ± 0%
Insert/10M-datapoints-retain-all-window-size-1000-18     352.0k ± 0%
Insert/10M-datapoints-retain-all-window-size-10000-18    351.0k ± 0%
Insert/10M-datapoints-retain-all-window-size-100000-18   356.1k ± 0%
Insert/10M-datapoints-retain-10k-window-size-10-18       303.0k ± 0%
Insert/10M-datapoints-retain-10k-window-size-100-18      302.1k ± 0%
Insert/10M-datapoints-retain-10k-window-size-1000-18     302.0k ± 0%
Insert/10M-datapoints-retain-10k-window-size-10000-18    344.0k ± 0%
geomean                                                              ¹
¹ summaries must be >0 to compute geomean

                                                       │        new.txt        │
                                                       │ B/stats-size-estimate │
Insert/10k-datapoints-retain-all-window-size-10-18                248.7Ki ± 0%
Insert/10k-datapoints-retain-all-window-size-100-18               164.3Ki ± 0%
Insert/10k-datapoints-retain-all-window-size-1000-18              155.9Ki ± 0%
Insert/10k-datapoints-retain-all-window-size-10000-18             22.02Ki ± 4%
Insert/10M-datapoints-retain-all-window-size-10-18                242.6Mi ± 0%
Insert/10M-datapoints-retain-all-window-size-100-18               160.2Mi ± 0%
Insert/10M-datapoints-retain-all-window-size-1000-18              152.0Mi ± 0%
Insert/10M-datapoints-retain-all-window-size-10000-18             21.59Mi ± 0%
Insert/10M-datapoints-retain-all-window-size-100000-18            2.345Mi ± 1%
Insert/10M-datapoints-retain-10k-window-size-10-18                248.6Ki ± 0%
Insert/10M-datapoints-retain-10k-window-size-100-18               164.2Ki ± 0%
Insert/10M-datapoints-retain-10k-window-size-1000-18              155.8Ki ± 0%
Insert/10M-datapoints-retain-10k-window-size-10000-18             22.45Ki ± 2%
geomean                                                           1.131Mi
*/

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

// NewWindow-18: 512.5µ ±  1%
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

func BenchmarkTenMillionRecordQuery(b *testing.B) {
	for windowSize := 10; windowSize <= 10_000; windowSize *= 10 {
		stats, _ := NewWindowedTDigest(WindowConfig{
			WindowSize:  time.Duration(windowSize) * time.Second,
			WindowCount: 10_000_000 / windowSize,
		})
		datapoints := generateDatapoints(1, 10_000_000, 1, rand.NewPCG(7, 42))
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
// It did not make a significant (<1%) impact on latency.
func BenchmarkInsert(b *testing.B) {
	slidingBenchmark(benchmarkInput{
		name:                "10k-datapoints-retain-all",
		b:                   b,
		inputDatapointCount: 10_000,
		windowRotations:     1,
		startWindowSize:     10,
		maxWindowSize:       10_000,
	})
	slidingBenchmark(benchmarkInput{
		name:                "10M-datapoints-retain-all",
		b:                   b,
		inputDatapointCount: 10_000_000,
		windowRotations:     1,
		startWindowSize:     10,
		maxWindowSize:       100_000,
	})
	slidingBenchmark(benchmarkInput{
		name:                "10M-datapoints-retain-10k",
		b:                   b,
		inputDatapointCount: 10_000_000,
		windowRotations:     1_000,
		startWindowSize:     10,
		maxWindowSize:       10_000,
	})
}

type benchmarkInput struct {
	name                string
	b                   *testing.B
	inputDatapointCount int
	windowRotations     int
	startWindowSize     int
	maxWindowSize       int
}

func slidingBenchmark(in benchmarkInput) {
	if (in.startWindowSize != 1 && in.startWindowSize%10 != 0) || in.maxWindowSize%10 != 0 ||
		in.inputDatapointCount%10 != 0 {
		in.b.Skip("startWindowSize, maxWindowSize, datapointCount must be a multiple of 10")
	}
	if in.inputDatapointCount/in.maxWindowSize < in.windowRotations {
		in.b.Skip("Not enough datapoints to rotate windows that many times.")
	}
	datapoints := generateDatapoints(uint64(in.maxWindowSize*in.windowRotations), in.inputDatapointCount,
		1, rand.NewPCG(7, 42))
	for windowSize := in.startWindowSize; windowSize <= in.maxWindowSize; windowSize *= 10 {
		in.b.Run(fmt.Sprintf("%s-window-size-%d", in.name, windowSize), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				// Need to recreate this each loop
				stats, _ := NewWindowedTDigest(WindowConfig{
					WindowSize:  time.Duration(windowSize) * time.Second,
					WindowCount: in.inputDatapointCount / (windowSize * in.windowRotations),
				})
				stats.(*timeWindowedTDigest).pretouchWindowsForTest(n(1))
				b.StartTimer()
				for _, dp := range datapoints {
					stats.RecordMulti(dp.Value, dp.Time, dp.Count)
				}
				b.StopTimer()
				b.ReportMetric(float64(estimateTDigestSize(stats.(*timeWindowedTDigest))), "B/stats-size-estimate")
			}
		})
	}
}

func estimateTDigestSize(impl *timeWindowedTDigest) uintptr {
	// Count outer struct size
	size := unsafe.Sizeof(*impl)
	for _, w := range impl.windows {
		// Count the window struct size
		size += unsafe.Sizeof(timedWindow{})
		if w.tdigest != nil {
			// Count tdigest-struct's size
			size += unsafe.Sizeof(*w.tdigest)
			// Reverse-engineered from the tdigest package implementation:
			// Each tdigest contains a "summary", which contains two slices
			// of equal size, "mean" and "count". ForEachCentroid iterates those slices.
			w.tdigest.ForEachCentroid(func(_ float64, _ uint64) bool {
				size += 16
				return true
			})
		}
	}
	return size
}

func generateDatapoints(timeStartSeconds uint64, datapoints int, pointsPerBucket uint64, random UIntGenerator) (output []RecordingValue) {
	output = make([]RecordingValue, datapoints)
	timeIdx := timeStartSeconds
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
