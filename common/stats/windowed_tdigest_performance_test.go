package stats

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
	"unsafe"
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
// Tests were run on a 2025 Macbook Pro M4 Max, your CPU features may differ!
//
// -                                                            s, ms, us, ns         MB, KB,  B                        GB, MB, KB,  B
// BenchmarkInsert/10k-datapoints-retain-all-window-size-10           402,160 ns/op      254,672 B/stats-size-estimate         656,036 B/op-alloc
// BenchmarkInsert/10k-datapoints-retain-all-window-size-100          680,058 ns/op      168,272 B/stats-size-estimate         649,773 B/op-alloc
// BenchmarkInsert/10k-datapoints-retain-all-window-size-1000       1,668,972 ns/op      159,632 B/stats-size-estimate         647,274 B/op-alloc
// BenchmarkInsert/10k-datapoints-retain-all-window-size-10000      4,438,201 ns/op       23,232 B/stats-size-estimate       1,139,031 B/op-alloc
// BenchmarkInsert/10M-datapoints-retain-all-window-size-10    11,239,292,208 ns/op  254,389,632 B/stats-size-estimate  22,680,700,936 B/op-alloc
// BenchmarkInsert/10M-datapoints-retain-all-window-size-100    1,765,953,792 ns/op  167,989,632 B/stats-size-estimate   2,851,912,088 B/op-alloc
// BenchmarkInsert/10M-datapoints-retain-all-window-size-1000   1,723,260,708 ns/op  159,349,632 B/stats-size-estimate    ,869,028,368 B/op-alloc
// BenchmarkInsert/10M-datapoints-retain-all-window-size-10000  4,644,341,542 ns/op   22,848,592 B/stats-size-estimate   1,162,587,904 B/op-alloc
// BenchmarkInsert/10M-datapoints-retain-all-window-size-100000 5,661,283,750 ns/op    2,395,696 B/stats-size-estimate   1,248,610,672 B/op-alloc
// BenchmarkInsert/10M-datapoints-retain-10k-window-size-10       392,717,680 ns/op      254,560 B/stats-size-estimate     656,047,216 B/op-alloc
// BenchmarkInsert/10M-datapoints-retain-10k-window-size-100      680,680,917 ns/op      168,160 B/stats-size-estimate     649,804,400 B/op-alloc
// BenchmarkInsert/10M-datapoints-retain-10k-window-size-1000   1,665,408,958 ns/op      159,520 B/stats-size-estimate     648,923,456 B/op-alloc
// BenchmarkInsert/10M-datapoints-retain-10k-window-size-10000  4,630,973,833 ns/op       23,168 B/stats-size-estimate   1,115,983,792 B/op-alloc
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
