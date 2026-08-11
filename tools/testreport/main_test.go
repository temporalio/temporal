package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSummarize(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`{"type":"run_started","timestamp":"2026-08-10T11:59:59Z"}`,
		`{"type":"cluster_created","timestamp":"2026-08-10T12:00:00Z","cluster_id":1,"duration_ms":120,"live_clusters":1,"namespaces":2}`,
		`{"type":"cluster_acquired","timestamp":"2026-08-10T12:00:01Z","cluster_id":1,"acquire_ms":1}`,
		`{"type":"cluster_acquired","timestamp":"2026-08-10T12:00:02Z","cluster_id":1,"acquire_ms":5}`,
		`{"type":"cluster_acquired","timestamp":"2026-08-10T12:00:03Z","cluster_id":1,"acquire_ms":100}`,
		`{"type":"runtime","timestamp":"2026-08-10T12:00:04Z","goroutines":700,"heap_in_use_bytes":10485760,"sys_bytes":20971520,"rss_bytes":31457280,"live_clusters":2}`,
		`{"type":"namespace_registered","timestamp":"2026-08-10T12:00:04Z","cluster_id":1,"namespace":"extra"}`,
		`{"type":"cluster_destroyed","timestamp":"2026-08-10T12:00:05Z","cluster_id":1,"live_clusters":0}`,
		`{"type":"run_finished","timestamp":"2026-08-10T12:00:06Z","exit_code":0}`,
	}, "\n"))

	summary, err := summarize(input)
	require.NoError(t, err)
	require.Equal(t, runSummary{
		WallTime:             7 * time.Second,
		ExitCode:             0,
		ClustersCreated:      1,
		NamespacesRegistered: 3,
		PeakLiveClusters:     2,
		PeakGoroutines:       700,
		PeakHeapInUseBytes:   10 * 1024 * 1024,
		PeakSysBytes:         20 * 1024 * 1024,
		PeakRSSBytes:         30 * 1024 * 1024,
		AcquireP50:           5 * time.Millisecond,
		AcquireP99:           100 * time.Millisecond,
		MeanBootTime:         120 * time.Millisecond,
	}, summary)
}

func TestSummarizeClassifiesPerTestAcquisitions(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`{"type":"run_started","timestamp":"2026-08-10T12:00:00Z"}`,
		`{"type":"cluster_acquired","timestamp":"2026-08-10T12:00:01Z","acquire_ms":1,"acquire_source":"warm-spare"}`,
		`{"type":"cluster_acquired","timestamp":"2026-08-10T12:00:02Z","acquire_ms":5,"acquire_source":"warm-spare"}`,
		`{"type":"cluster_acquired","timestamp":"2026-08-10T12:00:03Z","acquire_ms":100,"acquire_source":"warm-miss"}`,
		`{"type":"cluster_acquired","timestamp":"2026-08-10T12:00:04Z","acquire_ms":500,"acquire_source":"custom"}`,
		`{"type":"run_finished","timestamp":"2026-08-10T12:00:05Z","exit_code":0}`,
	}, "\n"))

	summary, err := summarize(input)
	require.NoError(t, err)
	require.Equal(t, 2, summary.WarmSpareHits)
	require.Equal(t, 1, summary.WarmSpareMisses)
	require.Equal(t, 1, summary.CustomAcquires)
	require.Equal(t, 5*time.Millisecond, summary.WarmEligibleAcquireP50)
	require.Equal(t, 100*time.Millisecond, summary.WarmEligibleAcquireP99)
	require.Equal(t, 5*time.Millisecond, summary.AcquireP50)
	require.Equal(t, 500*time.Millisecond, summary.AcquireP99)
}

func TestSummarizeRejectsMalformedEvent(t *testing.T) {
	_, err := summarize(strings.NewReader("not-json\n"))
	require.Error(t, err)
}

func TestSummarizeRunsSplitsAppendedRuns(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`{"type":"run_started","timestamp":"2026-08-10T12:00:00Z"}`,
		`{"type":"cluster_acquired","timestamp":"2026-08-10T12:00:01Z","acquire_ms":4}`,
		`{"type":"run_finished","timestamp":"2026-08-10T12:00:02Z","exit_code":0}`,
		`{"type":"run_started","timestamp":"2026-08-10T13:00:00Z"}`,
		`{"type":"cluster_acquired","timestamp":"2026-08-10T13:00:01Z","acquire_ms":8}`,
		`{"type":"run_finished","timestamp":"2026-08-10T13:00:04Z","exit_code":1}`,
	}, "\n"))

	runs, err := summarizeRuns(input)
	require.NoError(t, err)
	require.Equal(t, []runSummary{
		{WallTime: 2 * time.Second, AcquireP50: 4 * time.Millisecond, AcquireP99: 4 * time.Millisecond},
		{WallTime: 4 * time.Second, ExitCode: 1, AcquireP50: 8 * time.Millisecond, AcquireP99: 8 * time.Millisecond},
	}, runs)
}

func TestSummarizeRunsDoesNotJoinRunsAcrossFiles(t *testing.T) {
	_, err := summarizeRunFiles([]namedReader{
		{name: "first.jsonl", reader: strings.NewReader(`{"type":"run_started","timestamp":"2026-08-10T12:00:00Z"}`)},
		{name: "second.jsonl", reader: strings.NewReader(`{"type":"run_finished","timestamp":"2026-08-10T12:00:01Z"}`)},
	})

	require.ErrorContains(t, err, "first.jsonl: incomplete run")
}

func TestAggregateRuns(t *testing.T) {
	aggregate := aggregateRuns([]runSummary{
		{
			WallTime:               time.Second,
			ClustersCreated:        2,
			NamespacesRegistered:   4,
			PeakLiveClusters:       1,
			PeakGoroutines:         100,
			PeakHeapInUseBytes:     10,
			PeakSysBytes:           20,
			PeakRSSBytes:           30,
			AcquireP50:             time.Millisecond,
			AcquireP99:             2 * time.Millisecond,
			WarmEligibleAcquireP50: time.Millisecond,
			WarmEligibleAcquireP99: 2 * time.Millisecond,
			WarmSpareHits:          10,
			WarmSpareMisses:        2,
			CustomAcquires:         1,
			MeanBootTime:           3 * time.Millisecond,
		},
		{
			WallTime:               3 * time.Second,
			ExitCode:               1,
			ClustersCreated:        4,
			NamespacesRegistered:   8,
			PeakLiveClusters:       3,
			PeakGoroutines:         300,
			PeakHeapInUseBytes:     30,
			PeakSysBytes:           40,
			PeakRSSBytes:           50,
			AcquireP50:             3 * time.Millisecond,
			AcquireP99:             4 * time.Millisecond,
			WarmEligibleAcquireP50: 3 * time.Millisecond,
			WarmEligibleAcquireP99: 4 * time.Millisecond,
			WarmSpareHits:          20,
			WarmSpareMisses:        4,
			CustomAcquires:         3,
			MeanBootTime:           5 * time.Millisecond,
		},
	})

	require.Equal(t, 2, aggregate.Runs)
	require.Equal(t, 1, aggregate.FailedRuns)
	require.Equal(t, metricStatistics{Median: 2e9, Min: 1e9, Max: 3e9, StdDev: 1e9}, aggregate.WallTime)
	require.Equal(t, metricStatistics{Median: 3, Min: 2, Max: 4, StdDev: 1}, aggregate.ClustersCreated)
	require.Equal(t, metricStatistics{Median: 200, Min: 100, Max: 300, StdDev: 100}, aggregate.PeakGoroutines)
	require.Equal(t, metricStatistics{Median: 2e6, Min: 1e6, Max: 3e6, StdDev: 1e6}, aggregate.WarmEligibleAcquireP50)
	require.Equal(t, metricStatistics{Median: 15, Min: 10, Max: 20, StdDev: 5}, aggregate.WarmSpareHits)
	require.Equal(t, metricStatistics{Median: 3, Min: 2, Max: 4, StdDev: 1}, aggregate.WarmSpareMisses)
	require.Equal(t, metricStatistics{Median: 2, Min: 1, Max: 3, StdDev: 1}, aggregate.CustomAcquires)
}

func TestWriteMultiRunReport(t *testing.T) {
	var output bytes.Buffer
	err := writeMultiRunReport(&output, aggregateRuns([]runSummary{
		{WallTime: time.Second, PeakGoroutines: 100, WarmEligibleAcquireP99: time.Millisecond, WarmSpareHits: 10},
		{WallTime: 3 * time.Second, PeakGoroutines: 300, WarmEligibleAcquireP99: 3 * time.Millisecond, WarmSpareHits: 20},
	}))

	require.NoError(t, err)
	require.Contains(t, output.String(), "metric\tmedian\tmin\tmax\tstddev")
	require.Contains(t, output.String(), "runs\t2")
	require.Contains(t, output.String(), "wall_time\t2s\t1s\t3s\t1s")
	require.Contains(t, output.String(), "peak_goroutines\t200.0\t100.0\t300.0\t100.0")
	require.Contains(t, output.String(), "warm_eligible_acquire_p99\t2ms\t1ms\t3ms\t1ms")
	require.Contains(t, output.String(), "warm_spare_hits\t15.0\t10.0\t20.0\t5.0")
	require.Contains(t, output.String(), "peak_rss_mb")
}

func TestWriteReportIncludesClassifiedAcquisitions(t *testing.T) {
	var output bytes.Buffer
	err := writeReport(&output, runSummary{
		WarmEligibleAcquireP50: time.Millisecond,
		WarmEligibleAcquireP99: 2 * time.Millisecond,
		WarmSpareHits:          3,
		WarmSpareMisses:        4,
		CustomAcquires:         5,
	})

	require.NoError(t, err)
	require.Contains(t, output.String(), "warm_eligible_acquire_p50\t1ms")
	require.Contains(t, output.String(), "warm_eligible_acquire_p99\t2ms")
	require.Contains(t, output.String(), "warm_spare_hits\t3")
	require.Contains(t, output.String(), "warm_spare_misses\t4")
	require.Contains(t, output.String(), "custom_acquires\t5")
}

func TestSummarizeRunsRejectsEventsOutsideBoundaries(t *testing.T) {
	_, err := summarizeRuns(strings.NewReader(
		`{"type":"runtime","timestamp":"2026-08-10T12:00:00Z"}`,
	))

	require.ErrorContains(t, err, "event outside run")
}
