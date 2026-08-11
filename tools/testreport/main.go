package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strings"
	"time"
)

type event struct {
	Type           string    `json:"type"`
	Timestamp      time.Time `json:"timestamp"`
	DurationMS     float64   `json:"duration_ms"`
	AcquireMS      float64   `json:"acquire_ms"`
	AcquireSource  string    `json:"acquire_source"`
	Namespaces     int       `json:"namespaces"`
	LiveClusters   int       `json:"live_clusters"`
	Goroutines     int       `json:"goroutines"`
	HeapInUseBytes uint64    `json:"heap_in_use_bytes"`
	SysBytes       uint64    `json:"sys_bytes"`
	RSSBytes       uint64    `json:"rss_bytes"`
	ExitCode       int       `json:"exit_code"`
}

type runSummary struct {
	WallTime               time.Duration
	ExitCode               int
	ClustersCreated        int
	NamespacesRegistered   int
	PeakLiveClusters       int
	PeakGoroutines         int
	PeakHeapInUseBytes     uint64
	PeakSysBytes           uint64
	PeakRSSBytes           uint64
	AcquireP50             time.Duration
	AcquireP99             time.Duration
	WarmEligibleAcquireP50 time.Duration
	WarmEligibleAcquireP99 time.Duration
	WarmSpareHits          int
	WarmSpareMisses        int
	CustomAcquires         int
	MeanBootTime           time.Duration
}

type namedReader struct {
	name   string
	reader io.Reader
}

type metricStatistics struct {
	Median float64
	Min    float64
	Max    float64
	StdDev float64
}

type multiRunSummary struct {
	Runs                   int
	FailedRuns             int
	WallTime               metricStatistics
	ClustersCreated        metricStatistics
	NamespacesRegistered   metricStatistics
	PeakLiveClusters       metricStatistics
	PeakGoroutines         metricStatistics
	PeakHeapInUseBytes     metricStatistics
	PeakSysBytes           metricStatistics
	PeakRSSBytes           metricStatistics
	AcquireP50             metricStatistics
	AcquireP99             metricStatistics
	WarmEligibleAcquireP50 metricStatistics
	WarmEligibleAcquireP99 metricStatistics
	WarmSpareHits          metricStatistics
	WarmSpareMisses        metricStatistics
	CustomAcquires         metricStatistics
	MeanBootTime           metricStatistics
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: testreport <cluster-events.jsonl> [cluster-events.jsonl ...]")
		os.Exit(2)
	}

	readers := make([]namedReader, 0, len(os.Args)-1)
	files := make([]*os.File, 0, len(os.Args)-1)
	for _, path := range os.Args[1:] {
		file, err := os.Open(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		files = append(files, file)
		readers = append(readers, namedReader{name: path, reader: file})
	}

	runs, err := summarizeRunFiles(readers)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, file := range files {
		if err := file.Close(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	if len(runs) == 1 {
		err = writeReport(os.Stdout, runs[0])
	} else {
		err = writeMultiRunReport(os.Stdout, aggregateRuns(runs))
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func summarize(reader io.Reader) (runSummary, error) {
	runs, err := summarizeRuns(reader)
	if err != nil {
		return runSummary{}, err
	}
	if len(runs) != 1 {
		return runSummary{}, fmt.Errorf("expected one run, found %d", len(runs))
	}
	return runs[0], nil
}

func summarizeRunFiles(files []namedReader) ([]runSummary, error) {
	var runs []runSummary
	for _, file := range files {
		fileRuns, err := summarizeRuns(file.reader)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", file.name, err)
		}
		runs = append(runs, fileRuns...)
	}
	return runs, nil
}

func summarizeRuns(reader io.Reader) ([]runSummary, error) {
	var runs []runSummary
	var runEvents []event
	inRun := false
	scanner := bufio.NewScanner(reader)
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		var decodedEvent event
		if err := json.Unmarshal(scanner.Bytes(), &decodedEvent); err != nil {
			return nil, fmt.Errorf("decode event on line %d: %w", lineNumber, err)
		}
		switch decodedEvent.Type {
		case "run_started":
			if inRun {
				return nil, fmt.Errorf("nested run_started on line %d", lineNumber)
			}
			inRun = true
			runEvents = []event{decodedEvent}
		case "run_finished":
			if !inRun {
				return nil, fmt.Errorf("run_finished without run_started on line %d", lineNumber)
			}
			runEvents = append(runEvents, decodedEvent)
			runs = append(runs, summarizeEvents(runEvents))
			inRun = false
			runEvents = nil
		default:
			if !inRun {
				return nil, fmt.Errorf("event outside run on line %d", lineNumber)
			}
			runEvents = append(runEvents, decodedEvent)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read events: %w", err)
	}
	if inRun {
		return nil, errors.New("incomplete run")
	}
	if len(runs) == 0 {
		return nil, errors.New("no complete runs")
	}
	return runs, nil
}

func summarizeEvents(events []event) runSummary {
	var summary runSummary
	var firstTimestamp time.Time
	var lastTimestamp time.Time
	var totalBootTime time.Duration
	var acquireTimes []time.Duration
	var warmEligibleAcquireTimes []time.Duration

	for _, event := range events {
		if firstTimestamp.IsZero() || event.Timestamp.Before(firstTimestamp) {
			firstTimestamp = event.Timestamp
		}
		if event.Timestamp.After(lastTimestamp) {
			lastTimestamp = event.Timestamp
		}
		summary.PeakLiveClusters = max(summary.PeakLiveClusters, event.LiveClusters)
		summary.PeakGoroutines = max(summary.PeakGoroutines, event.Goroutines)
		summary.PeakHeapInUseBytes = max(summary.PeakHeapInUseBytes, event.HeapInUseBytes)
		summary.PeakSysBytes = max(summary.PeakSysBytes, event.SysBytes)
		summary.PeakRSSBytes = max(summary.PeakRSSBytes, event.RSSBytes)
		switch event.Type {
		case "run_finished":
			summary.ExitCode = event.ExitCode
		case "cluster_created":
			summary.ClustersCreated++
			summary.NamespacesRegistered += event.Namespaces
			totalBootTime += milliseconds(event.DurationMS)
		case "cluster_acquired":
			acquireTime := milliseconds(event.AcquireMS)
			acquireTimes = append(acquireTimes, acquireTime)
			switch event.AcquireSource {
			case "warm-spare":
				summary.WarmSpareHits++
				warmEligibleAcquireTimes = append(warmEligibleAcquireTimes, acquireTime)
			case "warm-miss":
				summary.WarmSpareMisses++
				warmEligibleAcquireTimes = append(warmEligibleAcquireTimes, acquireTime)
			case "custom":
				summary.CustomAcquires++
			default:
			}
		case "namespace_registered":
			summary.NamespacesRegistered++
		default:
		}
	}

	if !firstTimestamp.IsZero() {
		summary.WallTime = lastTimestamp.Sub(firstTimestamp)
	}
	if summary.ClustersCreated > 0 {
		summary.MeanBootTime = totalBootTime / time.Duration(summary.ClustersCreated)
	}
	slices.Sort(acquireTimes)
	summary.AcquireP50 = percentile(acquireTimes, 50)
	summary.AcquireP99 = percentile(acquireTimes, 99)
	slices.Sort(warmEligibleAcquireTimes)
	summary.WarmEligibleAcquireP50 = percentile(warmEligibleAcquireTimes, 50)
	summary.WarmEligibleAcquireP99 = percentile(warmEligibleAcquireTimes, 99)
	return summary
}

func aggregateRuns(runs []runSummary) multiRunSummary {
	aggregate := multiRunSummary{Runs: len(runs)}
	for _, run := range runs {
		if run.ExitCode != 0 {
			aggregate.FailedRuns++
		}
	}
	aggregate.WallTime = statistics(runs, func(run runSummary) float64 { return float64(run.WallTime) })
	aggregate.ClustersCreated = statistics(runs, func(run runSummary) float64 { return float64(run.ClustersCreated) })
	aggregate.NamespacesRegistered = statistics(runs, func(run runSummary) float64 { return float64(run.NamespacesRegistered) })
	aggregate.PeakLiveClusters = statistics(runs, func(run runSummary) float64 { return float64(run.PeakLiveClusters) })
	aggregate.PeakGoroutines = statistics(runs, func(run runSummary) float64 { return float64(run.PeakGoroutines) })
	aggregate.PeakHeapInUseBytes = statistics(runs, func(run runSummary) float64 { return float64(run.PeakHeapInUseBytes) })
	aggregate.PeakSysBytes = statistics(runs, func(run runSummary) float64 { return float64(run.PeakSysBytes) })
	aggregate.PeakRSSBytes = statistics(runs, func(run runSummary) float64 { return float64(run.PeakRSSBytes) })
	aggregate.AcquireP50 = statistics(runs, func(run runSummary) float64 { return float64(run.AcquireP50) })
	aggregate.AcquireP99 = statistics(runs, func(run runSummary) float64 { return float64(run.AcquireP99) })
	aggregate.WarmEligibleAcquireP50 = statistics(runs, func(run runSummary) float64 { return float64(run.WarmEligibleAcquireP50) })
	aggregate.WarmEligibleAcquireP99 = statistics(runs, func(run runSummary) float64 { return float64(run.WarmEligibleAcquireP99) })
	aggregate.WarmSpareHits = statistics(runs, func(run runSummary) float64 { return float64(run.WarmSpareHits) })
	aggregate.WarmSpareMisses = statistics(runs, func(run runSummary) float64 { return float64(run.WarmSpareMisses) })
	aggregate.CustomAcquires = statistics(runs, func(run runSummary) float64 { return float64(run.CustomAcquires) })
	aggregate.MeanBootTime = statistics(runs, func(run runSummary) float64 { return float64(run.MeanBootTime) })
	return aggregate
}

func statistics(runs []runSummary, value func(runSummary) float64) metricStatistics {
	values := make([]float64, len(runs))
	var sum float64
	for index, run := range runs {
		values[index] = value(run)
		sum += values[index]
	}
	slices.Sort(values)
	mean := sum / float64(len(values))
	var squaredDifferences float64
	for _, current := range values {
		difference := current - mean
		squaredDifferences += difference * difference
	}
	median := values[len(values)/2]
	if len(values)%2 == 0 {
		median = (values[len(values)/2-1] + median) / 2
	}
	return metricStatistics{
		Median: median,
		Min:    values[0],
		Max:    values[len(values)-1],
		StdDev: math.Sqrt(squaredDifferences / float64(len(values))),
	}
}

func percentile(values []time.Duration, percent int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := (percent*len(values) + 99) / 100
	return values[index-1]
}

func milliseconds(value float64) time.Duration {
	return time.Duration(value * float64(time.Millisecond))
}

func writeReport(writer io.Writer, summary runSummary) error {
	_, err := fmt.Fprintf(
		writer,
		"metric\tvalue\n"+
			"wall_time\t%s\n"+
			"exit_code\t%d\n"+
			"clusters_created\t%d\n"+
			"namespaces_registered\t%d\n"+
			"mean_boot_time\t%s\n"+
			"acquire_p50\t%s\n"+
			"acquire_p99\t%s\n"+
			"warm_eligible_acquire_p50\t%s\n"+
			"warm_eligible_acquire_p99\t%s\n"+
			"warm_spare_hits\t%d\n"+
			"warm_spare_misses\t%d\n"+
			"custom_acquires\t%d\n"+
			"peak_live_clusters\t%d\n"+
			"peak_goroutines\t%d\n"+
			"peak_heap_in_use_mb\t%.1f\n"+
			"peak_sys_mb\t%.1f\n"+
			"peak_rss_mb\t%.1f\n",
		summary.WallTime,
		summary.ExitCode,
		summary.ClustersCreated,
		summary.NamespacesRegistered,
		summary.MeanBootTime,
		summary.AcquireP50,
		summary.AcquireP99,
		summary.WarmEligibleAcquireP50,
		summary.WarmEligibleAcquireP99,
		summary.WarmSpareHits,
		summary.WarmSpareMisses,
		summary.CustomAcquires,
		summary.PeakLiveClusters,
		summary.PeakGoroutines,
		float64(summary.PeakHeapInUseBytes)/(1<<20),
		float64(summary.PeakSysBytes)/(1<<20),
		float64(summary.PeakRSSBytes)/(1<<20),
	)
	return err
}

func writeMultiRunReport(writer io.Writer, summary multiRunSummary) error {
	var report strings.Builder
	report.WriteString(fmt.Sprintf("runs\t%d\nfailed_runs\t%d\n", summary.Runs, summary.FailedRuns))
	report.WriteString("metric\tmedian\tmin\tmax\tstddev\n")
	writeDurationStatistics(&report, "wall_time", summary.WallTime)
	writeNumberStatistics(&report, "clusters_created", summary.ClustersCreated, 1)
	writeNumberStatistics(&report, "namespaces_registered", summary.NamespacesRegistered, 1)
	writeDurationStatistics(&report, "mean_boot_time", summary.MeanBootTime)
	writeDurationStatistics(&report, "acquire_p50", summary.AcquireP50)
	writeDurationStatistics(&report, "acquire_p99", summary.AcquireP99)
	writeDurationStatistics(&report, "warm_eligible_acquire_p50", summary.WarmEligibleAcquireP50)
	writeDurationStatistics(&report, "warm_eligible_acquire_p99", summary.WarmEligibleAcquireP99)
	writeNumberStatistics(&report, "warm_spare_hits", summary.WarmSpareHits, 1)
	writeNumberStatistics(&report, "warm_spare_misses", summary.WarmSpareMisses, 1)
	writeNumberStatistics(&report, "custom_acquires", summary.CustomAcquires, 1)
	writeNumberStatistics(&report, "peak_live_clusters", summary.PeakLiveClusters, 1)
	writeNumberStatistics(&report, "peak_goroutines", summary.PeakGoroutines, 1)
	writeNumberStatistics(&report, "peak_heap_in_use_mb", summary.PeakHeapInUseBytes, 1.0/(1<<20))
	writeNumberStatistics(&report, "peak_sys_mb", summary.PeakSysBytes, 1.0/(1<<20))
	writeNumberStatistics(&report, "peak_rss_mb", summary.PeakRSSBytes, 1.0/(1<<20))
	_, err := io.WriteString(writer, report.String())
	return err
}

func writeDurationStatistics(report *strings.Builder, name string, statistics metricStatistics) {
	fmt.Fprintf(report,
		"%s\t%s\t%s\t%s\t%s\n",
		name,
		time.Duration(statistics.Median),
		time.Duration(statistics.Min),
		time.Duration(statistics.Max),
		time.Duration(statistics.StdDev))
}

func writeNumberStatistics(report *strings.Builder, name string, statistics metricStatistics, scale float64) {
	fmt.Fprintf(report,
		"%s\t%.1f\t%.1f\t%.1f\t%.1f\n",
		name,
		statistics.Median*scale,
		statistics.Min*scale,
		statistics.Max*scale,
		statistics.StdDev*scale)
}
