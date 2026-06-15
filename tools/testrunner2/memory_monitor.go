package testrunner2

import (
	"fmt"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type processMemorySampler func(pid int) processMemorySample

type processMemorySample interface {
	stop() int64
}

type realMemorySample struct {
	pid  int
	peak atomic.Int64

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

type memoryRecord struct {
	displayName string
	attempt     string
	isolated    bool
	runtime     time.Duration
	peakRSSKB   int64
}

func realProcessMemorySampler(pid int) processMemorySample {
	s := &realMemorySample{
		pid:    pid,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	go s.run()
	return s
}

func (s *realMemorySample) run() {
	defer close(s.doneCh)
	s.sample()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.sample()
		case <-s.stopCh:
			s.sample()
			return
		}
	}
}

func (s *realMemorySample) sample() {
	rssKB := readRSSKB(s.pid)
	for {
		peak := s.peak.Load()
		if rssKB <= peak || s.peak.CompareAndSwap(peak, rssKB) {
			return
		}
	}
}

func (s *realMemorySample) stop() int64 {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		<-s.doneCh
	})
	return s.peak.Load()
}

func readRSSKB(pid int) int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	rssKB, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0
	}
	return rssKB
}

func (r *runner) startMemorySample(pid int) processMemorySample {
	if r.memorySampler == nil {
		return nil
	}
	return r.memorySampler(pid)
}

func stopMemorySample(sample processMemorySample) int64 {
	if sample == nil {
		return 0
	}
	return sample.stop()
}

func (r *runner) addMemoryRecord(record memoryRecord) {
	if record.peakRSSKB <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.memoryRecords = append(r.memoryRecords, record)
}

func (r *runner) logMemorySummary() {
	r.mu.Lock()
	records := append([]memoryRecord(nil), r.memoryRecords...)
	r.mu.Unlock()

	records = filterMemoryRecordsWithRSS(records)
	if len(records) == 0 {
		return
	}

	slices.SortStableFunc(records, func(a, b memoryRecord) int {
		switch {
		case a.peakRSSKB > b.peakRSSKB:
			return -1
		case a.peakRSSKB < b.peakRSSKB:
			return 1
		default:
			return 0
		}
	})
	if len(records) > 10 {
		records = records[:10]
	}

	var body strings.Builder
	for _, record := range records {
		isolated := ""
		if record.isolated {
			isolated = ", isolated"
		}
		fmt.Fprintf(&body, "%8s  %s (attempt=%s%s, runtime=%v)\n",
			formatRSS(record.peakRSSKB),
			record.displayName,
			record.attempt,
			isolated,
			record.runtime,
		)
	}
	r.console.WriteGrouped(fmt.Sprintf("%s%s memory peaks", logPrefix, time.Now().Format("15:04:05")), body.String())
}

func filterMemoryRecordsWithRSS(records []memoryRecord) []memoryRecord {
	out := records[:0]
	for _, record := range records {
		if record.peakRSSKB > 0 {
			out = append(out, record)
		}
	}
	return out
}

func peakRSSInfo(peakRSSKB int64) string {
	if peakRSSKB <= 0 {
		return ""
	}
	return fmt.Sprintf(", peak-rss=%s", formatRSS(peakRSSKB))
}

func formatRSS(rssKB int64) string {
	switch {
	case rssKB >= 1024*1024:
		return fmt.Sprintf("%.1fGiB", float64(rssKB)/(1024*1024))
	case rssKB >= 1024:
		return fmt.Sprintf("%.0fMiB", float64(rssKB)/1024)
	case rssKB > 0:
		return fmt.Sprintf("%dKiB", rssKB)
	default:
		return ""
	}
}
