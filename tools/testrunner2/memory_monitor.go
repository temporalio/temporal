package testrunner2

import (
	"fmt"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const memorySummaryLimit = 10

type processMemorySample interface {
	stop() int64
}

type memoryRecord struct {
	displayName string
	attempt     string
	isolated    bool
	runtime     time.Duration
	peakRSSKB   int64
}

type processMemoryMonitor struct {
	mu           sync.Mutex
	interval     time.Duration
	processTable func() processTable
	records      map[int]*processMemoryRecord
	stopCh       chan struct{}
}

type processMemoryRecord struct {
	peakRSSKB int64
}

type processMemoryLease struct {
	monitor *processMemoryMonitor
	pid     int
}

type processInfo struct {
	ppid  int
	rssKB int64
}

type processTable map[int]processInfo

func newProcessMemoryMonitor(interval time.Duration) *processMemoryMonitor {
	if interval <= 0 {
		interval = time.Second
	}
	return &processMemoryMonitor{
		interval:     interval,
		processTable: readProcessTable,
	}
}

func (m *processMemoryMonitor) start(pid int) processMemorySample {
	if m == nil {
		return nil
	}

	m.mu.Lock()
	if m.records == nil {
		m.records = make(map[int]*processMemoryRecord)
	}
	m.records[pid] = &processMemoryRecord{}
	if m.stopCh == nil {
		m.stopCh = make(chan struct{})
		go m.run(m.stopCh)
	}
	m.mu.Unlock()

	m.sample()
	return &processMemoryLease{monitor: m, pid: pid}
}

func (m *processMemoryMonitor) run(stopCh <-chan struct{}) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.sample()
		case <-stopCh:
			return
		}
	}
}

func (m *processMemoryMonitor) sample() {
	if m.processTable == nil {
		return
	}
	table := m.processTable()

	m.mu.Lock()
	defer m.mu.Unlock()
	for pid, record := range m.records {
		record.peakRSSKB = max(record.peakRSSKB, table.treeRSSKB(pid))
	}
}

func (l *processMemoryLease) stop() int64 {
	if l == nil || l.monitor == nil {
		return 0
	}

	l.monitor.sample()
	return l.monitor.stop(l.pid)
}

func (m *processMemoryMonitor) stop(pid int) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	record := m.records[pid]
	delete(m.records, pid)
	if len(m.records) == 0 && m.stopCh != nil {
		close(m.stopCh)
		m.stopCh = nil
	}
	if record == nil {
		return 0
	}
	return record.peakRSSKB
}

func readProcessTable() processTable {
	out, err := exec.Command("ps", "-axo", "pid=,ppid=,rss=").Output()
	if err != nil {
		return nil
	}

	table := make(processTable)
	for line := range strings.SplitSeq(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		ppid, err := strconv.Atoi(fields[1])
		if err != nil {
			continue
		}
		rssKB, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			continue
		}
		table[pid] = processInfo{
			ppid:  ppid,
			rssKB: rssKB,
		}
	}
	return table
}

func stopMemorySample(sample processMemorySample) int64 {
	if sample == nil {
		return 0
	}
	return sample.stop()
}

func (t processTable) treeRSSKB(root int) int64 {
	if len(t) == 0 {
		return 0
	}

	children := make(map[int][]int, len(t))
	for pid, info := range t {
		children[info.ppid] = append(children[info.ppid], pid)
	}

	var total int64
	seen := make(map[int]struct{})
	stack := []int{root}
	for len(stack) > 0 {
		pid := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := seen[pid]; ok {
			continue
		}
		seen[pid] = struct{}{}

		info, ok := t[pid]
		if ok {
			total += info.rssKB
		}
		stack = append(stack, children[pid]...)
	}
	return total
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
	if len(records) > memorySummaryLimit {
		records = records[:memorySummaryLimit]
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
