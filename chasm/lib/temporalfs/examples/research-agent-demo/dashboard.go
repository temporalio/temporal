package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	maxFeedLines = 12
	refreshRate  = 200 * time.Millisecond

	colorReset  = "\033[0m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorRed    = "\033[31m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
	cursorHome  = "\033[H"
	clearScreen = "\033[2J"
)

// FeedEntry is a single line in the live activity feed.
type FeedEntry struct {
	TopicSlug string
	StepName  string
	State     string // "done", "running", "retry", "failed"
	StepIdx   string // "1/5", "2/5", etc.
	Duration  string
}

// Dashboard renders a live terminal dashboard.
type Dashboard struct {
	runner    *Runner
	startTime time.Time
	total     int

	mu   sync.Mutex
	feed []FeedEntry
	done chan struct{}
}

// NewDashboard creates a dashboard that reads events from the runner.
func NewDashboard(runner *Runner, total int) *Dashboard {
	return &Dashboard{
		runner:    runner,
		total:     total,
		startTime: time.Now(),
		feed:      make([]FeedEntry, 0, maxFeedLines),
		done:      make(chan struct{}),
	}
}

// Start begins consuming events and rendering. Call Stop() to end.
func (d *Dashboard) Start() {
	// Event consumer goroutine.
	go func() {
		for ev := range d.runner.EventCh {
			d.mu.Lock()
			entry := FeedEntry{
				TopicSlug: ev.TopicSlug,
				StepName:  ev.StepName,
				StepIdx:   fmt.Sprintf("%d/5", ev.StepIndex+1),
			}
			switch ev.State {
			case "completed":
				entry.State = "done"
			case "started":
				entry.State = "running"
				entry.StepName = "WebResearch"
				entry.StepIdx = "1/5"
			case "retrying":
				entry.State = "retry"
			default:
				entry.State = ev.State
			}
			d.feed = append(d.feed, entry)
			if len(d.feed) > maxFeedLines {
				d.feed = d.feed[len(d.feed)-maxFeedLines:]
			}
			d.mu.Unlock()
		}
		close(d.done)
	}()

	// Render loop goroutine.
	go func() {
		fmt.Fprint(os.Stdout, clearScreen)
		ticker := time.NewTicker(refreshRate)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.render()
			case <-d.done:
				d.render() // final render
				return
			}
		}
	}()
}

// Wait blocks until the dashboard is done rendering.
func (d *Dashboard) Wait() {
	<-d.done
}

func (d *Dashboard) render() {
	elapsed := time.Since(d.startTime).Round(time.Second)
	started := int(d.runner.stats.Started.Load())
	completed := int(d.runner.stats.Completed.Load())
	failed := int(d.runner.stats.Failed.Load())
	running := started - completed - failed
	files := d.runner.stats.FilesCreated.Load()
	bytes := d.runner.stats.BytesWritten.Load()
	snapshots := d.runner.stats.Snapshots.Load()
	retries := d.runner.stats.Retries.Load()

	// Progress bar (40 chars wide).
	barWidth := 40
	var bar, progressLabel string
	if d.total > 0 {
		pct := completed * 100 / d.total
		filled := barWidth * completed / d.total
		bar = strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
		progressLabel = fmt.Sprintf("%d/%d  %d%%", completed, d.total, pct)
	} else {
		// Continuous mode — animate a cycling bar.
		pos := int(time.Since(d.startTime).Seconds()*4) % barWidth
		chars := make([]byte, barWidth)
		for i := range chars {
			chars[i] = '-'
		}
		for i := range 4 {
			chars[(pos+i)%barWidth] = '='
		}
		bar = string(chars)
		progressLabel = fmt.Sprintf("%d completed  ∞", completed)
	}

	// Throughput.
	elapsedMin := elapsed.Seconds() / 60.0
	wfPerMin := 0.0
	if elapsedMin > 0.1 {
		wfPerMin = float64(completed) / elapsedMin
	}

	var b strings.Builder
	b.WriteString(cursorHome)

	// Header.
	fmt.Fprintf(&b, "%s╔══════════════════════════════════════════════════════════════════╗%s\n", colorBold, colorReset)
	fmt.Fprintf(&b, "%s║  TemporalFS Research Agent Demo                  Elapsed: %5s ║%s\n", colorBold, elapsed, colorReset)
	fmt.Fprintf(&b, "%s╠══════════════════════════════════════════════════════════════════╣%s\n", colorBold, colorReset)
	fmt.Fprintf(&b, "║                                                                  ║\n")

	// Progress bar.
	fmt.Fprintf(&b, "║  Progress  [%s%s%s]  %s%-18s%s   ║\n",
		colorCyan, bar, colorReset,
		colorBold, progressLabel, colorReset)
	fmt.Fprintf(&b, "║                                                                  ║\n")

	// Status counts.
	fmt.Fprintf(&b, "║  %sRunning: %-4d%s  %sCompleted: %-4d%s  %sRetrying: %-4d%s  %sFailed: %d%s    ║\n",
		colorYellow, running, colorReset,
		colorGreen, completed, colorReset,
		colorRed, retries, colorReset,
		colorRed, failed, colorReset)
	fmt.Fprintf(&b, "║                                                                  ║\n")

	// Throughput section.
	fmt.Fprintf(&b, "║  %s── Throughput ─────────────────────────────────────────────────%s ║\n", colorDim, colorReset)
	fmt.Fprintf(&b, "║  Workflows/min: %s%-6.0f%s  Files: %s%-6d%s  Snapshots: %s%-6d%s        ║\n",
		colorCyan, wfPerMin, colorReset,
		colorCyan, files, colorReset,
		colorCyan, snapshots, colorReset)
	fmt.Fprintf(&b, "║  Data written: %s%-10s%s  Total retries: %s%-6d%s                 ║\n",
		colorCyan, humanBytes(bytes), colorReset,
		colorCyan, retries, colorReset)
	fmt.Fprintf(&b, "║                                                                  ║\n")

	// Live activity feed.
	fmt.Fprintf(&b, "║  %s── Live Activity Feed ────────────────────────────────────────%s ║\n", colorDim, colorReset)

	d.mu.Lock()
	feed := make([]FeedEntry, len(d.feed))
	copy(feed, d.feed)
	d.mu.Unlock()

	for i := range maxFeedLines {
		if i < len(feed) {
			e := feed[i]
			icon, color := stateIcon(e.State)
			slug := truncate(e.TopicSlug, 24)
			step := truncate(e.StepName, 14)
			fmt.Fprintf(&b, "║  %s%s %-24s  %-14s  %-7s %s%s   ║\n",
				color, icon, slug, step, e.State, e.StepIdx, colorReset)
		} else {
			fmt.Fprintf(&b, "║                                                                  ║\n")
		}
	}

	fmt.Fprintf(&b, "║                                                                  ║\n")
	fmt.Fprintf(&b, "║  Temporal UI: %shttp://localhost:8233%s                              ║\n", colorCyan, colorReset)
	fmt.Fprintf(&b, "%s╚══════════════════════════════════════════════════════════════════╝%s\n", colorBold, colorReset)

	fmt.Fprint(os.Stdout, b.String())
}

func stateIcon(state string) (string, string) {
	switch state {
	case "done":
		return "✓", colorGreen
	case "running":
		return "→", colorYellow
	case "retry":
		return "↻", colorRed
	case "failed":
		return "✗", colorRed
	default:
		return "·", colorDim
	}
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-1] + "…"
}

func humanBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
