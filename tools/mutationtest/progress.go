package mutationtest

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type progressStats struct {
	killed    int
	skipped   int
	survived  int
	survivors map[string]int
}

type progressWriter struct {
	lineBuf     bytes.Buffer
	mu          sync.Mutex
	logFile     io.Writer
	shard       int
	survived    int
	killed      int
	skipped     int
	survivors   map[string]int
	lastEvent   string
	pendingSkip bool
}

func newProgressWriter(shard int, logFile io.Writer) *progressWriter {
	return &progressWriter{shard: shard, logFile: logFile}
}

func (w *progressWriter) Stats() progressStats {
	w.mu.Lock()
	defer w.mu.Unlock()

	survivors := make(map[string]int, len(w.survivors))
	for file, count := range w.survivors {
		survivors[file] = count
	}
	return progressStats{
		killed:    w.killed,
		skipped:   w.skipped,
		survived:  w.survived,
		survivors: survivors,
	}
}

func (w *progressWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.logFile.Write(p); err != nil {
		return 0, err
	}
	for _, b := range p {
		if err := w.lineBuf.WriteByte(b); err != nil {
			return 0, err
		}
		if b == '\n' {
			w.handleLine(strings.TrimSpace(w.lineBuf.String()))
			w.lineBuf.Reset()
		}
	}
	return len(p), nil
}

func (w *progressWriter) handleLine(line string) {
	if line == "" {
		return
	}
	switch {
	case strings.HasPrefix(line, "PASS "):
		w.pendingSkip = false
		w.killed++
		w.printProgress("killed", line)
	case strings.HasPrefix(line, "FAIL "):
		if w.pendingSkip {
			w.pendingSkip = false
			w.skipped++
			w.printProgress("skipped", line)
			return
		}
		w.survived++
		w.recordSurvivor(line)
		w.printProgress("survived", line)
	case strings.Contains(line, "Mutation did not compile"):
		w.pendingSkip = true
		w.skipped++
		w.printProgress("skipped", line)
	case line == "exit status 2":
		w.pendingSkip = true
	case isBuildFailureLine(line):
		w.pendingSkip = true
	}
}

func (w *progressWriter) recordSurvivor(line string) {
	const quoted = `"`
	start := strings.Index(line, quoted)
	if start == -1 {
		return
	}
	start++
	end := strings.Index(line[start:], quoted)
	if end == -1 {
		return
	}
	file := filepath.ToSlash(line[start : start+end])
	if w.survivors == nil {
		w.survivors = make(map[string]int)
	}
	w.survivors[file]++
}

func (w *progressWriter) printProgress(event string, line string) {
	if event == w.lastEvent && line == "" {
		return
	}
	w.lastEvent = event
	fmt.Fprintf(
		os.Stderr,
		"[shard %d] killed=%d survived=%d skipped=%d :: %s\n",
		w.shard,
		w.killed,
		w.survived,
		w.skipped,
		line,
	)
}

func isBuildFailureLine(line string) bool {
	return strings.Contains(line, "[build failed]") ||
		strings.Contains(line, "undefined:") ||
		strings.Contains(line, "syntax error:")
}

func printMutationSummary(repoRoot string, runDir string, results []shardResult) {
	var total progressStats
	total.survivors = make(map[string]int)
	lines := make([]string, 0, len(results)+16)
	lines = append(lines, "Mutation Summary", "")
	lines = append(lines, "Overall")
	for _, result := range results {
		total.killed += result.stats.killed
		total.skipped += result.stats.skipped
		total.survived += result.stats.survived
		for file, count := range result.stats.survivors {
			total.survivors[file] += count
		}
	}
	lines = append(lines,
		fmt.Sprintf("  killed:    %d", total.killed),
		fmt.Sprintf("  survived:  %d", total.survived),
		fmt.Sprintf("  skipped:   %d", total.skipped),
		fmt.Sprintf("  total:     %d", total.killed+total.survived+total.skipped),
	)

	lines = append(lines, "", "Logs")
	for _, result := range results {
		if result.logPath == "" {
			continue
		}
		lines = append(lines, "  "+displayPath(repoRoot, result.logPath))
	}
	lines = append(lines, "", "Surviving Diffs")
	lines = append(lines, "  "+displayPath(repoRoot, filepath.Join(runDir, "survivors.diff")))

	fmt.Println()
	for _, line := range lines {
		fmt.Println(line)
	}
	summaryPath := filepath.Join(runDir, "summary.txt")
	_ = writeLines(summaryPath, lines)
	fmt.Fprintf(os.Stderr, "[run] wrote mutation summary to %s\n", displayPath(repoRoot, summaryPath))
}
