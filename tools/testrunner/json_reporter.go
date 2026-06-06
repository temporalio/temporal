package testrunner

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"strings"
	"sync"
	"time"
)

type goTestJSONReporter struct {
	mu            sync.Mutex
	raw           strings.Builder
	plain         bytes.Buffer
	lineBuf       []byte
	report        *junitReport
	console       io.Writer
	onReportFlush func()
	lastFlush     time.Time
}

func newGoTestJSONReporter(report *junitReport, console io.Writer, onReportFlush func()) *goTestJSONReporter {
	return &goTestJSONReporter{
		report:        report,
		console:       console,
		onReportFlush: onReportFlush,
		lastFlush:     time.Now(),
	}
}

func (w *goTestJSONReporter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.lineBuf = append(w.lineBuf, p...)
	w.processLines(false)
	raw := w.raw.String()
	shouldFlush := time.Since(w.lastFlush) >= reportFlushInterval
	if shouldFlush {
		w.lastFlush = time.Now()
	}
	w.mu.Unlock()
	if shouldFlush {
		w.writeReport(raw)
	}
	return len(p), nil
}

func (w *goTestJSONReporter) Close() {
	w.mu.Lock()
	w.processLines(true)
	raw := w.raw.String()
	w.mu.Unlock()
	w.writeReport(raw)
}

func (w *goTestJSONReporter) PlainOutput() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.plain.String()
}

func (w *goTestJSONReporter) processLines(flush bool) {
	for {
		idx := bytes.IndexByte(w.lineBuf, '\n')
		if idx < 0 {
			if flush && len(w.lineBuf) > 0 {
				w.writePlainLine(w.lineBuf)
				w.lineBuf = nil
			}
			return
		}
		line := w.lineBuf[:idx+1]
		w.lineBuf = w.lineBuf[idx+1:]
		w.raw.Write(line)
		w.writePlainLine(line)
	}
}

func (w *goTestJSONReporter) writePlainLine(line []byte) {
	var ev struct {
		Output string `json:"Output"`
	}
	output := string(line)
	if err := json.Unmarshal(bytes.TrimSpace(line), &ev); err == nil {
		output = ev.Output
	}
	if output == "" {
		return
	}
	w.plain.WriteString(output)
	_, _ = io.WriteString(w.console, output)
}

func (w *goTestJSONReporter) writeReport(raw string) {
	if strings.TrimSpace(raw) == "" {
		return
	}
	report, err := newJUnitReportFromGoTestJSON(raw)
	if err != nil {
		log.Printf("warning: failed to convert go test JSON to JUnit: %v", err)
		return
	}
	w.report.Testsuites = report.Testsuites
	if err := w.report.write(); err != nil {
		log.Printf("warning: failed to write in-progress JUnit report: %v", err)
		return
	}
	if w.onReportFlush != nil {
		w.onReportFlush()
	}
}
