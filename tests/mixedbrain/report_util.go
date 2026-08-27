package mixedbrain

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	mixedBrainSummaryFile = "mixedbrain-summary.md"
	diagnosticMaxLines    = 100
	diagnosticMaxBytes    = 64 << 10
)

type mixedBrainReport struct {
	StartedAt      time.Time
	FinishedAt     time.Time
	Passed         bool
	CurrentVersion string
	ReleaseVersion string
	Scenarios      []string
	ChaosInterval  time.Duration
	ChaosEvents    *chaosEvents
	ProxyCounts    map[string]int64
	Logs           map[string]string
}

func (r *mixedBrainReport) markdown() string {
	finishedAt := r.FinishedAt
	if finishedAt.IsZero() {
		finishedAt = time.Now()
	}
	result := "FAILED"
	if r.Passed {
		result = "PASSED"
	}
	var out strings.Builder
	fmt.Fprintf(&out, "# Mixed-brain test summary\n\n")
	fmt.Fprintf(&out, "- Result: **%s**\n", result)
	fmt.Fprintf(&out, "- Duration: %s\n", finishedAt.Sub(r.StartedAt).Round(time.Millisecond))
	fmt.Fprintf(&out, "- Current version: %s\n", valueOrUnavailable(r.CurrentVersion))
	fmt.Fprintf(&out, "- Previous release: %s\n", valueOrUnavailable(r.ReleaseVersion))
	fmt.Fprintf(&out, "- Scenarios: %s\n", valueOrUnavailable(strings.Join(r.Scenarios, ", ")))
	if r.ChaosInterval > 0 {
		fmt.Fprintf(&out, "- Process chaos interval: %s\n", r.ChaosInterval)
	} else {
		fmt.Fprintf(&out, "- Process chaos interval: unavailable\n")
	}

	var events []processChaosEvent
	if r.ChaosEvents != nil {
		events = r.ChaosEvents.snapshot()
	}
	fmt.Fprintf(&out, "- Process restarts: %d\n", len(events))
	if len(r.ProxyCounts) > 0 {
		fmt.Fprintf(&out, "- Proxy traffic: current=%d, release=%d\n", r.ProxyCounts["current"], r.ProxyCounts["release"])
	} else {
		fmt.Fprintf(&out, "- Proxy traffic: unavailable\n")
	}
	if len(events) > 0 {
		out.WriteString("\n## Process restarts\n\n")
		out.WriteString("| Target | Started | Restart completed | Cluster reformed | Result |\n")
		out.WriteString("| --- | --- | --- | --- | --- |\n")
		for _, event := range events {
			result := "success"
			if event.Err != "" {
				result = event.Err
			}
			fmt.Fprintf(&out, "| %s | %s | %s | %s | %s |\n",
				event.Target,
				formatEventTime(event.StartedAt),
				formatEventTime(event.RestartedAt),
				formatEventTime(event.ReformedAt),
				strings.ReplaceAll(result, "|", "\\|"),
			)
		}
	}
	return out.String()
}

func valueOrUnavailable(value string) string {
	if value == "" {
		return "unavailable"
	}
	return value
}

func formatEventTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.UTC().Format(time.RFC3339)
}

func registerMixedBrainReport(t *testing.T, outputRoot string, report *mixedBrainReport, proxyRef **frontendProxy) {
	t.Helper()
	t.Cleanup(func() {
		report.FinishedAt = time.Now()
		report.Passed = !t.Failed()
		if *proxyRef != nil {
			report.ProxyCounts = map[string]int64{
				"current": (*proxyRef).callCount[0].Load(),
				"release": (*proxyRef).callCount[1].Load(),
			}
		}
		if err := os.WriteFile(filepath.Join(outputRoot, mixedBrainSummaryFile), []byte(report.markdown()), 0644); err != nil {
			t.Errorf("write mixed-brain summary: %v", err)
		}
		if report.Passed {
			return
		}
		for name, path := range report.Logs {
			if strings.HasPrefix(name, "Omes ") {
				synopsis, err := summarizeOmesFailure(path)
				if os.IsNotExist(err) {
					continue
				}
				if err != nil {
					t.Logf("scan %s log: %v", name, err)
					continue
				}
				if findings := synopsis.formatErrorFindings(); findings != "" {
					t.Logf("Aggregated %s log errors (%s):\n%s", name, path, findings)
					continue
				}
			}
			tail, err := boundedLogTail(path, diagnosticMaxLines, diagnosticMaxBytes)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				t.Logf("read %s diagnostics: %v", name, err)
				continue
			}
			t.Logf("Trailing %s log (%s):\n%s", name, path, tail)
		}
	})
}

func boundedLogTail(path string, maxLines, maxBytes int) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if len(data) > maxBytes {
		data = data[len(data)-maxBytes:]
		if newline := bytes.IndexByte(data, '\n'); newline >= 0 {
			data = data[newline+1:]
		}
	}
	lines := bytes.Split(bytes.TrimRight(data, "\n"), []byte{'\n'})
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	return string(bytes.Join(lines, []byte{'\n'})), nil
}
