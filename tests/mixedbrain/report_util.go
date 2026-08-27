package mixedbrain

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const mixedBrainSummaryFile = "mixedbrain-summary.md"

type mixedBrainOmesError struct {
	Scenario string
	omesLogFinding
}

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
	OmesErrors     []mixedBrainOmesError
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
	if len(r.OmesErrors) > 0 {
		fmt.Fprintf(&out, "- **Recurring Omes errors:** %d\n", len(r.OmesErrors))
		out.WriteString("\n## Recurring Omes errors\n\n")
		out.WriteString("| Scenario | Level | Error | Occurrences | First seen |\n")
		out.WriteString("| --- | --- | --- | ---: | --- |\n")
		for _, finding := range r.OmesErrors {
			fmt.Fprintf(&out, "| %s | %s | %s | %d | %s |\n",
				markdownTableValue(finding.Scenario),
				finding.level,
				markdownTableValue(finding.message),
				finding.count,
				valueOrUnavailable(finding.firstSeen),
			)
		}
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
				markdownTableValue(result),
			)
		}
	}
	return out.String()
}

func markdownTableValue(value string) string {
	return strings.ReplaceAll(value, "|", "\\|")
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
		collectOmesErrorFindings(t, report)
		if err := os.WriteFile(filepath.Join(outputRoot, mixedBrainSummaryFile), []byte(report.markdown()), 0644); err != nil {
			t.Errorf("write mixed-brain summary: %v", err)
		}
	})
}

func collectOmesErrorFindings(t *testing.T, report *mixedBrainReport) {
	t.Helper()
	report.OmesErrors = nil
	for name, path := range report.Logs {
		if !strings.HasPrefix(name, "Omes ") {
			continue
		}
		synopsis, err := summarizeOmesFailure(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Logf("scan %s log: %v", name, err)
			continue
		}
		for _, finding := range synopsis.errorFindings {
			report.OmesErrors = append(report.OmesErrors, mixedBrainOmesError{
				Scenario:       strings.TrimPrefix(name, "Omes "),
				omesLogFinding: finding,
			})
		}
	}
	sort.Slice(report.OmesErrors, func(i, j int) bool {
		if report.OmesErrors[i].Scenario != report.OmesErrors[j].Scenario {
			return report.OmesErrors[i].Scenario < report.OmesErrors[j].Scenario
		}
		return report.OmesErrors[i].count > report.OmesErrors[j].count
	})
}
