package flakereport

import (
	"fmt"
	"math"
	"strings"
	"time"
)

const boldFlakeRateThreshold = 5.0

// hoursAgo formats a timestamp as "Xh ago" relative to now.
func hoursAgo(t time.Time) string {
	h := math.Round(time.Since(t).Hours())
	if h < 1 {
		h = 1
	}
	return fmt.Sprintf("%dh ago", int(h))
}

// formatReportLines returns a plain-text bullet line per report.
func formatReportLines(reports []TestReport) []string {
	var lines []string
	for _, r := range reports {
		pct := 0.0
		if r.TotalRuns > 0 {
			pct = float64(r.FailureCount) / float64(r.TotalRuns) * 100.0
		}
		lines = append(lines, fmt.Sprintf("• %.1f%% (%d failures): `%s`",
			pct, r.FailureCount, r.TestName))
	}
	return lines
}

// formatLinks formats GitHub URLs as numbered markdown links
func formatLinks(urls []string, maxLinks int) string {
	linkCount := len(urls)
	if linkCount > maxLinks {
		linkCount = maxLinks
	}
	var parts []string
	for i := 0; i < linkCount; i++ {
		parts = append(parts, fmt.Sprintf("[%d](%s)", i+1, urls[i]))
	}
	return strings.Join(parts, " ")
}

// generateSuiteBreakdownTable creates a markdown table of per-suite flake data
func generateSuiteBreakdownTable(suiteReports []SuiteReport) string {
	if len(suiteReports) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("| Suite | Flake Rate | Last Failure |\n")
	sb.WriteString("|-------|------------|-------------|\n")

	for _, sr := range suiteReports {
		lastFailure := "-"
		if sr.FailedRuns > 0 && !sr.LastFailure.IsZero() {
			lastFailure = hoursAgo(sr.LastFailure)
		}
		rate := fmt.Sprintf("%.1f%% (%d/%d)", sr.FlakeRate, sr.FailedRuns, sr.TotalRuns)
		if sr.FlakeRate > boldFlakeRateThreshold {
			rate = "**" + rate + "**"
		}
		sb.WriteString(fmt.Sprintf("| `%s` | %s | %s |\n", sr.SuiteName, rate, lastFailure))
	}

	return sb.String()
}

// generateTestReportTable creates a markdown table of per-test flake data
func generateTestReportTable(reports []TestReport, maxLinks int) string {
	if len(reports) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("| Test | Flake Rate | Last Failure | Links |\n")
	sb.WriteString("|------|------------|-------------|-------|\n")

	for _, report := range reports {
		pct := 0.0
		if report.TotalRuns > 0 {
			pct = float64(report.FailureCount) / float64(report.TotalRuns) * 100.0
		}
		links := formatLinks(report.GitHubURLs, maxLinks)
		lastFailure := "N/A"
		if !report.LastFailure.IsZero() {
			lastFailure = hoursAgo(report.LastFailure)
		}
		rate := fmt.Sprintf("%.1f%% (%d/%d)", pct, report.FailureCount, report.TotalRuns)
		if pct > boldFlakeRateThreshold {
			rate = "**" + rate + "**"
		}
		sb.WriteString(fmt.Sprintf("| `%s` | %s | %s | %s |\n",
			report.TestName, rate, lastFailure, links))
	}

	return sb.String()
}

// generateCIBreakerTable creates a markdown table for CI breakers (no flake rate column)
func generateCIBreakerTable(reports []TestReport, maxLinks int) string {
	if len(reports) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("| Test | CI Runs Broken | Total Failures | Last Failure | Links |\n")
	sb.WriteString("|------|---------------|----------------|-------------|-------|\n")

	for _, report := range reports {
		links := formatLinks(report.GitHubURLs, maxLinks)
		lastFailure := "N/A"
		if !report.LastFailure.IsZero() {
			lastFailure = hoursAgo(report.LastFailure)
		}
		sb.WriteString(fmt.Sprintf("| `%s` | %d | %d | %s | %s |\n",
			report.TestName, report.CIRunsBroken, report.FailureCount,
			lastFailure, links))
	}

	return sb.String()
}
