package flakereport

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/server/tools/common/github"
)

// writeFailuresJSON writes failures.json containing every individual test failure (for analytics).
func writeFailuresJSON(outputDir string, failures []TestFailure, repo string) error {
	records := make([]FailedTestRecord, 0, len(failures))
	for _, f := range failures {
		runIDStr := strconv.FormatInt(f.RunID, 10)
		records = append(records, FailedTestRecord{
			SuiteName:   f.SuiteName,
			TestName:    f.Name,
			FailureDate: f.Timestamp.Format(time.RFC3339),
			Link:        buildGitHubURL(repo, runIDStr, f.JobID),
			FailureType: classifyFailure(f.Name),
		})
	}

	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal failures.json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "failures.json"), data, 0644); err != nil {
		return fmt.Errorf("failed to write failures.json: %w", err)
	}
	return nil
}

// generateGitHubSummary creates markdown summary for GitHub Actions
func generateGitHubSummary(summary *ReportSummary, bisectReports []TestBisectReport, repo, runID string, maxLinks int, minProbability float64) string {
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	var content string
	content += fmt.Sprintf("## Flaky Tests Report - %s\n\n", timestamp)

	// Overall statistics
	content += "### Overall Statistics\n\n"

	// CI success rate
	ciSuccessRate := 0.0
	if summary.TotalWorkflowRuns > 0 {
		ciSuccessRate = (float64(summary.SuccessfulRuns) / float64(summary.TotalWorkflowRuns)) * 100.0
	}
	content += fmt.Sprintf("* **CI Success Rate**: %d/%d (%.2f%%)\n", summary.SuccessfulRuns, summary.TotalWorkflowRuns, ciSuccessRate)
	content += fmt.Sprintf("* **Total Test Runs**: %d\n", summary.TotalTestRuns)
	content += fmt.Sprintf("* **Total Failures**: %d\n", summary.TotalFailures)
	content += fmt.Sprintf("* **Overall Failure Rate**: %.1f per 1000 tests\n\n", summary.OverallFailureRate)

	if len(bisectReports) > 0 {
		content += generateBisectSummary(bisectReports, repo, minProbability)
	}

	// Summary table
	content += "### Failure Categories Summary\n\n"
	content += "| Category | Unique Entries |\n"
	content += "|----------|----------------|\n"
	content += fmt.Sprintf("| CI Execution Interruptions | %d |\n", len(summary.TestRunnerTimeouts))
	content += fmt.Sprintf("| Final-Retry Test Failures | %d |\n", len(summary.CIBreakers))
	content += fmt.Sprintf("| Crashes | %d |\n", len(summary.Crashes))
	content += fmt.Sprintf("| Timeouts | %d |\n", len(summary.Timeouts))
	content += fmt.Sprintf("| Flaky Tests | %d |\n\n", summary.TotalFlakyCount)

	if len(summary.TestRunnerTimeouts) > 0 {
		content += "### CI Execution Interruptions\n\n"
		content += generateOccurrenceReportTable(summary.TestRunnerTimeouts, "Event", "Affected Artifacts", maxLinks) + "\n"
	}

	// Final-retry failures are tracked per artifact, not per GitHub Actions workflow run.
	if len(summary.CIBreakers) > 0 {
		content += "### Final-Retry Test Failures\n\n"
		content += generateOccurrenceReportTable(summary.CIBreakers, "Test", "Affected Artifacts", maxLinks) + "\n"
	}

	// Crashes section
	if len(summary.Crashes) > 0 {
		content += "### Crashes\n\n"
		content += generateTestReportTable(summary.Crashes, "Crash Rate", maxLinks) + "\n"
	}

	// Timeouts section
	if len(summary.Timeouts) > 0 {
		content += "### Timeouts\n\n"
		content += generateTestReportTable(summary.Timeouts, "Flake Rate", maxLinks) + "\n"
	}

	// Flaky tests section
	if len(summary.FlakyTests) > 0 {
		content += "### Flaky Tests\n\n"
		content += generateTestReportTable(summary.FlakyTests, "Flake Rate", maxLinks) + "\n"
	}

	// Flaky suites
	if len(summary.Suites) > 0 {
		content += "### Flaky Suites\n\n"
		content += generateSuiteBreakdownTable(summary.Suites) + "\n"
	}

	// Link to run
	if runID != "" {
		content += fmt.Sprintf("\n[View Full Report & Artifacts](%s)\n", github.RunURL(defaultRepository, runID))
	}

	return content
}

// countQualifyingBisectReports returns the number of non-skipped reports.
func countQualifyingBisectReports(reports []TestBisectReport) int {
	n := 0
	for _, r := range reports {
		if !r.Skipped {
			n++
		}
	}
	return n
}

// escapeTableCell replaces pipe characters so they don't corrupt GFM table rows.
func escapeTableCell(s string) string {
	return strings.ReplaceAll(s, "|", "&#124;")
}

type bisectTableRow struct {
	testName string
	suspect  BisectResult
}

func buildBisectTableRows(reports []TestBisectReport) []bisectTableRow {
	rankedReports := make([]TestBisectReport, 0, len(reports))
	for _, report := range reports {
		if !report.Skipped && len(report.TopSuspects) > 0 {
			rankedReports = append(rankedReports, report)
		}
	}
	sort.Slice(rankedReports, func(i, j int) bool {
		pi := rankedReports[i].TopSuspects[0].Probability
		pj := rankedReports[j].TopSuspects[0].Probability
		if pi != pj {
			return pi > pj
		}
		return rankedReports[i].TestName < rankedReports[j].TestName
	})

	var rows []bisectTableRow
	for _, report := range rankedReports {
		for _, suspect := range report.TopSuspects {
			rows = append(rows, bisectTableRow{testName: report.TestName, suspect: suspect})
		}
	}
	return rows
}

// writeBisectTable writes suspect (test, commit) pairs into a single flat table.
func writeBisectTable(sb *strings.Builder, rows []bisectTableRow, repo string) {
	sb.WriteString("| Test | Prob | Commit | Date | Author | Before | After | Note |\n")
	sb.WriteString("|------|------|--------|------|--------|--------|-------|------|\n")
	for _, row := range rows {
		s := row.suspect
		shortSHA := s.CommitSHA
		if len(shortSHA) > 7 {
			shortSHA = shortSHA[:7]
		}
		commitURL := github.CommitURL(repo, s.CommitSHA)
		title := s.CommitTitle
		if title == s.CommitSHA || title == "" {
			title = shortSHA
		}
		beforeStr := fmt.Sprintf("%d/%d (%.0f%%)", s.FailsBefore, s.PassesBefore+s.FailsBefore,
			pct(s.FailsBefore, s.PassesBefore+s.FailsBefore))
		afterStr := fmt.Sprintf("%d/%d (%.0f%%)", s.FailsAfter, s.PassesAfter+s.FailsAfter,
			pct(s.FailsAfter, s.PassesAfter+s.FailsAfter))
		fmt.Fprintf(sb, "| `%s` | %.1f%% | [%s](%s) %s | %s | %s | %s | %s | %s |\n",
			escapeTableCell(row.testName), s.Probability*100, shortSHA, commitURL, escapeTableCell(title),
			s.CommitDate, escapeTableCell(s.CommitAuthor), beforeStr, afterStr, escapeTableCell(s.HeuristicNote))
	}
	sb.WriteString("\n")
}

// generateBisectSummary creates the markdown section for Bayesian bisect results.
func generateBisectSummary(reports []TestBisectReport, repo string, minProb float64) string {
	qualifying := countQualifyingBisectReports(reports)

	skipped := len(reports) - qualifying
	threshold := fmt.Sprintf("%.0f%%", minProb*100)

	var sb strings.Builder
	sb.WriteString("\n### Bayesian Commit Suspects\n\n")

	if qualifying == 0 {
		sb.WriteString("No actionable commit suspects found")
		if skipped > 0 {
			sb.WriteString(fmt.Sprintf(" — %d tests analyzed but none above %s confidence", skipped, threshold))
		}
		sb.WriteString("\n")
		return sb.String()
	}

	sb.WriteString(fmt.Sprintf("%d tests with actionable suspects (≥%s confidence)", qualifying, threshold))
	if skipped > 0 {
		sb.WriteString(fmt.Sprintf(", %d below confidence threshold", skipped))
	}
	sb.WriteString("\n\n")

	rows := limitReportRows(buildBisectTableRows(reports))
	writeBisectTable(&sb, rows, repo)
	return sb.String()
}

// pct returns percentage of num/denom, returning 0 if denom is 0.
func pct(num, denom int) float64 {
	if denom == 0 {
		return 0
	}
	return float64(num) / float64(denom) * 100.0
}

// writeGitHubSummary writes content to GITHUB_STEP_SUMMARY (if set)
// and always writes it to outputDir/github-report.md.
func writeGitHubSummary(content, outputDir string) error {
	// Always write to output dir
	outPath := filepath.Join(outputDir, "github-report.md")
	if err := os.WriteFile(outPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write github-report.md: %w", err)
	}
	fmt.Printf("GitHub report written to %s\n", outPath)

	// Also write to GITHUB_STEP_SUMMARY if available
	summaryFile := os.Getenv("GITHUB_STEP_SUMMARY")
	if summaryFile == "" {
		return nil
	}

	file, err := os.OpenFile(summaryFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open summary file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Printf("Warning: Failed to close summary file: %v\n", err)
		}
	}()

	if _, err := file.WriteString(content); err != nil {
		return fmt.Errorf("failed to write summary: %w", err)
	}

	fmt.Println("GitHub Actions step summary written")
	return nil
}
