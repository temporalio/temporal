package flakereport

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
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
func generateGitHubSummary(summary *ReportSummary, runID string, maxLinks int) string {
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

	// Summary table
	content += "### Failure Categories Summary\n\n"
	content += "| Category | Unique Tests |\n"
	content += "|----------|--------------|\n"
	content += fmt.Sprintf("| CI Breakers | %d |\n", len(summary.CIBreakers))
	content += fmt.Sprintf("| Crashes | %d |\n", len(summary.Crashes))
	content += fmt.Sprintf("| Timeouts | %d |\n", len(summary.Timeouts))
	content += fmt.Sprintf("| Flaky Tests | %d |\n\n", summary.TotalFlakyCount)

	// CI Breakers section (tests that failed all retries)
	if len(summary.CIBreakers) > 0 {
		content += "### CI Breakers (Failed All Retries)\n\n"
		content += generateCIBreakerTable(summary.CIBreakers, maxLinks) + "\n"
	}

	// Crashes section
	if len(summary.Crashes) > 0 {
		content += "### Crashes\n\n"
		content += generateTestReportTable(summary.Crashes, maxLinks) + "\n"
	}

	// Timeouts section
	if len(summary.Timeouts) > 0 {
		content += "### Timeouts\n\n"
		content += generateTestReportTable(summary.Timeouts, maxLinks) + "\n"
	}

	// Flaky tests section (show ALL tests)
	if len(summary.FlakyTests) > 0 {
		content += "### Flaky Tests\n\n"
		content += generateTestReportTable(summary.FlakyTests, maxLinks) + "\n"
	}

	// Flaky suites
	if len(summary.Suites) > 0 {
		content += "### Flaky Suites\n\n"
		content += generateSuiteBreakdownTable(summary.Suites) + "\n"
	}

	// Link to run
	if runID != "" {
		content += fmt.Sprintf("\n[View Full Report & Artifacts](https://github.com/%s/actions/runs/%s)\n", defaultRepository, runID)
	}

	return content
}

// writeGitHubSummary writes markdown summary to GITHUB_STEP_SUMMARY (if set)
// and always writes to outputDir/github-report.md.
func writeGitHubSummary(content string, outputDir string) error {
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
