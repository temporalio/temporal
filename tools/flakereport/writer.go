package flakereport

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// writeReportFiles writes all report files to output directory
// Files: flaky.txt, flaky_slack.txt, flaky_count.txt, timeout.txt, crash.txt
func writeReportFiles(outputDir string, summary *ReportSummary, maxLinks int) error {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Generate report content (all flaky tests shown)
	flakyMarkdown, flakySlack, flakyCount := generateFlakyReport(summary.FlakyTests, maxLinks)
	timeoutMarkdown := generateStandardReport(summary.Timeouts, maxLinks)
	crashMarkdown := generateStandardReport(summary.Crashes, maxLinks)
	ciBreakerMarkdown, _ := generateCIBreakerReport(summary.CIBreakers, maxLinks)

	// Write flaky.txt (markdown with links, top 10)
	if err := os.WriteFile(filepath.Join(outputDir, "flaky.txt"), []byte(flakyMarkdown), 0644); err != nil {
		return fmt.Errorf("failed to write flaky.txt: %w", err)
	}

	// Write flaky_slack.txt (plain text without links, top 10)
	if err := os.WriteFile(filepath.Join(outputDir, "flaky_slack.txt"), []byte(flakySlack), 0644); err != nil {
		return fmt.Errorf("failed to write flaky_slack.txt: %w", err)
	}

	// Write flaky_count.txt (total count of flaky tests)
	countStr := strconv.Itoa(flakyCount)
	if err := os.WriteFile(filepath.Join(outputDir, "flaky_count.txt"), []byte(countStr), 0644); err != nil {
		return fmt.Errorf("failed to write flaky_count.txt: %w", err)
	}

	// Write timeout.txt (all timeouts)
	if err := os.WriteFile(filepath.Join(outputDir, "timeout.txt"), []byte(timeoutMarkdown), 0644); err != nil {
		return fmt.Errorf("failed to write timeout.txt: %w", err)
	}

	// Write crash.txt (all crashes)
	if err := os.WriteFile(filepath.Join(outputDir, "crash.txt"), []byte(crashMarkdown), 0644); err != nil {
		return fmt.Errorf("failed to write crash.txt: %w", err)
	}

	// Write ci_breakers.txt (tests that broke CI by failing all retries)
	if err := os.WriteFile(filepath.Join(outputDir, "ci_breakers.txt"), []byte(ciBreakerMarkdown), 0644); err != nil {
		return fmt.Errorf("failed to write ci_breakers.txt: %w", err)
	}

	fmt.Printf("Report files written to %s\n", outputDir)
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
		ciBreakerMarkdown, _ := generateCIBreakerReport(summary.CIBreakers, maxLinks)
		content += ciBreakerMarkdown + "\n\n"
	}

	// Crashes section
	if len(summary.Crashes) > 0 {
		content += "### Crashes\n\n"
		crashReport := generateStandardReport(summary.Crashes, maxLinks)
		content += crashReport + "\n\n"
	}

	// Timeouts section
	if len(summary.Timeouts) > 0 {
		content += "### Timeouts\n\n"
		timeoutReport := generateStandardReport(summary.Timeouts, maxLinks)
		content += timeoutReport + "\n\n"
	}

	// Flaky tests section (show ALL tests)
	if len(summary.FlakyTests) > 0 {
		content += "### Flaky Tests\n\n"
		flakyMarkdown, _, _ := generateFlakyReport(summary.FlakyTests, maxLinks)
		content += flakyMarkdown + "\n\n"
	}

	// Link to run
	if runID != "" {
		content += fmt.Sprintf("\n[View Full Report & Artifacts](https://github.com/%s/actions/runs/%s)\n", defaultRepository, runID)
	}

	return content
}

// writeGitHubSummary writes summary to GITHUB_STEP_SUMMARY env var
func writeGitHubSummary(content string) error {
	summaryFile := os.Getenv("GITHUB_STEP_SUMMARY")
	if summaryFile == "" {
		return errors.New("GITHUB_STEP_SUMMARY environment variable not set")
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

	fmt.Println("GitHub Actions summary written successfully")
	return nil
}
