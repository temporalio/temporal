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
		content += generateTestReportTable(summary.CIBreakers, "CI Break Rate", maxLinks) + "\n"
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

	// Flaky tests section (show ALL tests)
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
		content += fmt.Sprintf("\n[View Full Report & Artifacts](https://github.com/%s/actions/runs/%s)\n", defaultRepository, runID)
	}

	return content
}

// bisectHotCommit groups commit-level data used when building cross-test summaries.
type bisectHotCommit struct {
	title string
	tests []string
	prob  float64
}

// bisectHotEntry pairs a SHA with its cross-test data for sorting.
type bisectHotEntry struct {
	sha string
	cc  *bisectHotCommit
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

// buildBisectHotList builds a sorted list of commits that are the #1 suspect in 2+ tests.
func buildBisectHotList(reports []TestBisectReport) []bisectHotEntry {
	hotCommits := make(map[string]*bisectHotCommit)
	for _, r := range reports {
		if r.Skipped || len(r.TopSuspects) == 0 {
			continue
		}
		top := r.TopSuspects[0]
		if _, ok := hotCommits[top.CommitSHA]; !ok {
			hotCommits[top.CommitSHA] = &bisectHotCommit{title: top.CommitTitle, prob: top.Probability}
		}
		hotCommits[top.CommitSHA].tests = append(hotCommits[top.CommitSHA].tests, r.TestName)
	}
	var hotList []bisectHotEntry
	for sha, cc := range hotCommits {
		if len(cc.tests) >= 2 {
			hotList = append(hotList, bisectHotEntry{sha, cc})
		}
	}
	sort.Slice(hotList, func(i, j int) bool {
		return len(hotList[i].cc.tests) > len(hotList[j].cc.tests)
	})
	return hotList
}

// writeBisectHotCommitsTable writes the cross-test hot commits markdown table into sb.
func writeBisectHotCommitsTable(sb *strings.Builder, hotList []bisectHotEntry, repo string) {
	if len(hotList) == 0 {
		return
	}
	sb.WriteString("### Cross-Test Hot Commits\n\n")
	sb.WriteString("| Commit | Tests | Title |\n")
	sb.WriteString("|--------|-------|-------|\n")
	for _, h := range hotList {
		shortSHA := h.sha
		if len(shortSHA) > 7 {
			shortSHA = shortSHA[:7]
		}
		commitURL := fmt.Sprintf("https://github.com/%s/commit/%s", repo, h.sha)
		title := h.cc.title
		if title == h.sha || title == "" {
			title = shortSHA
		}
		fmt.Fprintf(sb, "| [%s](%s) | %d | %s |\n",
			shortSHA, commitURL, len(h.cc.tests), title)
	}
	sb.WriteString("\n")
}

// writeBisectPerTestTable writes per-test suspect tables into sb.
func writeBisectPerTestTable(sb *strings.Builder, reports []TestBisectReport, repo string) {
	sb.WriteString("### Per-Test Suspects\n\n")
	for _, r := range reports {
		if r.Skipped || len(r.TopSuspects) == 0 {
			continue
		}
		overallRate := 0.0
		if r.TotalObs > 0 {
			top := r.TopSuspects[0]
			allFails := top.FailsBefore + top.FailsAfter
			overallRate = float64(allFails) / float64(r.TotalObs) * 100.0
		}
		fmt.Fprintf(sb, "**`%s`** (%.1f%% flake, %d runs)\n\n", r.TestName, overallRate, r.TotalObs)
		sb.WriteString("| Prob | Commit | Author | Before | After | Note |\n")
		sb.WriteString("|------|--------|--------|--------|-------|------|\n")
		for _, s := range r.TopSuspects {
			shortSHA := s.CommitSHA
			if len(shortSHA) > 7 {
				shortSHA = shortSHA[:7]
			}
			commitURL := fmt.Sprintf("https://github.com/%s/commit/%s", repo, s.CommitSHA)
			beforeStr := fmt.Sprintf("%d/%d (%.0f%%)", s.FailsBefore, s.PassesBefore+s.FailsBefore,
				pct(s.FailsBefore, s.PassesBefore+s.FailsBefore))
			afterStr := fmt.Sprintf("%d/%d (%.0f%%)", s.FailsAfter, s.PassesAfter+s.FailsAfter,
				pct(s.FailsAfter, s.PassesAfter+s.FailsAfter))
			title := s.CommitTitle
			if title == s.CommitSHA || title == "" {
				title = shortSHA
			}
			fmt.Fprintf(sb, "| %.1f%% | [%s](%s) %s | %s | %s | %s | %s |\n",
				s.Probability*100, shortSHA, commitURL, title, s.CommitAuthor,
				beforeStr, afterStr, s.HeuristicNote)
		}
		sb.WriteString("\n")
	}
}

// generateBisectSummary creates the markdown section for bisect results to append to the GitHub summary.
func generateBisectSummary(reports []TestBisectReport, repo string) string {
	qualifying := countQualifyingBisectReports(reports)

	skipped := len(reports) - qualifying

	var sb strings.Builder
	sb.WriteString("\n## Bayesian Commit Suspects\n\n")

	if qualifying == 0 {
		sb.WriteString("No actionable commit suspects found")
		if skipped > 0 {
			sb.WriteString(fmt.Sprintf(" — %d tests analyzed but none above 50%% confidence", skipped))
		}
		sb.WriteString("\n")
		return sb.String()
	}

	sb.WriteString(fmt.Sprintf("%d tests with actionable suspects (≥50%% confidence)", qualifying))
	if skipped > 0 {
		sb.WriteString(fmt.Sprintf(", %d below confidence threshold", skipped))
	}
	sb.WriteString("\n\n")

	hotList := buildBisectHotList(reports)
	writeBisectHotCommitsTable(&sb, hotList, repo)
	writeBisectPerTestTable(&sb, reports, repo)
	return sb.String()
}

// pct returns percentage of num/denom, returning 0 if denom is 0.
func pct(num, denom int) float64 {
	if denom == 0 {
		return 0
	}
	return float64(num) / float64(denom) * 100.0
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
