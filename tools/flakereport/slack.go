package flakereport

import (
	"fmt"
	"sort"
	"strings"

	"go.temporal.io/server/tools/common/github"
	"go.temporal.io/server/tools/common/slack"
)

// buildSuccessMessage creates success notification with report summary
func buildSuccessMessage(summary *ReportSummary, runID, repo string, days int) *slack.Message {
	// Calculate CI success rate
	ciSuccessRate := 0.0
	if summary.TotalWorkflowRuns > 0 {
		ciSuccessRate = (float64(summary.SuccessfulRuns) / float64(summary.TotalWorkflowRuns)) * 100.0
	}
	testRunnerTimeouts := 0
	for _, report := range summary.TestRunnerTimeouts {
		testRunnerTimeouts += report.FailureCount
	}

	// Summary stats
	summaryText := fmt.Sprintf("*CI Success Rate:* %d/%d (%.2f%%)\n*Total Test Runs:* %d\n*Total Failures:* %d\n*Failure Rate:* %.2f per 1000 tests\n\n*CI Execution Interruptions:* %d\n*Final-Retry Test Failures:* %d\n*Crashes:* %d\n*Flaky Tests:* %d\n*Timeouts:* %d",
		summary.SuccessfulRuns,
		summary.TotalWorkflowRuns,
		ciSuccessRate,
		summary.TotalTestRuns,
		summary.TotalFailures,
		summary.OverallFailureRate,
		testRunnerTimeouts,
		len(summary.CIBreakers),
		len(summary.Crashes),
		summary.TotalFlakyCount,
		len(summary.Timeouts))

	msg := slack.NewMessage("Flaky Tests Report Generated")
	msg.AddHeader(fmt.Sprintf("Flaky Tests Report - Last %d Days", days))
	msg.AddSection(summaryText)

	// Add final-retry test failure details.
	if lines := formatOccurrenceLines(summary.CIBreakers, "affected artifacts"); len(lines) > 0 {
		if len(lines) > slackMaxListItems {
			lines = lines[:slackMaxListItems]
		}
		text := fmt.Sprintf("*Final-Retry Test Failures (top %d):*\n%s", slackMaxListItems, strings.Join(lines, "\n"))
		msg.AddSection(text)
	}

	// Add flaky tests details (already sorted by failure rate)
	var flakyFiltered []TestReport
	for _, r := range summary.FlakyTests {
		if r.FailureCount >= minFlakyFailures {
			flakyFiltered = append(flakyFiltered, r)
		}
	}
	if lines := formatReportLines(flakyFiltered); len(lines) > 0 {
		if len(lines) > slackMaxListItems {
			lines = lines[:slackMaxListItems]
		}
		text := fmt.Sprintf("*Flaky Tests (top %d):*\n%s", slackMaxListItems, strings.Join(lines, "\n"))
		msg.AddSection(text)
	}

	// Add link to report
	if runID != "" {
		linkURL := github.RunURL(repo, runID)
		msg.AddSection(fmt.Sprintf("<%s|Report & Artifacts>", linkURL))
	}

	return msg
}

// addBisectSection appends a Bayesian bisect section to an existing Slack message.
// TODO seankane: after validating this methodology can identify problematic commits
// add the section to the Slack message.
func addBisectSection(msg *slack.Message, reports []TestBisectReport, repo string) {
	// Count qualifying
	qualifying := 0
	for _, r := range reports {
		if !r.Skipped {
			qualifying++
		}
	}
	if qualifying == 0 {
		return
	}

	// Find hot commits (top suspect in 2+ tests)
	type commitCount struct {
		title string
		count int
	}
	hotCommits := make(map[string]*commitCount)
	for _, r := range reports {
		if r.Skipped || len(r.TopSuspects) == 0 {
			continue
		}
		top := r.TopSuspects[0]
		if _, ok := hotCommits[top.CommitSHA]; !ok {
			hotCommits[top.CommitSHA] = &commitCount{title: top.CommitTitle}
		}
		hotCommits[top.CommitSHA].count++
	}

	var lines []string
	for sha, cc := range hotCommits {
		if cc.count >= 2 {
			shortSHA := sha
			if len(shortSHA) > 7 {
				shortSHA = shortSHA[:7]
			}
			commitURL := github.CommitURL(repo, sha)
			title := cc.title
			if title == sha || title == "" {
				title = shortSHA
			}
			lines = append(lines, fmt.Sprintf("• <%s|%s> — %d tests — %s", commitURL, shortSHA, cc.count, title))
		}
	}

	if len(lines) == 0 {
		// No multi-test suspects; just report the count
		msg.AddSection(fmt.Sprintf(
			"*🔍 Commit Suspects (Bayesian)*\n%d tests analyzed. See GitHub summary for details.",
			qualifying,
		))
		return
	}

	// Sort lines for deterministic output
	sort.Strings(lines)
	if len(lines) > slackMaxListItems {
		lines = lines[:slackMaxListItems]
	}

	text := fmt.Sprintf("*🔍 Hot Commits (Bayesian, implicated in 2+ tests):*\n%s\n\nSee GitHub summary for full details.",
		strings.Join(lines, "\n"))
	msg.AddSection(text)
}

// buildFailureMessage creates failure notification
func buildFailureMessage(runID, refName, sha, repo string) *slack.Message {
	msg := slack.NewMessage("Flaky Tests Report Generation Failed")
	msg.AddHeader("Flaky Tests Report Generation Failed")
	msg.AddFields(
		fmt.Sprintf("*Run ID:*\n%s", runID),
		fmt.Sprintf("*Branch:*\n%s", refName),
		fmt.Sprintf("*Commit:*\n%.7s", sha),
		"*Status:*\nFailed",
	)

	// Add link to workflow run
	if runID != "" {
		linkURL := github.RunURL(repo, runID)
		msg.AddSection(fmt.Sprintf("<%s|View Workflow Run>", linkURL))
	}

	return msg
}
