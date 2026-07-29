package cinotify

import (
	"fmt"
	"strings"

	"go.temporal.io/server/tools/common/slack"
)

const maxFailures = 5

// BuildFailureMessage creates a Slack message for CI failure
func BuildFailureMessage(report *FailureReport) *slack.Message {
	message := slack.NewMessage("CI Failed on Main")
	message.AddSection(":rotating_light: *CI Failed on Main Branch* :rotating_light:")

	// List of failed jobs
	var failedJobNames []string
	for _, job := range report.FailedJobs {
		failedJobNames = append(failedJobNames,
			fmt.Sprintf("<%s|%s>", job.URL, job.Name))
	}

	if len(report.Failures) > 0 {
		failures := report.Failures
		if len(failures) > maxFailures {
			failures = failures[:maxFailures]
		}

		var failureLines []string
		for _, failure := range failures {
			failureLines = append(failureLines, fmt.Sprintf("`%s`", failure))
		}
		message.AddSection(fmt.Sprintf(
			"*Failures (%d):* %s",
			len(report.Failures),
			strings.Join(failureLines, ", "),
		))
	}

	message.AddSection(fmt.Sprintf(
		"*Failed jobs (%d/%d):* %s",
		len(report.FailedJobs),
		report.TotalJobs,
		strings.Join(failedJobNames, ", "),
	))
	message.AddSection(fmt.Sprintf("<%s|View Run>", report.Run.URL))
	return message
}

// FormatMessageForDebug formats the message for console output
func FormatMessageForDebug(report *FailureReport) string {
	var sb strings.Builder
	fmt.Fprint(&sb, "🚨 CI Failed on Main Branch 🚨\n\n")
	if len(report.Failures) > 0 {
		var failures []string
		for _, failure := range report.Failures[:min(len(report.Failures), maxFailures)] {
			failures = append(failures, fmt.Sprintf("`%s`", failure))
		}
		fmt.Fprintf(&sb, "Failures (%d): %s\n", len(report.Failures), strings.Join(failures, ", "))
		fmt.Fprintln(&sb)
	}

	var failedJobNames []string
	for _, job := range report.FailedJobs {
		failedJobNames = append(failedJobNames, fmt.Sprintf("%s (%s)", job.Name, job.URL))
	}
	fmt.Fprintf(&sb, "Failed jobs (%d/%d): %s\n", len(report.FailedJobs), report.TotalJobs, strings.Join(failedJobNames, ", "))
	fmt.Fprintf(&sb, "\nView Run: %s\n", report.Run.URL)
	return sb.String()
}

// BuildSuccessReportMessage creates a Slack message for success report
func BuildSuccessReportMessage(report *DigestReport) *slack.Message {
	message := slack.NewMessage(fmt.Sprintf("Weekly CI Report - %s Branch", report.Branch))
	message.AddSection(fmt.Sprintf(":chart_with_upwards_trend: *Weekly CI Report - %s Branch*", report.Branch))
	message.AddSection(fmt.Sprintf(
		"*Report Period:* %s to %s",
		report.StartDate.Format("Jan 2, 2006"),
		report.EndDate.Format("Jan 2, 2006"),
	))
	message.AddFields(
		fmt.Sprintf("*Success Rate:*\n%.1f%%", report.SuccessRate),
		fmt.Sprintf("*Total Runs:*\n%d", report.TotalRuns),
		fmt.Sprintf("*Failed Runs:*\n%d", report.FailedRuns),
		fmt.Sprintf("*Successful Runs:*\n%d", report.SuccessfulRuns),
		fmt.Sprintf("*Average Duration:*\n%s", formatDuration(report.AverageDuration)),
		fmt.Sprintf("*Median Duration:*\n%s", formatDuration(report.MedianDuration)),
	)
	message.AddSection(fmt.Sprintf(
		"*Run Duration Distribution:*\n"+
			"• Under 20 minutes: %.1f%%\n"+
			"• Under 25 minutes: %.1f%%\n"+
			"• Under 30 minutes: %.1f%%",
		report.Under20MinutesPercent,
		report.Under25MinutesPercent,
		report.Under30MinutesPercent,
	))

	slowestRuns := report.slowestRuns(3)
	if len(slowestRuns) > 0 {
		var slowest []string
		for _, run := range slowestRuns {
			slowest = append(slowest, fmt.Sprintf("• <%s|%s> — %s (%s)",
				run.URL,
				run.ShortSHA(),
				formatDuration(run.Duration),
				run.Conclusion,
			))
		}
		message.AddSection(fmt.Sprintf("*Slowest Runs:*\n%s", strings.Join(slowest, "\n")))
	}

	return message
}

// FormatReportForDebug formats the success report for console output
func FormatReportForDebug(report *DigestReport) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "📊 Weekly CI Report - %s Branch\n\n", report.Branch)
	fmt.Fprintf(&sb, "Report Period: %s to %s\n\n",
		report.StartDate.Format("Jan 2, 2006"),
		report.EndDate.Format("Jan 2, 2006"))
	fmt.Fprintln(&sb, "Metrics:")
	fmt.Fprintf(&sb, "  Success Rate: %.1f%%\n", report.SuccessRate)
	fmt.Fprintf(&sb, "  Total Runs: %d\n", report.TotalRuns)
	fmt.Fprintf(&sb, "  Successful Runs: %d\n", report.SuccessfulRuns)
	fmt.Fprintf(&sb, "  Failed Runs: %d\n", report.FailedRuns)
	fmt.Fprintf(&sb, "  Average Duration: %s\n", formatDuration(report.AverageDuration))
	fmt.Fprintf(&sb, "  Median Duration: %s\n", formatDuration(report.MedianDuration))

	fmt.Fprintln(&sb, "\nRun Duration Distribution:")
	fmt.Fprintf(&sb, "  Under 20 minutes: %.1f%%\n", report.Under20MinutesPercent)
	fmt.Fprintf(&sb, "  Under 25 minutes: %.1f%%\n", report.Under25MinutesPercent)
	fmt.Fprintf(&sb, "  Under 30 minutes: %.1f%%\n", report.Under30MinutesPercent)

	slowestRuns := report.slowestRuns(3)
	if len(slowestRuns) > 0 {
		fmt.Fprintln(&sb, "\nSlowest Runs:")
		for _, run := range slowestRuns {
			fmt.Fprintf(&sb, "  %s (%s): %s\n    %s\n",
				formatDuration(run.Duration),
				run.Conclusion,
				run.ShortSHA(),
				run.URL,
			)
		}
	}

	return sb.String()
}
