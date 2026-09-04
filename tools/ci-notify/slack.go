package cinotify

import (
	"cmp"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/server/tools/common/github"
	"go.temporal.io/server/tools/common/slack"
)

const (
	maxFailures                    = 5
	successRateComparisonTolerance = 0.1
	durationComparisonTolerance    = 30 * time.Second
)

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

// BuildDataRaceMessage creates a Slack message announcing data races on main.
func BuildDataRaceMessage(report *DataRaceReport) *slack.Message {
	runID := strconv.FormatInt(report.Run.DatabaseID, 10)

	message := slack.NewMessage(fmt.Sprintf("Data Race Detected on Main (%d)", len(report.DataRaces)))
	message.AddSection(":rotating_light: *Data Race Detected on Main Branch* :rotating_light:")

	for _, race := range report.DataRaces {
		var sb strings.Builder
		fmt.Fprintln(&sb, "```")
		if race.Location != "" {
			fmt.Fprintln(&sb, race.Location)
		}
		for _, site := range raceSites(race.Details) {
			fmt.Fprintln(&sb, site)
		}
		fmt.Fprintf(&sb, "```\n<%s|View job logs>", raceLink(runID, race))
		message.AddSection(sb.String())
	}

	return message
}

// raceLink points at the specific job that reported the race so the alert is
// directly actionable, falling back to the run when the job is unknown.
func raceLink(runID string, race DataRace) string {
	if race.JobID != "" {
		return github.JobURL(temporalRepository, runID, race.JobID)
	}
	return github.RunURL(temporalRepository, runID)
}

func formatComparison(direction int, difference string) string {
	switch {
	case direction > 0:
		return fmt.Sprintf("↑ %s", difference)
	case direction < 0:
		return fmt.Sprintf("↓ %s", difference)
	default:
		return "— no change"
	}
}

func formatPercentagePointComparison(current, previous float64, available bool) string {
	if !available {
		return "N/A"
	}
	difference := math.Abs(current - previous)
	if difference <= successRateComparisonTolerance {
		return formatComparison(0, "")
	}
	return formatComparison(cmp.Compare(current, previous), fmt.Sprintf("%.1f pp", difference))
}

func formatDurationComparison(current, previous time.Duration, available bool) string {
	if !available {
		return "N/A"
	}
	difference := (current - previous).Abs()
	if difference <= durationComparisonTolerance {
		return formatComparison(0, "")
	}
	return formatComparison(cmp.Compare(current, previous), formatDuration(difference))
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
	durationsComparable := report.DurationSamples > 0 && report.Previous.DurationSamples > 0
	message.AddFields(
		fmt.Sprintf("*Success Rate:*\n%.1f%% (%s)", report.SuccessRate,
			formatPercentagePointComparison(
				report.SuccessRate, report.Previous.SuccessRate,
				report.TotalRuns > 0 && report.Previous.TotalRuns > 0)),
		fmt.Sprintf("*Failed Runs:*\n%d/%d", report.FailedRuns, report.TotalRuns),
		fmt.Sprintf("*Average Duration:*\n%s (%s)", formatDuration(report.AverageDuration),
			formatDurationComparison(
				report.AverageDuration, report.Previous.AverageDuration,
				durationsComparable)),
		fmt.Sprintf("*Median Duration:*\n%s (%s)", formatDuration(report.MedianDuration),
			formatDurationComparison(
				report.MedianDuration, report.Previous.MedianDuration,
				durationsComparable)),
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
