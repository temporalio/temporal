package cinotify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// SlackMessage represents a Slack Block Kit message
type SlackMessage struct {
	Text   string       `json:"text"`
	Blocks []SlackBlock `json:"blocks"`
}

// SlackBlock represents a block in the Slack message
type SlackBlock struct {
	Type   string      `json:"type"`
	Text   *SlackText  `json:"text,omitempty"`
	Fields []SlackText `json:"fields,omitempty"`
}

// SlackText represents text content in a Slack block
type SlackText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// BuildFailureMessage creates a Slack message for CI failure
func BuildFailureMessage(report *FailureReport) *SlackMessage {
	// Header with alert emoji
	headerBlock := SlackBlock{
		Type: "section",
		Text: &SlackText{
			Type: "mrkdwn",
			Text: ":rotating_light: *CI Failed on Main Branch* :rotating_light:",
		},
	}

	// Workflow and commit info
	commitURL := fmt.Sprintf("https://github.com/temporalio/temporal/commit/%s", report.Commit.SHA)
	infoBlock := SlackBlock{
		Type: "section",
		Fields: []SlackText{
			{
				Type: "mrkdwn",
				Text: fmt.Sprintf("*Workflow:*\n%s", report.Workflow.Name),
			},
			{
				Type: "mrkdwn",
				Text: fmt.Sprintf("*Branch:*\n`%s`", report.Workflow.HeadBranch),
			},
			{
				Type: "mrkdwn",
				Text: fmt.Sprintf("*Commit:*\n<%s|%s>", commitURL, report.Commit.ShortSHA),
			},
			{
				Type: "mrkdwn",
				Text: fmt.Sprintf("*Author:*\n%s", report.Commit.Author),
			},
		},
	}

	// Failure summary
	summaryBlock := SlackBlock{
		Type: "section",
		Text: &SlackText{
			Type: "mrkdwn",
			Text: fmt.Sprintf("*Failed Jobs:* %d of %d total jobs",
				len(report.FailedJobs), report.TotalJobs),
		},
	}

	// List of failed jobs
	var failedJobNames []string
	for _, job := range report.FailedJobs {
		failedJobNames = append(failedJobNames,
			fmt.Sprintf("• <%s|%s>", job.URL, job.Name))
	}

	jobsBlock := SlackBlock{
		Type: "section",
		Text: &SlackText{
			Type: "mrkdwn",
			Text: fmt.Sprintf("*Failed Jobs:*\n%s",
				strings.Join(failedJobNames, "\n")),
		},
	}

	// Link to workflow run
	linkBlock := SlackBlock{
		Type: "section",
		Text: &SlackText{
			Type: "mrkdwn",
			Text: fmt.Sprintf("<%s|View Full Workflow Run>", report.Workflow.URL),
		},
	}

	return &SlackMessage{
		Text: fmt.Sprintf("CI Failed on Main: %s", report.Workflow.Name),
		Blocks: []SlackBlock{
			headerBlock,
			infoBlock,
			summaryBlock,
			jobsBlock,
			linkBlock,
		},
	}
}

// FormatMessageForDebug formats the message for console output
func FormatMessageForDebug(report *FailureReport) string {
	var sb strings.Builder
	fmt.Fprint(&sb, "🚨 CI Failed on Main Branch 🚨\n\n")
	fmt.Fprintf(&sb, "Workflow: %s\n", report.Workflow.Name)
	fmt.Fprintf(&sb, "Branch: %s\n", report.Workflow.HeadBranch)
	fmt.Fprintf(&sb, "Commit: %s (%s)\n", report.Commit.ShortSHA, report.Commit.Author)
	fmt.Fprintf(&sb, "Failed Jobs: %d of %d total jobs\n\n", len(report.FailedJobs), report.TotalJobs)
	fmt.Fprintln(&sb, "Failed Jobs:")
	for _, job := range report.FailedJobs {
		fmt.Fprintf(&sb, "  • %s\n    %s\n", job.Name, job.URL)
	}
	fmt.Fprintf(&sb, "\nView Full Workflow Run: %s\n", report.Workflow.URL)
	return sb.String()
}

// SendSlackMessage sends the message to Slack webhook
func SendSlackMessage(webhookURL string, message *SlackMessage) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Post(webhookURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack returned status %d", resp.StatusCode)
	}

	return nil
}

// BuildSuccessReportMessage creates a Slack message for success report
func BuildSuccessReportMessage(report *DigestReport) *SlackMessage {
	// Header
	headerBlock := SlackBlock{
		Type: "section",
		Text: &SlackText{
			Type: "mrkdwn",
			Text: fmt.Sprintf(":chart_with_upwards_trend: *Weekly CI Report - %s Branch*", report.Branch),
		},
	}

	// Period
	periodBlock := SlackBlock{
		Type: "section",
		Text: &SlackText{
			Type: "mrkdwn",
			Text: fmt.Sprintf("*Report Period:* %s to %s",
				report.StartDate.Format("Jan 2, 2006"),
				report.EndDate.Format("Jan 2, 2006")),
		},
	}

	// Metrics grid
	metricsBlock := SlackBlock{
		Type: "section",
		Fields: []SlackText{
			{
				Type: "mrkdwn",
				Text: fmt.Sprintf("*Success Rate:*\n%.1f%%", report.SuccessRate),
			},
			{
				Type: "mrkdwn",
				Text: fmt.Sprintf("*Total Runs:*\n%d", report.TotalRuns),
			},
			{
				Type: "mrkdwn",
				Text: fmt.Sprintf("*Failed Runs:*\n%d", report.FailedRuns),
			},
			{
				Type: "mrkdwn",
				Text: fmt.Sprintf("*Successful Runs:*\n%d", report.SuccessfulRuns),
			},
			{
				Type: "mrkdwn",
				Text: fmt.Sprintf("*Average Duration:*\n%s", formatDuration(report.AverageDuration)),
			},
			{
				Type: "mrkdwn",
				Text: fmt.Sprintf("*Median Duration:*\n%s", formatDuration(report.MedianDuration)),
			},
		},
	}

	// Timing percentiles section
	timingBlock := SlackBlock{
		Type: "section",
		Text: &SlackText{
			Type: "mrkdwn",
			Text: fmt.Sprintf("*Run Duration Distribution:*\n"+
				"• Under 20 minutes: %.1f%%\n"+
				"• Under 25 minutes: %.1f%%\n"+
				"• Under 30 minutes: %.1f%%",
				report.Under20MinutesPercent,
				report.Under25MinutesPercent,
				report.Under30MinutesPercent),
		},
	}

	blocks := []SlackBlock{headerBlock, periodBlock, metricsBlock, timingBlock}

	return &SlackMessage{
		Text:   fmt.Sprintf("Weekly CI Report - %s Branch", report.Branch),
		Blocks: blocks,
	}
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

	return sb.String()
}
