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
	sb.WriteString("🚨 CI Failed on Main Branch 🚨\n\n")
	sb.WriteString(fmt.Sprintf("Workflow: %s\n", report.Workflow.Name))
	sb.WriteString(fmt.Sprintf("Branch: %s\n", report.Workflow.HeadBranch))
	sb.WriteString(fmt.Sprintf("Commit: %s (%s)\n", report.Commit.ShortSHA, report.Commit.Author))
	sb.WriteString(fmt.Sprintf("Failed Jobs: %d of %d total jobs\n\n", len(report.FailedJobs), report.TotalJobs))
	sb.WriteString("Failed Jobs:\n")
	for _, job := range report.FailedJobs {
		sb.WriteString(fmt.Sprintf("  • %s\n    %s\n", job.Name, job.URL))
	}
	sb.WriteString(fmt.Sprintf("\nView Full Workflow Run: %s\n", report.Workflow.URL))
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
