package flakereport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// SlackMessage represents Slack Block Kit message
type SlackMessage struct {
	Text   string       `json:"text"`
	Blocks []SlackBlock `json:"blocks"`
}

type SlackBlock struct {
	Type   string      `json:"type"`
	Text   *SlackText  `json:"text,omitempty"`
	Fields []SlackText `json:"fields,omitempty"`
}

type SlackText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// truncateToSlackLimit truncates text to stay within Slack's block text limit
// Slack blocks have a 3000 character limit per text field
func truncateToSlackLimit(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return text[:limit-50] + "\n\n...(truncated due to length)"
}

// buildSuccessMessage creates success notification with report summary
func buildSuccessMessage(summary *ReportSummary, runID, repo string, days int) *SlackMessage {
	// Calculate CI success rate
	ciSuccessRate := 0.0
	if summary.TotalWorkflowRuns > 0 {
		ciSuccessRate = (float64(summary.SuccessfulRuns) / float64(summary.TotalWorkflowRuns)) * 100.0
	}

	// Summary stats
	summaryText := fmt.Sprintf("*CI Success Rate:* %d/%d (%.2f%%)\n*Total Test Runs:* %d\n*Total Failures:* %d\n*Failure Rate:* %.2f per 1000 tests\n\n*CI Breakers:* %d\n*Crashes:* %d\n*Flaky Tests:* %d\n*Timeouts:* %d",
		summary.SuccessfulRuns,
		summary.TotalWorkflowRuns,
		ciSuccessRate,
		summary.TotalTestRuns,
		summary.TotalFailures,
		summary.OverallFailureRate,
		len(summary.CIBreakers),
		len(summary.Crashes),
		summary.TotalFlakyCount,
		len(summary.Timeouts))

	// Build message
	msg := &SlackMessage{
		Text: "Flaky Tests Report Generated",
		Blocks: []SlackBlock{
			{
				Type: "header",
				Text: &SlackText{
					Type: "plain_text",
					Text: fmt.Sprintf("Flaky Tests Report - Last %d Days", days),
				},
			},
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: truncateToSlackLimit(summaryText, 2900), // Keep under 3000 char limit
				},
			},
		},
	}

	// Add CI breakers details
	if lines := formatReportLines(summary.CIBreakers); len(lines) > 0 {
		if len(lines) > slackMaxListItems {
			lines = lines[:slackMaxListItems]
		}
		text := fmt.Sprintf("*CI Breakers (top %d):*\n%s", slackMaxListItems, strings.Join(lines, "\n"))
		msg.Blocks = append(msg.Blocks, SlackBlock{
			Type: "section",
			Text: &SlackText{
				Type: "mrkdwn",
				Text: truncateToSlackLimit(text, 2900),
			},
		})
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
		msg.Blocks = append(msg.Blocks, SlackBlock{
			Type: "section",
			Text: &SlackText{
				Type: "mrkdwn",
				Text: truncateToSlackLimit(text, 2900),
			},
		})
	}

	// Add link to report
	if runID != "" {
		linkURL := fmt.Sprintf("https://github.com/%s/actions/runs/%s", repo, runID)
		msg.Blocks = append(msg.Blocks, SlackBlock{
			Type: "section",
			Text: &SlackText{
				Type: "mrkdwn",
				Text: fmt.Sprintf("<%s|Report & Artifacts>", linkURL),
			},
		})
	}

	return msg
}

// buildFailureMessage creates failure notification
func buildFailureMessage(runID, refName, sha, repo string) *SlackMessage {
	msg := &SlackMessage{
		Text: "Flaky Tests Report Generation Failed",
		Blocks: []SlackBlock{
			{
				Type: "header",
				Text: &SlackText{
					Type: "plain_text",
					Text: "Flaky Tests Report Generation Failed",
				},
			},
			{
				Type: "section",
				Fields: []SlackText{
					{
						Type: "mrkdwn",
						Text: fmt.Sprintf("*Run ID:*\n%s", runID),
					},
					{
						Type: "mrkdwn",
						Text: fmt.Sprintf("*Branch:*\n%s", refName),
					},
					{
						Type: "mrkdwn",
						Text: fmt.Sprintf("*Commit:*\n%.7s", sha),
					},
					{
						Type: "mrkdwn",
						Text: "*Status:*\nFailed",
					},
				},
			},
		},
	}

	// Add link to workflow run
	if runID != "" {
		linkURL := fmt.Sprintf("https://github.com/%s/actions/runs/%s", repo, runID)
		msg.Blocks = append(msg.Blocks, SlackBlock{
			Type: "section",
			Text: &SlackText{
				Type: "mrkdwn",
				Text: fmt.Sprintf("<%s|View Workflow Run>", linkURL),
			},
		})
	}

	return msg
}

// renderMarkdown renders a SlackMessage as markdown, treating each block's text as markdown.
func (msg *SlackMessage) renderMarkdown() string {
	var sb strings.Builder
	for _, block := range msg.Blocks {
		if block.Text != nil {
			sb.WriteString(block.Text.Text)
			sb.WriteString("\n\n")
		}
		for _, field := range block.Fields {
			sb.WriteString(field.Text)
			sb.WriteString("\n")
		}
		if len(block.Fields) > 0 {
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

// send sends message to webhook URL
func (msg *SlackMessage) send(webhookURL string) error {
	if webhookURL == "" {
		return errors.New("webhook URL is empty")
	}

	jsonData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequest("POST", webhookURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Printf("Warning: Failed to close response body: %v\n", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	fmt.Println("Slack notification sent successfully")
	return nil
}
