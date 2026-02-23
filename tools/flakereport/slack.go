package flakereport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
func buildSuccessMessage(summary *ReportSummary, flakyContent, ciBreakerContent string, runID, repo string, days int) *SlackMessage {
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

	// Add CI breakers details if present
	if ciBreakerContent != "" {
		ciBreakerText := fmt.Sprintf("*CI Breakers (Failed All Retries):*\n%s", ciBreakerContent)
		msg.Blocks = append(msg.Blocks, SlackBlock{
			Type: "section",
			Text: &SlackText{
				Type: "mrkdwn",
				Text: truncateToSlackLimit(ciBreakerText, 2900),
			},
		})
	}

	// Add flaky tests details if present
	if flakyContent != "" {
		flakyText := fmt.Sprintf("*Flaky Tests Details:*\n%s", flakyContent)
		msg.Blocks = append(msg.Blocks, SlackBlock{
			Type: "section",
			Text: &SlackText{
				Type: "mrkdwn",
				Text: truncateToSlackLimit(flakyText, 2900),
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
						Text: fmt.Sprintf("*Commit:*\n%s", sha[:7]),
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

// sendSlackMessage sends message to webhook URL
func sendSlackMessage(webhookURL string, message *SlackMessage) error {
	if webhookURL == "" {
		return errors.New("webhook URL is empty")
	}

	jsonData, err := json.Marshal(message)
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
