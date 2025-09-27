# Flaky Tests Analysis Tool

This tool processes test failure data and generates reports, with optional Slack notifications.

## Usage

### Basic Processing
```bash
# Process test data and generate reports
uv run main.py --file out.json
```

### With Slack Notifications

#### Success Notification
```bash
# Process data and send success notification to Slack
uv run main.py \
  --file out.json \
  --slack-webhook "https://hooks.slack.com/services/..." \
  --slack-message-type success \
  --run-id "123456789"
```

#### Failure Notification
```bash
# Send failure notification to Slack (no file processing needed)
uv run main.py \
  --slack-webhook "https://hooks.slack.com/services/..." \
  --slack-message-type failure \
  --run-id "123456789" \
  --ref-name "main" \
  --sha "abc123def456"
```

## Command Line Options

- `--file`, `-f`: Input JSON file to process (default: out7.json)
- `--slack-webhook`: Slack webhook URL for notifications
- `--slack-message-type`: Type of message ('success' or 'failure')
- `--run-id`: GitHub Actions run ID
- `--ref-name`: Git branch name (required for failure messages)
- `--sha`: Git commit SHA (required for failure messages)

## Output Files

The tool generates several report files:
- `flaky.txt`: Markdown table of flaky tests
- `flaky_slack.txt`: Plain text version for Slack
- `crash.txt`: Tests that crashed
- `timeout.txt`: Tests that timed out
- `retry.txt`: Tests that failed on retry

## GitHub Actions Integration

The tool is designed to work seamlessly with GitHub Actions workflows. It reads environment variables and generates structured Slack messages with proper formatting and links back to the workflow run.
