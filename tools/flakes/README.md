# Flaky Tests Analysis Tool

This tool processes test failure data and generates reports, with optional GitHub Actions summaries and Slack notifications.

## Usage

### Basic Processing
```bash
# Process test data and generate reports
uv run main.py --file out.json
```

### With GitHub Actions Summary
```bash
# Process data and generate GitHub Actions summary
uv run main.py \
  --file out.json \
  --github-summary \
  --run-id "123456789"
```

### With Slack Notifications
```bash
# Process data, generate summary, and send Slack notification
uv run main.py \
  --file out.json \
  --github-summary \
  --slack-webhook "https://hooks.slack.com/services/..." \
  --run-id "123456789" \
  --ref-name "main" \
  --sha "abc123def456"
```

## Command Line Options

- `--file`, `-f`: Input JSON file to process (default: out.json)
- `--github-summary`: Generate GitHub Actions summary (writes to GITHUB_STEP_SUMMARY)
- `--slack-webhook`: Slack webhook URL for notifications
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

### Automatic Error Handling
- **Success Case**: Processes data, generates GitHub Actions summary, and sends success Slack notification
- **Failure Case**: Detects failures and sends Slack notification with workflow failure details

### GitHub Actions Summary
When `--github-summary` is specified, the tool:
- Creates a comprehensive summary with failure counts by category
- Includes detailed tables for each failure type
- Writes directly to the GitHub Actions step summary
- Provides links back to the workflow run

### Workflow Integration
The tool is typically called once in the workflow with all necessary parameters:
```yaml
- name: Run Python script to process flaky tests
  run: |
    cd tools/flakes && uv run main.py \
      --file ../../out.json \
      --github-summary \
      --slack-webhook "${{ secrets.SLACK_WEBHOOK }}" \
      --run-id "${{ github.run_id }}" \
      --ref-name "${{ github.ref_name }}" \
      --sha "${{ github.sha }}"
```
