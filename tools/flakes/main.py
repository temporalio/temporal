import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional

import requests


def process_tests(data, pattern, output_file):
    lines = []
    lines.append(f"|{pattern}, Name with Url|Count|\n")
    lines.append("| -------- | ------- |\n")

    for item in data:
        name_parts = item["name"].split("/")
        if len(name_parts) < 2:
            continue
        if not item["name"].endswith(pattern):
            continue

        parts = item["artifact"].split("--")
        if len(parts) > 0 and len(parts[1]) > 0 and len(parts[2]) > 0:
            p2 = parts[2]
            if p2 == "unknown":
                job_url = (
                    f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}"
                )
            else:
                job_url = f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}/job/{p2}"
        else:
            job_url = item["artifact"]

        lines.append(f"|[{item['name']}]({job_url})|{item['failure_count']}|\n")

    with open(output_file, "w") as outfile:
        outfile.writelines(lines)


def process_crash(data, pattern, output_file):
    lines = []
    lines.append(f"|{pattern}, Name with Url|Count|\n")
    lines.append("| -------- | ------- |\n")

    for item in data:
        if "crash" in item["name"]:
            job_url = ""
            parts = item["artifact"].split("--")
            if len(parts) > 0 and len(parts[1]) > 0:
                job_url = (
                    f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}"
                )
            else:
                job_url = item["artifact"]
            lines.append(f"|[{item['name']}]({job_url})|{item['failure_count']}|\n")

    with open(output_file, "w") as outfile:
        outfile.writelines(lines)


def process_flaky(data, output_file):
    transformed = []
    for item in data:
        name_parts = item["name"].split("/")
        if len(name_parts) < 2:
            continue
        parts = item["artifact"].split("--")
        if len(parts) > 0 and len(parts[1]) > 0 and len(parts[2]) > 0:
            p2 = parts[2]
            if p2 == "unknown":
                job_url = (
                    f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}"
                )
            else:
                job_url = f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}/job/{p2}"
        else:
            job_url = item["artifact"]

        transformed.append(
            {
                "name": item["name"],  # the 'name' field from the original
                "job_url": job_url,  # the newly constructed job URL
                "count": item["failure_count"],  # store the failure_count
            }
        )

        if len(transformed) > 10:
            break

    # Write both markdown table (for GitHub) and plain text (for Slack)
    lines = []
    lines.append("|Name with Url|Count|\n")
    lines.append("| -------- | ------- |\n")
    for item in transformed:
        lines.append(f"|[{item['name']}]({item['job_url']})|{item['count']}|\n")

    with open(output_file, "w") as outfile:
        outfile.writelines(lines)

    # Also create a plain text version for Slack
    slack_file = output_file.replace(".txt", "_slack.txt")
    slack_lines = []
    for item in transformed:
        slack_lines.append(f"• {item['count']} failures: `{item['name']}`\n")

    with open(slack_file, "w") as outfile:
        outfile.writelines(slack_lines)


def create_success_message(
    crash_count: int,
    flaky_count: int,
    retry_count: int,
    timeout_count: int,
    flaky_content: str,
    run_id: str,
) -> Dict[str, Any]:
    """Create a success Slack message with flaky tests report."""

    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*📊 Flaky Tests Report - Last 7 Days*"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*💥 Crashes:* {crash_count}"},
                {"type": "mrkdwn", "text": f"*🔄 Flaky Tests:* {flaky_count}"},
                {"type": "mrkdwn", "text": f"*🔁 Retry Failures:* {retry_count}"},
                {"type": "mrkdwn", "text": f"*⏰ Timeouts:* {timeout_count}"},
            ],
        },
    ]

    # Add flaky tests details if there are any
    if flaky_count > 0 and flaky_content:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🔄 Flaky Tests Details:*\n{flaky_content}",
                },
            }
        )

    # Add link to full report
    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"📋 <https://github.com/temporalio/temporal/actions/runs/{run_id}|View Full Report & Download Artifacts>",
            },
        }
    )

    return {"text": "📊 Flaky Tests Report - Last 7 Days", "blocks": blocks}


def create_failure_message(run_id: str, ref_name: str, sha: str) -> Dict[str, Any]:
    """Create a failure Slack message."""

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*❌ Flaky Tests Report Generation Failed*",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "The flaky tests report workflow failed to generate the report.",
            },
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Run ID:* <https://github.com/temporalio/temporal/actions/runs/{run_id}|{run_id}>",
                },
                {"type": "mrkdwn", "text": f"*Branch:* `{ref_name}`"},
                {
                    "type": "mrkdwn",
                    "text": f"*Commit:* <https://github.com/temporalio/temporal/commit/{sha}|{sha}>",
                },
                {"type": "mrkdwn", "text": "*Status:* Failed"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Please check the workflow logs for more details.",
            },
        },
    ]

    return {"text": "❌ Flaky Tests Report Generation Failed", "blocks": blocks}


def send_slack_message(webhook_url: str, message: Dict[str, Any]) -> bool:
    """Send message to Slack webhook."""
    try:
        response = requests.post(
            webhook_url,
            json=message,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        print(f"✅ Slack message sent successfully (status: {response.status_code})")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to send Slack message: {e}", file=sys.stderr)
        return False


def read_flaky_content(
    file_path: str, max_items: int = 10, max_length: int = 1000
) -> str:
    """Read and format flaky tests content for Slack."""
    try:
        if not os.path.exists(file_path):
            return ""

        with open(file_path, "r") as f:
            lines = f.readlines()

        # Take first max_items lines and format them
        content_lines = []
        for line in lines[:max_items]:
            line = line.strip()
            if line:
                content_lines.append(line)

        # Join with newlines for proper formatting in Slack
        content = "\n".join(content_lines)
        if len(content) > max_length:
            content = content[:max_length] + "..."

        return content
    except Exception as e:
        print(
            f"⚠️ Warning: Could not read flaky content from {file_path}: {e}",
            file=sys.stderr,
        )
        return ""


def count_failures_in_file(file_path: str) -> int:
    """Count the number of failure entries in a report file."""
    try:
        if not os.path.exists(file_path):
            return 0
        with open(file_path, "r") as f:
            content = f.read()
        return content.count("|[")
    except Exception:
        return 0


def process_json_file(input_filename):
    with open(input_filename, "r") as file:
        # Load the file content as JSON
        data = json.load(file)

    process_flaky(data, "flaky.txt")
    process_tests(data, "(timeout)", "timeout.txt")
    process_tests(data, "(retry 2)", "retry.txt")
    process_crash(data, "(crash)", "crash.txt")


def main():
    parser = argparse.ArgumentParser(
        description="Process flaky test data and optionally send Slack notifications"
    )
    parser.add_argument(
        "--file",
        "-f",
        default="out7.json",
        help="Input JSON file to process (default: out7.json)",
    )

    # Slack notification options
    parser.add_argument("--slack-webhook", help="Slack webhook URL for notifications")
    parser.add_argument(
        "--slack-message-type",
        choices=["success", "failure"],
        help="Type of Slack message to send",
    )
    parser.add_argument("--run-id", help="GitHub Actions run ID")
    parser.add_argument("--ref-name", help="Git branch name")
    parser.add_argument("--sha", help="Git commit SHA")

    args = parser.parse_args()

    # Process the JSON file first
    try:
        process_json_file(args.file)
        print(f"Successfully processed {args.file}")
    except FileNotFoundError:
        print(f"Error: File {args.file} not found", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {args.file}: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error processing {args.file}: {e}", file=sys.stderr)
        sys.exit(1)

    # Handle Slack notification if requested
    if args.slack_webhook and args.slack_message_type:
        print("📤 Sending Slack notification...")

        if args.slack_message_type == "success":
            if not args.run_id:
                print(
                    "❌ Error: --run-id is required for success messages",
                    file=sys.stderr,
                )
                sys.exit(1)

            # Count failures from generated files
            crash_count = count_failures_in_file("crash.txt")
            flaky_count = count_failures_in_file("flaky.txt")
            retry_count = count_failures_in_file("retry.txt")
            timeout_count = count_failures_in_file("timeout.txt")

            # Read flaky content
            flaky_content = read_flaky_content("flaky_slack.txt")

            message = create_success_message(
                crash_count,
                flaky_count,
                retry_count,
                timeout_count,
                flaky_content,
                args.run_id,
            )

        elif args.slack_message_type == "failure":
            if not all([args.run_id, args.ref_name, args.sha]):
                print(
                    "❌ Error: --run-id, --ref-name, and --sha are required for failure messages",
                    file=sys.stderr,
                )
                sys.exit(1)

            message = create_failure_message(args.run_id, args.ref_name, args.sha)

        # Send the message
        success = send_slack_message(args.slack_webhook, message)
        if not success:
            sys.exit(1)


if __name__ == "__main__":
    main()
