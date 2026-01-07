import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests


def get_test_contributors(
    test_names: List[str],
    find_test_lines_bin: Optional[str] = None,
    commit: str = "HEAD",
    repo_root: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get contributors for a list of test names using the find-test-lines tool.
    Returns a dict mapping test_path -> list of contributors.
    """
    if not find_test_lines_bin or not test_names:
        return {}

    if not os.path.exists(find_test_lines_bin):
        print(f"Warning: find-test-lines binary not found at {find_test_lines_bin}", file=sys.stderr)
        return {}

    # Write test names to a temp file
    test_paths_file = "out/test_paths.txt"
    os.makedirs("out", exist_ok=True)
    with open(test_paths_file, "w") as f:
        for name in test_names:
            f.write(name + "\n")

    # Determine repo root (for Go package loading)
    if not repo_root:
        # Try to find repo root from git
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--show-toplevel"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                repo_root = result.stdout.strip()
        except Exception:
            pass

    # Run the tool
    try:
        env = os.environ.copy()
        env["INPUT_TEST_PATHS_FILE"] = os.path.abspath(test_paths_file)
        env["INPUT_COMMIT"] = commit
        if repo_root:
            env["INPUT_DIR"] = repo_root

        result = subprocess.run(
            [find_test_lines_bin],
            capture_output=True,
            text=True,
            env=env,
            timeout=300,  # 5 minute timeout
        )

        if result.returncode != 0:
            print(f"Warning: find-test-lines failed: {result.stderr}", file=sys.stderr)
            return {}

        # Parse JSON output
        results = json.loads(result.stdout)
        contributors_map = {}
        for item in results:
            test_path = item.get("test_path", "")
            contributors = item.get("contributors", [])
            if test_path and contributors:
                contributors_map[test_path] = contributors

        return contributors_map

    except subprocess.TimeoutExpired:
        print("Warning: find-test-lines timed out", file=sys.stderr)
        return {}
    except json.JSONDecodeError as e:
        print(f"Warning: failed to parse find-test-lines output: {e}", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"Warning: error running find-test-lines: {e}", file=sys.stderr)
        return {}


def format_contributors(contributors: List[Dict[str, Any]], max_contributors: int = 5) -> str:
    """Format contributors list for display."""
    if not contributors:
        return ""
    names = [c["name"] for c in contributors[:max_contributors]]
    return ", ".join(names)


def process_tests(data, pattern, output_file: str, max_links: int = 3):
    # Group data by test name and collect artifacts for tests matching pattern
    test_groups = {}
    for item in data:
        name_parts = item["name"].split("/")
        if len(name_parts) < 2:
            continue
        if not item["name"].endswith(pattern):
            continue

        test_name = item["name"]
        if test_name not in test_groups:
            test_groups[test_name] = []

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

        test_groups[test_name].append(job_url)

    # Transform into list with counts and multiple links
    transformed = []
    for test_name, artifacts in test_groups.items():
        failure_count = len(artifacts)
        # Get up to max_links most recent artifacts (already sorted desc from SQL)
        recent_artifacts = artifacts[:max_links]

        transformed.append({
            "name": test_name,
            "count": failure_count,
            "artifacts": recent_artifacts,
        })

    # Sort by failure count descending
    transformed.sort(key=lambda x: x["count"], reverse=True)

    # Write bullet point format
    lines = []
    for item in transformed:
        # Format: * XXX failures: `TestName` [1](url1) [2](url2) [3](url3)
        links = " ".join([f"[{i+1}]({url})" for i, url in enumerate(item["artifacts"])])
        lines.append(f"* {item['count']} failures: `{item['name']}` {links}\n")

    with open(output_file, "w") as outfile:
        outfile.writelines(lines)


def process_crash(data, pattern, output_file: str, max_links: int = 3):
    # Group data by test name and collect artifacts for crash tests
    test_groups = {}
    for item in data:
        if "crash" not in item["name"]:
            continue

        test_name = item["name"]
        if test_name not in test_groups:
            test_groups[test_name] = []

        parts = item["artifact"].split("--")
        if len(parts) > 0 and len(parts[1]) > 0:
            job_url = (
                f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}"
            )
        else:
            job_url = item["artifact"]

        test_groups[test_name].append(job_url)

    # Transform into list with counts and multiple links
    transformed = []
    for test_name, artifacts in test_groups.items():
        failure_count = len(artifacts)
        # Get up to max_links most recent artifacts (already sorted desc from SQL)
        recent_artifacts = artifacts[:max_links]

        transformed.append({
            "name": test_name,
            "count": failure_count,
            "artifacts": recent_artifacts,
        })

    # Sort by failure count descending
    transformed.sort(key=lambda x: x["count"], reverse=True)

    # Write bullet point format
    lines = []
    for item in transformed:
        # Format: * XXX failures: `TestName` [1](url1) [2](url2) [3](url3)
        links = " ".join([f"[{i+1}]({url})" for i, url in enumerate(item["artifacts"])])
        lines.append(f"* {item['count']} failures: `{item['name']}` {links}\n")

    with open(output_file, "w") as outfile:
        outfile.writelines(lines)


def process_flaky(
    data,
    output_file: str,
    max_links: int = 3,
    contributors_map: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> List[str]:
    """
    Process flaky test data and write output files.
    Returns list of test names for contributor lookup.
    """
    if contributors_map is None:
        contributors_map = {}

    # Group data by test name and collect artifacts
    test_groups = {}
    for item in data:
        name_parts = item["name"].split("/")
        if len(name_parts) < 2:
            continue

        test_name = item["name"]
        if test_name not in test_groups:
            test_groups[test_name] = []

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

        test_groups[test_name].append(job_url)

    # Transform into list with counts and multiple links
    transformed = []
    for test_name, artifacts in test_groups.items():
        failure_count = len(artifacts)
        # Get up to max_links most recent artifacts (already sorted desc from SQL)
        recent_artifacts = artifacts[:max_links]

        transformed.append({
            "name": test_name,
            "count": failure_count,
            "artifacts": recent_artifacts,
        })

    # Sort by failure count descending
    transformed.sort(key=lambda x: x["count"], reverse=True)

    # Limit to top 10 flaky tests
    transformed = transformed[:10]

    # Write bullet point format (for GitHub)
    lines = []
    for item in transformed:
        # Format: * XXX failures: `TestName` [1](url1) [2](url2) [3](url3)
        links = " ".join([f"[{i+1}]({url})" for i, url in enumerate(item["artifacts"])])
        contrib_str = format_contributors(contributors_map.get(item["name"], []))
        if contrib_str:
            lines.append(f"* {item['count']} failures: `{item['name']}` ({contrib_str}) {links}\n")
        else:
            lines.append(f"* {item['count']} failures: `{item['name']}` {links}\n")

    with open(output_file, "w") as outfile:
        outfile.writelines(lines)

    # Also create a plain text version for Slack (without links for cleaner viewing)
    slack_file = output_file.replace(".txt", "_slack.txt")
    slack_lines = []
    for item in transformed:
        # Format for Slack: • XXX failures: `TestName` (contributors)
        contrib_str = format_contributors(contributors_map.get(item["name"], []))
        if contrib_str:
            slack_lines.append(f"• {item['count']} failures: `{item['name']}` ({contrib_str})\n")
        else:
            slack_lines.append(f"• {item['count']} failures: `{item['name']}`\n")

    with open(slack_file, "w") as outfile:
        outfile.writelines(slack_lines)

    # Return test names for contributor lookup
    return [item["name"] for item in transformed]


def create_success_message(
    crash_count: int,
    flaky_count: int,
    retry_count: int,
    timeout_count: int,
    flaky_content: str,
    run_id: str,
    total_failures: int,
) -> Dict[str, Any]:
    """Create a success Slack message with flaky tests report."""

    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "Flaky Tests Report - Last 7 Days"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"Total Failures: {total_failures}"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"Crashes: {crash_count}"},
                {"type": "mrkdwn", "text": f"Flaky Tests: {flaky_count}"},
                {"type": "mrkdwn", "text": f"Retry Failures: {retry_count}"},
                {"type": "mrkdwn", "text": f"Timeouts: {timeout_count}"},
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
                    "text": f"Flaky Tests Details:\n{flaky_content}",
                },
            }
        )

    # Add link to full report
    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<https://github.com/temporalio/temporal/actions/runs/{run_id}| Report & Artifacts>",
            },
        }
    )

    return {"text": "Flaky Tests Report - Last 7 Days", "blocks": blocks}


def create_failure_message(run_id: str, ref_name: str, sha: str) -> Dict[str, Any]:
    """Create a failure Slack message."""

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Flaky Tests Report Generation Failed*",
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

    return {"text": "Flaky Tests Report Generation Failed", "blocks": blocks}


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
        print(f"Slack message sent successfully (status: {response.status_code})")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to send Slack message: {e}", file=sys.stderr)
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
            f"Warning: Could not read flaky content from {file_path}: {e}",
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
        return content.count("* ")
    except Exception:
        return 0


def create_github_actions_summary(
    crash_count: int,
    flaky_count: int,
    retry_count: int,
    timeout_count: int,
    run_id: str,
) -> str:
    """Create GitHub Actions summary content."""
    summary_lines = []
    
    # Header
    summary_lines.append(f"## Flaky Tests Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    summary_lines.append("")

    # Summary table
    summary_lines.append("### Failure Categories Summary")
    summary_lines.append("")
    summary_lines.append("| Category | Count |")
    summary_lines.append("|----------|-------|")
    summary_lines.append(f"| Crashes | {crash_count} |")
    summary_lines.append(f"| Timeouts | {timeout_count} |")
    summary_lines.append(f"| Flaky Tests | {flaky_count} |")
    summary_lines.append(f"| Retry Failures | {retry_count} |")
    summary_lines.append("")
    
    # Add detailed tables for each category
    if crash_count > 0 and os.path.exists("out/crash.txt"):
        summary_lines.append("### Crashes")
        summary_lines.append("")
        with open("out/crash.txt", "r") as f:
            summary_lines.append(f.read())
        summary_lines.append("")

    if timeout_count > 0 and os.path.exists("out/timeout.txt"):
        summary_lines.append("### Timeouts")
        summary_lines.append("")
        with open("out/timeout.txt", "r") as f:
            summary_lines.append(f.read())
        summary_lines.append("")

    if flaky_count > 0 and os.path.exists("out/flaky.txt"):
        summary_lines.append("### Flaky Tests")
        summary_lines.append("")
        with open("out/flaky.txt", "r") as f:
            summary_lines.append(f.read())
        summary_lines.append("")

    if retry_count > 0 and os.path.exists("out/retry.txt"):
        summary_lines.append("### Retry Failures")
        summary_lines.append("")
        with open("out/retry.txt", "r") as f:
            summary_lines.append(f.read())
        summary_lines.append("")
    
    if crash_count == 0 and flaky_count == 0 and retry_count == 0 and timeout_count == 0:
        summary_lines.append("**No test failures found in the last 7 days!**")
    
    return "\n".join(summary_lines)


def write_github_actions_summary(summary_content: str) -> None:
    """Write GitHub Actions summary to the step summary file."""
    try:
        summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary_file:
            with open(summary_file, "w") as f:
                f.write(summary_content)
            print(f"GitHub Actions summary written to {summary_file}")
        else:
            print("GITHUB_STEP_SUMMARY environment variable not set, skipping summary creation")
    except Exception as e:
        print(f"Warning: Could not write GitHub Actions summary: {e}", file=sys.stderr)


def process_json_file(
    input_filename: str,
    max_links: int = 3,
    find_test_lines_bin: Optional[str] = None,
    commit: str = "HEAD",
):
    with open(input_filename, "r") as file:
        # Load the file content as JSON
        data = json.load(file)

    # Create output directory if it doesn't exist
    os.makedirs("out", exist_ok=True)

    # First pass: get test names from flaky tests (without contributors)
    test_names = process_flaky(data, "out/flaky.txt", max_links, contributors_map={})

    # Look up contributors if the tool is available
    contributors_map = {}
    if find_test_lines_bin and test_names:
        print(f"Looking up contributors for {len(test_names)} flaky tests...")
        contributors_map = get_test_contributors(test_names, find_test_lines_bin, commit)
        print(f"Found contributors for {len(contributors_map)} tests")

        # Re-process flaky tests with contributors
        if contributors_map:
            process_flaky(data, "out/flaky.txt", max_links, contributors_map)

    process_tests(data, "(timeout)", "out/timeout.txt", max_links)
    process_tests(data, "(retry 2)", "out/retry.txt", max_links)
    process_crash(data, "(crash)", "out/crash.txt", max_links)

    # Return total number of failures in the original data
    return len(data)


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Process flaky test data, generate GitHub Actions summary, and optionally send Slack notifications"
    )
    parser.add_argument(
        "--file",
        "-f",
        default="out.json",
        help="Input JSON file to process (default: out.json)",
    )

    # GitHub Actions summary options
    parser.add_argument(
        "--github-summary",
        action="store_true",
        help="Generate GitHub Actions summary",
    )

    # Slack notification options
    parser.add_argument("--slack-webhook", help="Slack webhook URL for notifications")
    parser.add_argument("--run-id", help="GitHub Actions run ID")
    parser.add_argument("--ref-name", help="Git branch name")
    parser.add_argument("--sha", help="Git commit SHA")

    # Display options
    parser.add_argument(
        "--max-links",
        type=int,
        default=3,
        help="Maximum number of failure links to show per test (default: 3)",
    )

    # Contributor lookup options
    parser.add_argument(
        "--find-test-lines-bin",
        help="Path to find-test-lines binary for contributor lookup",
    )
    parser.add_argument(
        "--commit",
        default="HEAD",
        help="Git commit to use for contributor lookup (default: HEAD)",
    )

    return parser


def get_failure_counts() -> tuple[int, int, int, int]:
    """Count failures from generated report files."""
    crash_count = count_failures_in_file("out/crash.txt")
    flaky_count = count_failures_in_file("out/flaky.txt")
    retry_count = count_failures_in_file("out/retry.txt")
    timeout_count = count_failures_in_file("out/timeout.txt")
    
    print(f"Failure counts - Crashes: {crash_count}, Flaky: {flaky_count}, Retry: {retry_count}, Timeout: {timeout_count}")
    
    return crash_count, flaky_count, retry_count, timeout_count


def handle_success_case(args, total_failures: int) -> None:
    """Handle the successful processing case."""
    # Count failures from generated files
    crash_count, flaky_count, retry_count, timeout_count = get_failure_counts()

    # Generate GitHub Actions summary if requested
    if args.github_summary:
        print("Generating GitHub Actions summary...")
        summary_content = create_github_actions_summary(
            crash_count, flaky_count, retry_count, timeout_count, args.run_id or "unknown"
        )
        write_github_actions_summary(summary_content)

    if args.slack_webhook:
        send_success_slack_notification(args, crash_count, flaky_count, retry_count, timeout_count, total_failures)


def send_success_slack_notification(args, crash_count: int, flaky_count: int, retry_count: int, timeout_count: int, total_failures: int) -> None:
    """Send success Slack notification."""
    print("Sending success Slack notification...")
    if not args.run_id:
        print(
            "Error: --run-id is required for success messages",
            file=sys.stderr,
        )
        sys.exit(1)

    # Read flaky content
    flaky_content = read_flaky_content("out/flaky_slack.txt")

    message = create_success_message(
        crash_count,
        flaky_count,
        retry_count,
        timeout_count,
        flaky_content,
        args.run_id,
        total_failures,
    )

    # Send the message
    if not send_slack_message(args.slack_webhook, message):
        sys.exit(1)


def send_failure_slack_notification(args) -> None:
    """Send failure Slack notification."""
    print("Sending failure Slack notification...")
    if not all([args.run_id, args.ref_name, args.sha]):
        print(
            "Error: --run-id, --ref-name, and --sha are required for failure messages",
            file=sys.stderr,
        )
        sys.exit(1)
    message = create_failure_message(args.run_id, args.ref_name, args.sha)
    if not send_slack_message(args.slack_webhook, message):
        sys.exit(1)


def handle_failure_case(args, error_msg: str) -> None:
    """Handle the failure case with appropriate error reporting and notifications."""
    print(error_msg, file=sys.stderr)
    
    if args.slack_webhook:
        send_failure_slack_notification(args)
    
    sys.exit(1)


def main():
    """Main entry point for the flaky tests processing script."""
    parser = create_argument_parser()
    args = parser.parse_args()

    # Try to process the JSON file and handle both success and failure cases
    try:
        total_failures = process_json_file(
            args.file,
            args.max_links,
            find_test_lines_bin=args.find_test_lines_bin,
            commit=args.commit,
        )
        print(f"Successfully processed {args.file}")
        handle_success_case(args, total_failures)

    except FileNotFoundError:
        handle_failure_case(args, f"Error: File {args.file} not found")
    except json.JSONDecodeError as e:
        handle_failure_case(args, f"Error: Invalid JSON in {args.file}: {e}")
    except Exception as e:
        handle_failure_case(args, f"Error processing {args.file}: {e}")


if __name__ == "__main__":
    main()
