import argparse
import json
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict

import requests


# Minimum number of failures for a test to be considered flaky
MIN_FLAKY_FAILURES = 3


def normalize_test_name(name: str) -> str:
    """
    Strip retry suffix from test name.
    Examples:
      - "TestSomething (retry 1)" -> "TestSomething"
      - "TestSomething (retry 2)" -> "TestSomething"
      - "TestSomething" -> "TestSomething"
    """
    return re.sub(r'\s*\(retry \d+\)$', '', name)


def build_job_url_from_artifact(artifact: str) -> str:
    """Build GitHub Actions job or run URL from artifact string.

    Artifact format: {field0}--{run_id}--{job_id}
    If job_id is available and not 'unknown', links to the specific job page.
    Otherwise, links to the run page.
    """
    parts = artifact.split("--")
    if len(parts) > 2 and parts[1] and parts[2]:
        job_id = parts[2]
        if job_id != "unknown" and job_id != "":
            return f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}/job/{job_id}"
        else:
            return f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}"
    elif len(parts) > 1 and parts[1]:
        return f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}"
    return artifact


def write_report_files(
    transformed: list,
    output_file: str,
    count_label: str = "failures",
    create_slack_file: bool = True,
    create_count_file: bool = False,
    total_count: int = None,
) -> None:
    """Write report files in standardized format.

    Args:
        transformed: List of dicts with 'name', 'count', 'artifacts' keys
        output_file: Path to main output file
        count_label: Label for count (e.g., "failures", "runs")
        create_slack_file: Whether to create a Slack-friendly version
        create_count_file: Whether to create a count metadata file
        total_count: Total count to write to metadata file (if None, uses len(transformed))
    """
    # Write bullet point format (for GitHub)
    lines = []
    for item in transformed:
        links = " ".join([f"[{i+1}]({url})" for i, url in enumerate(item["artifacts"])])
        lines.append(f"* {item['count']} {count_label}: `{item['name']}` {links}\n")

    with open(output_file, "w") as outfile:
        outfile.writelines(lines)

    # Create Slack version (without links)
    if create_slack_file:
        slack_file = output_file.replace(".txt", "_slack.txt")
        slack_lines = []
        for item in transformed:
            slack_lines.append(f"• {item['count']} {count_label}: `{item['name']}`\n")
        with open(slack_file, "w") as outfile:
            outfile.writelines(slack_lines)

    # Write count metadata
    if create_count_file:
        count_file = output_file.replace(".txt", "_count.txt")
        count_to_write = total_count if total_count is not None else len(transformed)
        with open(count_file, "w") as f:
            f.write(str(count_to_write))


def process_tests(data, pattern, output_file: str, max_links: int = 3):
    # Group data by test name and collect artifacts for tests matching pattern
    test_groups = {}
    for item in data:
        name_parts = item["name"].split("/")
        if len(name_parts) < 2:
            continue
        if not item["name"].endswith(pattern):
            continue

        test_name = normalize_test_name(item["name"])
        if test_name not in test_groups:
            test_groups[test_name] = []

        job_url = build_job_url_from_artifact(item["artifact"])
        test_groups[test_name].append(job_url)

    # Transform into list with counts and multiple links
    transformed = []
    for test_name, artifacts in test_groups.items():
        failure_count = len(artifacts)
        recent_artifacts = artifacts[:max_links]
        transformed.append({
            "name": test_name,
            "count": failure_count,
            "artifacts": recent_artifacts,
        })

    # Sort by failure count descending
    transformed.sort(key=lambda x: x["count"], reverse=True)

    # Write report files
    write_report_files(transformed, output_file, count_label="failures", create_slack_file=False, create_count_file=False)


def process_crash(data, pattern, output_file: str, max_links: int = 3):
    # Group data by test name and collect artifacts for crash tests
    test_groups = {}
    for item in data:
        if "crash" not in item["name"]:
            continue

        test_name = normalize_test_name(item["name"])
        if test_name not in test_groups:
            test_groups[test_name] = []

        job_url = build_job_url_from_artifact(item["artifact"])
        test_groups[test_name].append(job_url)

    # Transform into list with counts and multiple links
    transformed = []
    for test_name, artifacts in test_groups.items():
        failure_count = len(artifacts)
        recent_artifacts = artifacts[:max_links]
        transformed.append({
            "name": test_name,
            "count": failure_count,
            "artifacts": recent_artifacts,
        })

    # Sort by failure count descending
    transformed.sort(key=lambda x: x["count"], reverse=True)

    # Write report files
    write_report_files(transformed, output_file, count_label="failures", create_slack_file=False, create_count_file=False)


def process_flaky(data, output_file: str, max_links: int = 3):
    """
    Process flaky tests and generate reports.

    Args:
        data: Test failure data
        output_file: Path to output file
        max_links: Maximum number of failure links to include per test

    Returns:
        Total count of tests with > MIN_FLAKY_FAILURES
    """
    # Group data by test name and collect artifacts
    test_groups = {}
    for item in data:
        name_parts = item["name"].split("/")
        if len(name_parts) < 2:
            continue

        test_name = normalize_test_name(item["name"])
        if test_name not in test_groups:
            test_groups[test_name] = []

        job_url = build_job_url_from_artifact(item["artifact"])
        test_groups[test_name].append(job_url)

    # Transform into list with counts and multiple links
    transformed = []
    for test_name, artifacts in test_groups.items():
        failure_count = len(artifacts)

        # Only include tests that meet the minimum threshold
        if failure_count <= MIN_FLAKY_FAILURES:
            continue

        recent_artifacts = artifacts[:max_links]
        transformed.append({
            "name": test_name,
            "count": failure_count,
            "artifacts": recent_artifacts,
        })

    # Sort by failure count descending
    transformed.sort(key=lambda x: x["count"], reverse=True)

    # Remember total count before limiting for display
    total_count = len(transformed)

    # Limit to top 10 flaky tests for display
    display_tests = transformed[:10]

    # Write report files
    write_report_files(
        display_tests,
        output_file,
        count_label="failures",
        create_slack_file=True,
        create_count_file=True,
        total_count=total_count,
    )

    return total_count


def extract_run_id(artifact: str) -> str:
    """Extract run_id from artifact string.

    Artifact format: {field0}--{run_id}--{job_id}
    """
    parts = artifact.split("--")
    if len(parts) > 1:
        return parts[1]
    return artifact


def process_ci_breaking(data, output_file: str, max_links: int = 3):
    """
    Identify tests that failed all 3 consecutive attempts on single runs.

    A test is considered "CI-breaking" if it failed the initial attempt plus
    both retry 1 and retry 2 within the same CI run, causing that run to fail.

    Args:
        data: Test failure data from Tringa
        output_file: Path to output file
        max_links: Maximum number of failure links per test

    Returns:
        Total count of CI-breaking tests
    """
    # Step 1: Group failures by run_id
    runs = {}  # {run_id: [failure_records]}
    for item in data:
        run_id = extract_run_id(item["artifact"])
        if run_id not in runs:
            runs[run_id] = []
        runs[run_id].append(item)

    # Step 2: For each run, identify tests with 3 consecutive failures
    ci_breaking_tests = {}  # {test_name: [run_urls]}

    for run_id, failures in runs.items():
        # Group by normalized test name within this run
        test_attempts = {}  # {normalized_name: [failure_records]}
        for failure in failures:
            normalized = normalize_test_name(failure["name"])
            if normalized not in test_attempts:
                test_attempts[normalized] = []
            test_attempts[normalized].append(failure)

        # Check for 3 consecutive failures (base + retry 1 + retry 2)
        for normalized_name, attempts in test_attempts.items():
            # Check if we have base test + retry 1 + retry 2
            has_base = any(normalize_test_name(f["name"]) == normalized_name and "(retry" not in f["name"] for f in attempts)
            has_retry1 = any("(retry 1)" in f["name"] for f in attempts)
            has_retry2 = any("(retry 2)" in f["name"] for f in attempts)

            if has_base and has_retry1 and has_retry2:
                # This test failed all 3 attempts in this run
                if normalized_name not in ci_breaking_tests:
                    ci_breaking_tests[normalized_name] = []

                # Get the job URL from the first failure's artifact
                job_url = build_job_url_from_artifact(attempts[0]["artifact"])
                ci_breaking_tests[normalized_name].append(job_url)

    # Step 3: Transform into list with counts and multiple links
    transformed = []
    for test_name, run_urls in ci_breaking_tests.items():
        run_count = len(run_urls)
        recent_runs = run_urls[:max_links]
        transformed.append({
            "name": test_name,
            "count": run_count,
            "artifacts": recent_runs,
        })

    # Sort by run count descending
    transformed.sort(key=lambda x: x["count"], reverse=True)

    total_count = len(transformed)

    # Write report files
    write_report_files(
        transformed,
        output_file,
        count_label="runs",
        create_slack_file=True,
        create_count_file=True,
        total_count=total_count,
    )

    return total_count


def create_success_message(
    crash_count: int,
    flaky_count: int,
    timeout_count: int,
    ci_breaking_count: int,
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
                {"type": "mrkdwn", "text": f"Timeouts: {timeout_count}"},
                {"type": "mrkdwn", "text": f"CI-Breaking: {ci_breaking_count}"},
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
    timeout_count: int,
    ci_breaking_count: int,
) -> str:
    """Create GitHub Actions summary content."""
    summary_lines = []

    # Header
    summary_lines.append(f"## Flaky Tests Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    summary_lines.append("")

    # Summary table
    summary_lines.append("### Failure Categories Summary")
    summary_lines.append("")
    summary_lines.append("| Category | Unique Tests |")
    summary_lines.append("|----------|--------------|")
    summary_lines.append(f"| Crashes | {crash_count} |")
    summary_lines.append(f"| Timeouts | {timeout_count} |")
    summary_lines.append(f"| Flaky Tests | {flaky_count} |")
    summary_lines.append(f"| CI-Breaking Tests | {ci_breaking_count} |")
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

    if ci_breaking_count > 0 and os.path.exists("out/ci_breaking.txt"):
        summary_lines.append("### CI-Breaking Tests (Failed All 3 Attempts)")
        summary_lines.append("")
        summary_lines.append("These tests failed all 3 consecutive retry attempts and caused CI runs to fail:")
        summary_lines.append("")
        with open("out/ci_breaking.txt", "r") as f:
            summary_lines.append(f.read())
        summary_lines.append("")

    if crash_count == 0 and flaky_count == 0 and timeout_count == 0 and ci_breaking_count == 0:
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


def process_json_file(input_filename: str, max_links: int = 3):
    with open(input_filename, "r") as file:
        # Load the file content as JSON
        data = json.load(file)

    # Create output directory if it doesn't exist
    os.makedirs("out", exist_ok=True)

    process_flaky(data, "out/flaky.txt", max_links)
    process_tests(data, "(timeout)", "out/timeout.txt", max_links)
    process_crash(data, "(crash)", "out/crash.txt", max_links)
    process_ci_breaking(data, "out/ci_breaking.txt", max_links)

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

    return parser


def get_failure_counts() -> tuple[int, int, int, int]:
    """Count failures from generated report files."""
    crash_count = count_failures_in_file("out/crash.txt")

    # For flaky tests, read from metadata file which contains total count (not just top 10)
    try:
        with open("out/flaky_count.txt", "r") as f:
            flaky_count = int(f.read().strip())
    except (FileNotFoundError, ValueError):
        # Fallback to counting lines if metadata doesn't exist
        flaky_count = count_failures_in_file("out/flaky.txt")

    timeout_count = count_failures_in_file("out/timeout.txt")

    # For CI-breaking tests, read from metadata file
    try:
        with open("out/ci_breaking_count.txt", "r") as f:
            ci_breaking_count = int(f.read().strip())
    except (FileNotFoundError, ValueError):
        # Fallback to counting lines if metadata doesn't exist
        ci_breaking_count = count_failures_in_file("out/ci_breaking.txt")

    print(f"Failure counts - Crashes: {crash_count}, Flaky: {flaky_count}, Timeout: {timeout_count}, CI-Breaking: {ci_breaking_count}")

    return crash_count, flaky_count, timeout_count, ci_breaking_count


def handle_success_case(args, total_failures: int) -> None:
    """Handle the successful processing case."""
    # Count failures from generated files
    crash_count, flaky_count, timeout_count, ci_breaking_count = get_failure_counts()

    # Generate GitHub Actions summary if requested
    if args.github_summary:
        print("Generating GitHub Actions summary...")
        summary_content = create_github_actions_summary(
            crash_count, flaky_count, timeout_count, ci_breaking_count
        )
        write_github_actions_summary(summary_content)

    if args.slack_webhook:
        send_success_slack_notification(args, crash_count, flaky_count, timeout_count, ci_breaking_count, total_failures)


def send_success_slack_notification(args, crash_count: int, flaky_count: int, timeout_count: int, ci_breaking_count: int, total_failures: int) -> None:
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
        timeout_count,
        ci_breaking_count,
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
        total_failures = process_json_file(args.file, args.max_links)
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
