import argparse
import fnmatch
import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import requests


@dataclass
class TestRun:
    artifact: str
    passed: bool
    timestamp: str | None = None
    job_url: str = ""


@dataclass
class TestStats:
    name: str
    runs: list[TestRun] = field(default_factory=list)

    @property
    def failure_rate(self) -> float:
        if not self.runs:
            return 0.0
        failures = sum(1 for r in self.runs if not r.passed)
        return (failures / len(self.runs)) * 100

    @property
    def failure_count(self) -> int:
        return sum(1 for r in self.runs if not r.passed)

    @property
    def total_runs(self) -> int:
        return len(self.runs)


class SuiteToFileMapper:
    """Maps test suite names to their source files."""

    # Static overrides for edge cases
    STATIC_OVERRIDES = {
        "TestVersioning3FunctionalSuiteV0": "tests/versioning_3_test.go",
        "TestVersioning3FunctionalSuiteV2": "tests/versioning_3_test.go",
    }

    def __init__(self, repo_root: str = "../.."):
        self.repo_root = repo_root
        self.suite_to_file: dict[str, str] = {}
        self._build_mapping()

    def _build_mapping(self) -> None:
        """Build mapping by parsing test files for suite definitions."""
        tests_dir = os.path.join(self.repo_root, "tests")
        if not os.path.isdir(tests_dir):
            return

        # Pattern to match: func TestXxxSuite(t *testing.T)
        pattern = re.compile(r"func\s+(Test\w+)\s*\(\s*t\s+\*testing\.T\s*\)")

        for root, _, files in os.walk(tests_dir):
            for filename in files:
                if not filename.endswith("_test.go"):
                    continue
                filepath = os.path.join(root, filename)
                rel_path = os.path.relpath(filepath, self.repo_root)
                try:
                    with open(filepath, "r") as f:
                        content = f.read()
                    for match in pattern.finditer(content):
                        func_name = match.group(1)
                        self.suite_to_file[func_name] = rel_path
                except Exception:
                    continue

    def get_file(self, test_name: str) -> str | None:
        """Get the file path for a test name.

        Args:
            test_name: Full test name like "TestCallbacksSuite/TestWorkflowCallbacks"

        Returns:
            Relative file path or None if not found
        """
        # Extract suite name (first part before /)
        parts = test_name.split("/")
        if not parts:
            return None
        suite_name = parts[0]

        # Check static overrides first
        if suite_name in self.STATIC_OVERRIDES:
            return self.STATIC_OVERRIDES[suite_name]

        # Check dynamic mapping
        if suite_name in self.suite_to_file:
            return self.suite_to_file[suite_name]

        # Heuristic fallback: strip Test prefix and Suite suffix, convert to snake_case
        return self._heuristic_lookup(suite_name)

    def _heuristic_lookup(self, suite_name: str) -> str | None:
        """Fallback heuristic to find file from suite name."""
        # Remove Test prefix
        name = suite_name
        if name.startswith("Test"):
            name = name[4:]

        # Remove common suffixes
        for suffix in ["Suite", "TestSuite", "FunctionalSuite", "ClientSuite"]:
            if name.endswith(suffix):
                name = name[: -len(suffix)]
                break

        # Convert CamelCase to snake_case
        snake_name = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()

        # Try to find matching file
        candidate = f"tests/{snake_name}_test.go"
        full_path = os.path.join(self.repo_root, candidate)
        if os.path.isfile(full_path):
            return candidate

        return None


class CodeOwnersParser:
    """Parse CODEOWNERS file and match files to owners."""

    def __init__(self, path: str = ".github/CODEOWNERS"):
        self.rules: list[tuple[str, list[str]]] = []
        self.default_owners = ["@temporalio/server", "@temporalio/cgs"]
        self._parse(path)

    def _parse(self, path: str) -> None:
        """Parse CODEOWNERS file."""
        if not os.path.isfile(path):
            return

        try:
            with open(path, "r") as f:
                for line in f:
                    line = line.strip()
                    # Skip comments and empty lines
                    if not line or line.startswith("#"):
                        continue

                    parts = line.split()
                    if len(parts) < 2:
                        continue

                    pattern = parts[0]
                    owners = parts[1:]
                    self.rules.append((pattern, owners))
        except Exception:
            pass

    def get_owners(self, file_path: str) -> list[str]:
        """Get owners for a file path.

        Last matching rule wins (most specific).
        """
        matched_owners = self.default_owners

        for pattern, owners in self.rules:
            # Handle patterns starting with /
            check_pattern = pattern.lstrip("/")

            # Use fnmatch for glob-style matching
            if fnmatch.fnmatch(file_path, check_pattern):
                matched_owners = owners
            # Also check if pattern matches with wildcard prefix
            elif fnmatch.fnmatch(file_path, f"**/{check_pattern}"):
                matched_owners = owners

        return matched_owners


def parse_job_url(artifact: str) -> str:
    """Parse artifact string to extract job URL."""
    parts = artifact.split("--")
    if len(parts) > 2 and parts[1] and parts[2]:
        p2 = parts[2]
        if p2 == "unknown":
            return f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}"
        return f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}/job/{p2}"
    return artifact


def process_tests_with_stats(data: list[dict], max_links: int = 3) -> dict[str, TestStats]:
    """Process test data into TestStats objects with failure rates."""
    test_stats: dict[str, TestStats] = {}

    for item in data:
        name_parts = item["name"].split("/")
        if len(name_parts) < 2:
            continue

        test_name = item["name"]
        if test_name not in test_stats:
            test_stats[test_name] = TestStats(name=test_name)

        job_url = parse_job_url(item.get("artifact", ""))
        passed = item.get("passed", False)
        timestamp = item.get("timestamp")

        test_run = TestRun(
            artifact=item.get("artifact", ""),
            passed=passed,
            timestamp=timestamp,
            job_url=job_url,
        )
        test_stats[test_name].runs.append(test_run)

    return test_stats


def generate_table_report(
    test_stats: dict[str, TestStats],
    suite_mapper: SuiteToFileMapper,
    codeowners: CodeOwnersParser,
    max_links: int = 3,
    max_tests_per_owner: int = 10,
) -> str:
    """Generate markdown table report grouped by owner."""
    # Group tests by owner
    owner_tests: dict[str, list[TestStats]] = {}

    for stats in test_stats.values():
        # Only include tests with failures
        if stats.failure_count == 0:
            continue

        file_path = suite_mapper.get_file(stats.name)
        if file_path:
            owners = codeowners.get_owners(file_path)
        else:
            owners = codeowners.default_owners

        # Use first owner for grouping (or join if multiple)
        owner_key = " ".join(owners)
        if owner_key not in owner_tests:
            owner_tests[owner_key] = []
        owner_tests[owner_key].append(stats)

    # Sort each owner's tests by failure rate descending
    for tests in owner_tests.values():
        tests.sort(key=lambda x: x.failure_rate, reverse=True)

    # Sort owners by total failures descending
    sorted_owners = sorted(
        owner_tests.keys(),
        key=lambda o: sum(t.failure_count for t in owner_tests[o]),
        reverse=True,
    )

    # Generate markdown
    lines = []
    for owner in sorted_owners:
        tests = owner_tests[owner][:max_tests_per_owner]

        lines.append(f"### {owner}")
        lines.append("")
        lines.append("| Test Name | Failure Rate | Failures | Total | Links |")
        lines.append("|-----------|-------------|----------|-------|-------|")

        for stats in tests:
            # Get failure job URLs (only failures, up to max_links)
            failure_urls = [r.job_url for r in stats.runs if not r.passed][:max_links]
            links = " ".join([f"[{i+1}]({url})" for i, url in enumerate(failure_urls)])

            lines.append(
                f"| `{stats.name}` | {stats.failure_rate:.1f}% | {stats.failure_count} | {stats.total_runs} | {links} |"
            )

        lines.append("")

    return "\n".join(lines)


def output_all_tests_json(test_stats: dict[str, TestStats], output_file: str) -> None:
    """Write all test runs to a JSON file for aggregation."""
    all_tests = []

    for stats in test_stats.values():
        for run in stats.runs:
            all_tests.append({
                "name": stats.name,
                "passed": run.passed,
                "timestamp": run.timestamp,
                "job_url": run.job_url,
            })

    with open(output_file, "w") as f:
        json.dump(all_tests, f, indent=2)


def process_tests(data, pattern, output_file: str, max_links: int = 3):
    # Group data by test name and collect artifacts for tests matching pattern
    test_groups = {}
    for item in data:
        # Only process failures for legacy reports
        if item.get("passed", False):
            continue

        name_parts = item["name"].split("/")
        if len(name_parts) < 2:
            continue
        if not item["name"].endswith(pattern):
            continue

        test_name = item["name"]
        if test_name not in test_groups:
            test_groups[test_name] = []

        job_url = parse_job_url(item.get("artifact", ""))
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
        # Only process failures for legacy reports
        if item.get("passed", False):
            continue

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
    repo_root: str = "../..",
):
    """Process flaky tests with table format output grouped by owner."""
    # Process all tests (passed and failed) for stats
    test_stats = process_tests_with_stats(data, max_links)

    # Initialize mappers
    suite_mapper = SuiteToFileMapper(repo_root)
    codeowners_path = os.path.join(repo_root, ".github/CODEOWNERS")
    codeowners = CodeOwnersParser(codeowners_path)

    # Generate table report
    table_content = generate_table_report(
        test_stats, suite_mapper, codeowners, max_links
    )

    with open(output_file, "w") as outfile:
        outfile.write(table_content)

    # Output all tests JSON for aggregation
    all_tests_file = output_file.replace(".txt", "_all.json")
    output_all_tests_json(test_stats, all_tests_file)

    # Also create a plain text version for Slack (without links for cleaner viewing)
    slack_file = output_file.replace(".txt", "_slack.txt")
    slack_lines = []

    # Get top 10 flaky tests by failure rate for Slack
    flaky_tests = [s for s in test_stats.values() if s.failure_count > 0]
    flaky_tests.sort(key=lambda x: x.failure_rate, reverse=True)
    flaky_tests = flaky_tests[:10]

    for stats in flaky_tests:
        slack_lines.append(
            f"* {stats.failure_rate:.1f}% failure rate ({stats.failure_count}/{stats.total_runs}): `{stats.name}`\n"
        )

    with open(slack_file, "w") as outfile:
        outfile.writelines(slack_lines)


def create_success_message(
    crash_count: int,
    flaky_count: int,
    retry_count: int,
    timeout_count: int,
    flaky_content: str,
    run_id: str,
    total_failures: int,
) -> dict[str, Any]:
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


def create_failure_message(run_id: str, ref_name: str, sha: str) -> dict[str, Any]:
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


def send_slack_message(webhook_url: str, message: dict[str, Any]) -> bool:
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
        # Count table rows (lines starting with |) minus header rows
        # Or bullet points for legacy format
        table_rows = content.count("\n| `")
        bullet_points = content.count("* ")
        return max(table_rows, bullet_points)
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
        summary_lines.append("### Flaky Tests (by Owner)")
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


def process_json_file(input_filename: str, max_links: int = 3, repo_root: str = "../.."):
    with open(input_filename, "r") as file:
        # Load the file content as JSON
        data = json.load(file)

    # Create output directory if it doesn't exist
    os.makedirs("out", exist_ok=True)

    process_flaky(data, "out/flaky.txt", max_links, repo_root)
    process_tests(data, "(timeout)", "out/timeout.txt", max_links)
    process_tests(data, "(retry 2)", "out/retry.txt", max_links)
    process_crash(data, "(crash)", "out/crash.txt", max_links)

    # Also output all tests JSON at the top level
    test_stats = process_tests_with_stats(data, max_links)
    output_all_tests_json(test_stats, "out/all_tests.json")

    # Return total number of failures in the original data
    return sum(1 for item in data if not item.get("passed", False))


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
    parser.add_argument(
        "--send-slack",
        action="store_true",
        help="Enable Slack notifications (requires --slack-webhook)",
    )
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

    # Repository options
    parser.add_argument(
        "--repo-root",
        default="../..",
        help="Path to repository root for file mapping (default: ../..)",
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

    # Only send Slack if --send-slack flag is set and webhook is provided
    if args.send_slack and args.slack_webhook:
        send_success_slack_notification(args, crash_count, flaky_count, retry_count, timeout_count, total_failures)
    elif args.send_slack and not args.slack_webhook:
        print("Warning: --send-slack specified but --slack-webhook not provided", file=sys.stderr)


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

    if args.send_slack and args.slack_webhook:
        send_failure_slack_notification(args)

    sys.exit(1)


def main():
    """Main entry point for the flaky tests processing script."""
    parser = create_argument_parser()
    args = parser.parse_args()

    # Try to process the JSON file and handle both success and failure cases
    try:
        total_failures = process_json_file(args.file, args.max_links, args.repo_root)
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
