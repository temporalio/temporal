# Flakereport Filtering Design

## Goal

Keep every Markdown report table capped at 100 rows without displaying a truncation notice, and exclude 100%-rate entries from the Flaky Tests category because they represent false positives.

## Behavior

- Remove the truncation notice from every capped table while retaining the existing 100-row limits.
- Exclude a report from `FlakyTests` when its failure count equals its total run count.
- Apply the exclusion before building `ReportSummary` so Markdown output, Slack output, summary counts, and bisect selection see the same filtered set.
- Leave CI breakers, timeouts, crashes, and flaky suites unchanged.

## Implementation

- Remove `writeTableLimitNotice` and its call sites.
- Simplify `limitReportRows` so it returns only the capped slice because callers no longer need the original row count.
- Filter `flakyReports` after converting classified flaky failures into reports. Keep generic report construction unchanged so other report categories retain their current behavior.

## Tests

- Verify every table remains capped at 100 rows and no truncation notice is rendered.
- Verify a 100%-rate flaky test is excluded.
- Verify a flaky test below 100% remains.
- Verify other report categories are unaffected.
