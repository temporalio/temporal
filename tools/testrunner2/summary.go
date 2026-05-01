package testrunner2

import (
	"fmt"
	"html"
	"io"
	"path/filepath"
	"slices"
	"strings"
)

const (
	summaryCommand        = "print-summary"
	junitGlobFlag         = "--junit-glob"
	summaryMaxBytes       = 1000 * 1024
	summaryMaxDetailBytes = 1024
)

type failureType string

const (
	failureTypeFailed   failureType = "Failed"
	failureTypeTimeout  failureType = "TIMEOUT"
	failureTypeCrash    failureType = "CRASH"
	failureTypeDataRace failureType = "DATA RACE"
	failureTypePanic    failureType = "PANIC"
	failureTypeFatal    failureType = "FATAL"
)

type summary struct {
	rows []summaryRow
}

type summaryRow struct {
	kind    failureType
	name    string
	details string
}

func printSummary(args []string, output io.Writer) error {
	var junitGlob string
	for _, arg := range args {
		value, ok := strings.CutPrefix(arg, junitGlobFlag+"=")
		if !ok {
			return fmt.Errorf("unknown argument %q", arg)
		}
		junitGlob = value
	}
	if junitGlob == "" {
		return fmt.Errorf("missing required argument %q", junitGlobFlag)
	}

	paths, err := filepath.Glob(junitGlob)
	if err != nil {
		return fmt.Errorf("failed to expand junit glob %q: %w", junitGlob, err)
	}
	if len(paths) == 0 {
		return nil
	}
	slices.Sort(paths)

	reports := make([]*junitReport, 0, len(paths))
	for _, path := range paths {
		report := &junitReport{path: path}
		if err := report.read(); err != nil {
			return fmt.Errorf("failed to read junit report %q: %w", path, err)
		}
		reports = append(reports, report)
	}

	content := newSummaryFromReports(reports).String()
	if content == "" {
		return nil
	}
	if _, err := io.WriteString(output, content); err != nil {
		return fmt.Errorf("failed to write summary: %w", err)
	}
	return nil
}

func newSummaryFromReports(reports []*junitReport) summary {
	return summary{
		rows: newSummaryRowsFromReports(reports),
	}
}

func newSummaryRowsFromReports(reports []*junitReport) []summaryRow {
	var rows []summaryRow
	for _, report := range reports {
		for _, suite := range report.Suites {
			for _, tc := range suite.Testcases {
				if tc.Failure == nil {
					continue
				}
				rows = append(rows, summaryRow{
					kind:    failureType(tc.Failure.Type),
					name:    tc.Name,
					details: tc.Failure.Data,
				})
			}
		}
	}
	slices.SortFunc(rows, func(a, b summaryRow) int {
		if byName := strings.Compare(a.name, b.name); byName != 0 {
			return byName
		}
		if byKind := strings.Compare(string(a.kind), string(b.kind)); byKind != 0 {
			return byKind
		}
		return strings.Compare(a.details, b.details)
	})
	return rows
}

func (s summary) String() string {
	if len(s.rows) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("<table>\n<tr><th>Kind</th><th>Test</th></tr>\n")

	const tableClose = "</table>\n"
	budget := summaryMaxBytes - sb.Len() - len(tableClose)

	written := 0
	for _, row := range s.rows {
		rendered := row.String()
		if len(rendered) > budget {
			omitted := len(s.rows) - written
			fmt.Fprintf(&sb, "<tr><td colspan=\"2\">... %d failure(s) not shown - see full output in job logs</td></tr>\n", omitted)
			break
		}
		sb.WriteString(rendered)
		budget -= len(rendered)
		written++
	}

	sb.WriteString(tableClose)
	return sb.String()
}

func (row summaryRow) String() string {
	details := row.details
	truncated := false
	if len(details) > summaryMaxDetailBytes {
		details = details[:summaryMaxDetailBytes]
		truncated = true
	}

	kind := string(row.kind)
	if strings.Contains(row.name, "(final)") {
		kind = "❌ " + kind
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "<tr><td>%s</td><td>", html.EscapeString(kind))
	if details != "" {
		escaped := html.EscapeString(details)
		if truncated {
			escaped += "\n... (truncated - see full output in job logs)"
		}
		fmt.Fprintf(&sb, "<details><summary>%s</summary><pre>%s</pre></details>",
			html.EscapeString(row.name), escaped)
	} else {
		sb.WriteString(html.EscapeString(row.name))
	}
	sb.WriteString("</td></tr>\n")
	return sb.String()
}
