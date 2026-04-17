package testrunner

import (
	"fmt"
	"html"
	"slices"
	"strings"
)

const summaryMaxBytes = 1000 * 1024
const summaryMaxDetailBytes = 1024

type summary struct {
	rows []summaryRow
}

// summaryRow is a single entry in the summary table.
type summaryRow struct {
	kind    failureType
	name    string // test or alert name
	details string // failure body
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

// String renders the GitHub step summary HTML and enforces both the total
// summary budget and per-row detail truncation.
func (s summary) String() string {
	if len(s.rows) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("<table>\n<tr><th>Kind</th><th>Test</th></tr>\n")

	// Reserve bytes for the closing tag so we can always finish the table.
	const tableClose = "</table>\n"
	budget := summaryMaxBytes - sb.Len() - len(tableClose)

	written := 0
	for _, row := range s.rows {
		rendered := row.String()
		if len(rendered) > budget {
			omitted := len(s.rows) - written
			fmt.Fprintf(&sb, "<tr><td colspan=\"2\">… %d failure(s) not shown — see full output in job logs</td></tr>\n", omitted)
			break
		}
		sb.WriteString(rendered)
		budget -= len(rendered)
		written++
	}

	sb.WriteString(tableClose)
	return sb.String()
}

// String renders one summary table row.
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
			escaped += "\n… (truncated — see full output in job logs)"
		}
		fmt.Fprintf(&sb, "<details><summary>%s</summary><pre>%s</pre></details>",
			html.EscapeString(row.name), escaped)
	} else {
		sb.WriteString(html.EscapeString(row.name))
	}
	sb.WriteString("</td></tr>\n")
	return sb.String()
}
